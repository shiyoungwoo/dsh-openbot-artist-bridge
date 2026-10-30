#!/usr/bin/env python3
"""Protocol tests for the standalone OpenBot artist ACP frontend.

The bridge is exercised end-to-end over its real stdin/stdout JSON-RPC wire,
against a fake AF_UNIX "artist bridge" server that stands in for OpenBot
Desktop. No real Bot, DSH process, or network is involved.
"""

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRIDGE = os.path.join(PROJECT_DIR, "acp_bridge.py")
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

ARTIST_SESSION = "ds-artist-session-0001"


# --------------------------------------------------------------------------- #
# Fake artist-bridge AF_UNIX server
# --------------------------------------------------------------------------- #


class FakeBridge:
    """Stands in for the socket server that binds the artist Bot.

    Behaviour is selected by keywords in the prompt text:
      * ``SLOW``    -> emit one chunk, then block until the client disconnects
                       (records ``prompt_aborted``) — models a cancellable turn.
      * ``DELAYED`` -> sleep briefly, then finish normally.
      * ``ERROR``   -> emit a single ``error`` frame.
      * otherwise   -> emit two text chunks and a ``done`` frame.
    """

    def __init__(self, path):
        if os.path.exists(path):
            os.unlink(path)
        self.path = path
        self.events = []
        self._lock = threading.Lock()
        self._stop = False
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(path)
        self._server.listen(16)
        self._server.settimeout(0.1)
        self._accept_thread = threading.Thread(target=self._accept, daemon=True)
        self._accept_thread.start()

    # -- records ------------------------------------------------------------ #

    def record(self, **event):
        with self._lock:
            self.events.append(event)

    def wait_event(self, predicate, timeout=5.0):
        end = time.time() + timeout
        while time.time() < end:
            with self._lock:
                for event in self.events:
                    if predicate(event):
                        return event
            time.sleep(0.01)
        raise AssertionError("timed out waiting for fake event; got %r" % (self.events,))

    # -- socket handling ---------------------------------------------------- #

    def _accept(self):
        while not self._stop:
            try:
                conn, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _read_line(self, conn, buf):
        while b"\n" not in buf:
            try:
                chunk = conn.recv(4096)
            except OSError:
                return None, buf
            if not chunk:
                return None, buf
            buf += chunk
        line, _, rest = buf.partition(b"\n")
        return line, rest

    def _send(self, conn, obj):
        conn.sendall((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))

    def _serve(self, conn):
        buf = b""
        try:
            line, buf = self._read_line(conn, buf)
            if line is None:
                return
            request = json.loads(line.decode("utf-8"))
            op = request.get("op")
            if op == "status":
                self._send(
                    conn,
                    {"bot": {"id": "artist-bot-id", "name": "\u827a\u672f\u5bb6", "sessionId": ARTIST_SESSION}},
                )
            elif op == "prompt":
                self._prompt(conn, request)
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _prompt(self, conn, request):
        text = request.get("text", "")
        self.record(op="prompt", text=text, requestId=request.get("requestId"))
        if "SLOW" in text:
            self._send(conn, {"type": "text", "text": "part1"})
            while True:
                try:
                    chunk = conn.recv(4096)
                except OSError:
                    chunk = b""
                if not chunk:
                    self.record(op="prompt_aborted", text=text)
                    return
        elif "ERROR" in text:
            self._send(conn, {"type": "error", "message": "artist bridge exploded"})
        elif "DELAYED" in text:
            time.sleep(0.4)
            self._send(conn, {"type": "text", "text": "B1"})
            self._send(conn, {"type": "done", "stopReason": "end_turn"})
        else:
            self._send(conn, {"type": "text", "text": "Hello, "})
            self._send(conn, {"type": "text", "text": "world"})
            self._send(conn, {"type": "done", "stopReason": "end_turn"})

    def close(self):
        self._stop = True
        try:
            self._server.close()
        except OSError:
            pass
        try:
            os.unlink(self.path)
        except OSError:
            pass


# --------------------------------------------------------------------------- #
# Bridge subprocess harness (real stdin/stdout ACP wire)
# --------------------------------------------------------------------------- #


class BridgeProcess:
    def __init__(self, socket_path):
        self.proc = subprocess.Popen(
            [sys.executable, BRIDGE, "--socket", socket_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._msgs = []
        self._cond = threading.Condition()
        self._next_id = 1
        threading.Thread(target=self._read_stdout, daemon=True).start()

    def _read_stdout(self):
        try:
            for line in self.proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except ValueError:
                    continue
                with self._cond:
                    self._msgs.append(message)
                    self._cond.notify_all()
        except Exception:
            pass

    def send(self, obj):
        self.proc.stdin.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self.proc.stdin.flush()

    def request(self, method, params):
        request_id = self._next_id
        self._next_id += 1
        self.send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        return request_id

    def notify(self, method, params):
        self.send({"jsonrpc": "2.0", "method": method, "params": params})

    def wait(self, predicate, timeout=5.0):
        end = time.time() + timeout
        with self._cond:
            while True:
                for message in self._msgs:
                    if predicate(message):
                        return message
                remaining = end - time.time()
                if remaining <= 0:
                    raise AssertionError(
                        "timed out waiting for message; received=%r" % (self._msgs,)
                    )
                self._cond.wait(remaining)

    def wait_response(self, request_id, timeout=5.0):
        return self.wait(
            lambda m: m.get("id") == request_id and ("result" in m or "error" in m),
            timeout,
        )

    def chunks(self, session_id):
        with self._cond:
            messages = list(self._msgs)
        out = []
        for message in messages:
            if message.get("method") != "session/update":
                continue
            params = message.get("params", {})
            if params.get("sessionId") != session_id:
                continue
            update = params.get("update", {})
            if update.get("sessionUpdate") == "agent_message_chunk":
                content = update.get("content", {})
                if content.get("type") == "text":
                    out.append(content.get("text"))
        return out

    def close(self):
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        try:
            self.proc.terminate()
            self.proc.wait(timeout=5)
        except Exception:
            try:
                self.proc.kill()
            except Exception:
                pass
        for stream in (self.proc.stdout, self.proc.stderr):
            try:
                stream.close()
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# Base test case
# --------------------------------------------------------------------------- #


class BridgeTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(dir=TESTS_DIR, prefix=".tmp-acp-")
        self.sock_path = os.path.join(self.tmpdir, "bridge.sock")
        self.fake = None
        self.bridge = None

    def tearDown(self):
        if self.bridge is not None:
            self.bridge.close()
        if self.fake is not None:
            self.fake.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def start_fake(self):
        self.fake = FakeBridge(self.sock_path)
        return self.fake

    def start_bridge(self):
        self.bridge = BridgeProcess(self.sock_path)
        return self.bridge

    def initialize(self):
        rid = self.bridge.request(
            "initialize",
            {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": False}}},
        )
        return self.bridge.wait_response(rid)["result"]

    def new_session(self):
        rid = self.bridge.request("session/new", {"cwd": self.tmpdir, "mcpServers": []})
        response = self.bridge.wait_response(rid)
        self.assertIn("result", response, response)
        return response["result"]["sessionId"], response["result"]

    def prompt(self, session_id, blocks):
        rid = self.bridge.request(
            "session/prompt", {"sessionId": session_id, "prompt": blocks}
        )
        return self.bridge.wait_response(rid)


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


class HandshakeTests(BridgeTest):
    def test_initialize_is_truthful_text_only(self):
        self.start_fake()
        self.start_bridge()
        result = self.initialize()
        self.assertEqual(result["protocolVersion"], 1)
        self.assertEqual(result["authMethods"], [])
        self.assertTrue(result["agentCapabilities"]["loadSession"])
        caps = result["agentCapabilities"]["promptCapabilities"]
        self.assertFalse(caps["image"])
        self.assertFalse(caps["audio"])
        self.assertFalse(caps["embeddedContext"])

    def test_new_session_single_artist_model_and_mode(self):
        self.start_fake()
        self.start_bridge()
        self.initialize()
        session_id, result = self.new_session()
        self.assertTrue(session_id.startswith("openbot-artist-"))
        self.assertEqual(result["models"]["currentModelId"], "artist")
        self.assertEqual(
            [m["modelId"] for m in result["models"]["availableModels"]], ["artist"]
        )
        self.assertEqual(result["modes"]["currentModeId"], "artist")
        self.assertEqual(
            [m["id"] for m in result["modes"]["availableModes"]], ["artist"]
        )

    def test_unknown_method_is_rejected(self):
        self.start_fake()
        self.start_bridge()
        self.initialize()
        rid = self.bridge.request("session/set_temperature", {})
        response = self.bridge.wait_response(rid)
        self.assertEqual(response["error"]["code"], -32601)
        self.assertIn("Method not found", response["error"]["message"])


class PromptTests(BridgeTest):
    def test_two_turns_stream_and_finish(self):
        self.start_fake()
        self.start_bridge()
        self.initialize()
        session_id, _ = self.new_session()

        first = self.prompt(session_id, [{"type": "text", "text": "hi there"}])
        self.assertEqual(first["result"]["stopReason"], "end_turn")
        self.assertEqual(self.bridge.chunks(session_id), ["Hello, ", "world"])

        second = self.prompt(session_id, [{"type": "text", "text": "again"}])
        self.assertEqual(second["result"]["stopReason"], "end_turn")
        self.assertEqual(
            self.bridge.chunks(session_id),
            ["Hello, ", "world", "Hello, ", "world"],
        )

    def test_load_accepts_old_dsh_session_id(self):
        fake = self.start_fake()
        self.start_bridge()
        self.initialize()
        old_id = "5d3c1e7a-2b9f-4c8a-9d10-abcdef012345"
        rid = self.bridge.request(
            "session/load", {"sessionId": old_id, "cwd": self.tmpdir, "mcpServers": []}
        )
        result = self.bridge.wait_response(rid)["result"]
        self.assertEqual(result["sessionId"], old_id)
        self.assertEqual(result["models"]["currentModelId"], "artist")

        resumed = self.prompt(old_id, [{"type": "text", "text": "continue"}])
        self.assertEqual(resumed["result"]["stopReason"], "end_turn")
        self.assertEqual(self.bridge.chunks(old_id), ["Hello, ", "world"])

        # session/load only restores the id -> artist mapping; the old
        # transcript must not be replayed, so the bot receives just the new
        # prompt text and nothing from prior history.
        recorded = fake.wait_event(lambda e: e.get("op") == "prompt")
        self.assertEqual(recorded["text"], "continue")

    def test_unsupported_image_block_is_rejected(self):
        self.start_fake()
        self.start_bridge()
        self.initialize()
        session_id, _ = self.new_session()
        response = self.prompt(
            session_id,
            [{"type": "image", "data": "AAAA", "mimeType": "image/png"}],
        )
        self.assertEqual(response["error"]["code"], -32602)
        self.assertIn("text and resource_link", response["error"]["message"])
        self.assertEqual(self.bridge.chunks(session_id), [])

    def test_unsupported_resource_block_is_rejected(self):
        self.start_fake()
        self.start_bridge()
        self.initialize()
        session_id, _ = self.new_session()
        response = self.prompt(
            session_id,
            [{"type": "resource", "resource": {"uri": "file:///x", "text": "x"}}],
        )
        self.assertEqual(response["error"]["code"], -32602)
        self.assertEqual(self.bridge.chunks(session_id), [])

    def test_empty_prompt_is_rejected(self):
        self.start_fake()
        self.start_bridge()
        self.initialize()
        session_id, _ = self.new_session()
        response = self.prompt(session_id, [{"type": "text", "text": "   "}])
        self.assertEqual(response["error"]["code"], -32602)
        self.assertIn("empty prompt", response["error"]["message"])

    def test_resource_link_is_encoded_for_the_bot(self):
        fake = self.start_fake()
        self.start_bridge()
        self.initialize()
        session_id, _ = self.new_session()
        response = self.prompt(
            session_id,
            [
                {"type": "text", "text": "see "},
                {"type": "resource_link", "name": "f", "uri": "file:///x"},
            ],
        )
        self.assertEqual(response["result"]["stopReason"], "end_turn")
        recorded = fake.wait_event(lambda e: e.get("op") == "prompt")
        self.assertTrue(recorded["text"].startswith("see "))
        self.assertIn('[resource_link name="f" uri="file:///x"]', recorded["text"])

    def test_bridge_error_frame_surfaces_as_error(self):
        self.start_fake()
        self.start_bridge()
        self.initialize()
        session_id, _ = self.new_session()
        response = self.prompt(session_id, [{"type": "text", "text": "ERROR now"}])
        self.assertEqual(response["error"]["code"], -32603)
        self.assertIn("artist bridge exploded", response["error"]["message"])


class CancellationTests(BridgeTest):
    def test_cancel_aborts_only_its_own_prompt(self):
        fake = self.start_fake()
        self.start_bridge()
        self.initialize()
        slow_id, _ = self.new_session()
        other_id, _ = self.new_session()

        # Session A: a slow, cancellable turn.
        slow_rid = self.bridge.request(
            "session/prompt",
            {"sessionId": slow_id, "prompt": [{"type": "text", "text": "SLOW work"}]},
        )
        self.bridge.wait(
            lambda m: m.get("method") == "session/update"
            and m["params"].get("sessionId") == slow_id
            and m["params"]["update"]["content"].get("text") == "part1"
        )

        # Session B: a turn still running when A is cancelled.
        other_rid = self.bridge.request(
            "session/prompt",
            {"sessionId": other_id, "prompt": [{"type": "text", "text": "DELAYED work"}]},
        )

        self.bridge.notify("session/cancel", {"sessionId": slow_id})

        slow_response = self.bridge.wait_response(slow_rid)
        self.assertEqual(slow_response["result"]["stopReason"], "cancelled")

        other_response = self.bridge.wait_response(other_rid)
        self.assertEqual(other_response["result"]["stopReason"], "end_turn")
        self.assertIn("B1", self.bridge.chunks(other_id))

        aborted = fake.wait_event(lambda e: e.get("op") == "prompt_aborted")
        self.assertIn("SLOW", aborted["text"])
        # The unrelated B turn was never aborted.
        self.assertFalse(
            any(
                e.get("op") == "prompt_aborted" and "DELAYED" in e.get("text", "")
                for e in fake.events
            )
        )

        # The bridge (and the shared artist binding) is still serving turns.
        follow_up = self.prompt(slow_id, [{"type": "text", "text": "after cancel"}])
        self.assertEqual(follow_up["result"]["stopReason"], "end_turn")

    def test_second_prompt_while_busy_is_rejected(self):
        self.start_fake()
        self.start_bridge()
        self.initialize()
        session_id, _ = self.new_session()

        slow_rid = self.bridge.request(
            "session/prompt",
            {"sessionId": session_id, "prompt": [{"type": "text", "text": "SLOW busy"}]},
        )
        self.bridge.wait(
            lambda m: m.get("method") == "session/update"
            and m["params"].get("sessionId") == session_id
            and m["params"]["update"]["content"].get("text") == "part1"
        )

        overlapping = self.prompt(session_id, [{"type": "text", "text": "second"}])
        self.assertEqual(overlapping["error"]["code"], -32602)
        self.assertIn("already in flight", overlapping["error"]["message"])

        self.bridge.notify("session/cancel", {"sessionId": session_id})
        slow_response = self.bridge.wait_response(slow_rid)
        self.assertEqual(slow_response["result"]["stopReason"], "cancelled")


class ModelAndModeTests(BridgeTest):
    def test_artist_is_accepted_others_rejected(self):
        self.start_fake()
        self.start_bridge()
        self.initialize()
        session_id, _ = self.new_session()

        ok = self.bridge.request(
            "session/set_model", {"sessionId": session_id, "modelId": "artist"}
        )
        self.assertEqual(self.bridge.wait_response(ok)["result"], {})

        bad = self.bridge.request(
            "session/set_model", {"sessionId": session_id, "modelId": "gpt"}
        )
        self.assertEqual(self.bridge.wait_response(bad)["error"]["code"], -32602)

        ok_mode = self.bridge.request(
            "session/set_mode", {"sessionId": session_id, "modeId": "artist"}
        )
        self.assertEqual(self.bridge.wait_response(ok_mode)["result"], {})

        bad_mode = self.bridge.request(
            "session/set_mode", {"sessionId": session_id, "modeId": "chat"}
        )
        self.assertEqual(self.bridge.wait_response(bad_mode)["error"]["code"], -32602)

        ok_cfg = self.bridge.request(
            "session/set_config_option",
            {"sessionId": session_id, "configId": "model", "value": "artist"},
        )
        self.assertEqual(
            self.bridge.wait_response(ok_cfg)["result"]["configOptions"][0]["id"], "model"
        )

        bad_cfg = self.bridge.request(
            "session/set_config_option",
            {"sessionId": session_id, "configId": "model", "value": "gpt"},
        )
        self.assertEqual(self.bridge.wait_response(bad_cfg)["error"]["code"], -32602)


class DesktopFailureTests(BridgeTest):
    def test_new_session_without_desktop_is_actionable(self):
        self.start_bridge()  # no fake server on the socket path
        self.initialize()
        rid = self.bridge.request("session/new", {"cwd": self.tmpdir, "mcpServers": []})
        response = self.bridge.wait_response(rid)
        self.assertEqual(response["error"]["code"], -32603)
        message = response["error"]["message"].lower()
        self.assertIn("dsh desktop is not reachable", message)

    def test_prompt_after_desktop_closed_is_actionable(self):
        self.start_fake()
        self.start_bridge()
        self.initialize()
        session_id, _ = self.new_session()
        # Desktop closes: the shared socket disappears.
        self.fake.close()
        self.fake = None
        response = self.prompt(session_id, [{"type": "text", "text": "hi"}])
        self.assertEqual(response["error"]["code"], -32603)
        self.assertIn("not reachable", response["error"]["message"].lower())


if __name__ == "__main__":
    unittest.main(verbosity=2)
