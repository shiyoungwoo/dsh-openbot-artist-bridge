#!/usr/bin/env python3
"""Standalone Agent Client Protocol (ACP) frontend for the OpenBot artist Bot.

This process speaks ACP over newline-delimited JSON-RPC on stdin/stdout (the
side OpenBot Desktop drives) and connects, as a client, to an AF_UNIX socket
server that binds the single existing "artist" DS Bot. The socket server is the
sole owner of the Bot; this frontend never spawns its own DSH process and never
cancels the Bot's unrelated work.

Socket wire (newline-delimited JSON, one request per connection):

    request  {"op": "status"}
    reply    {"bot": {"id": ..., "name": ..., "sessionId": ...}}

    request  {"op": "prompt", "text": ..., "requestId": ...}
    stream   {"type": "text", "text": ...}          (zero or more)
    terminal {"type": "done", "stopReason": "end_turn" | "cancelled"}
             or {"type": "error", "message": ...}

Cancellation is realised by disconnecting the prompt's own socket: the server
sees one client vanish and aborts only that stream. This frontend never sends a
global cancel, so a shared artist's unrelated work keeps running.

ACP capabilities are advertised truthfully: text + resource_link prompts only,
no image/audio/embedded context, loadSession allowed (it maps an existing id
onto the live artist binding; the earlier transcript is not replayed), and a
single fixed model ("artist") in a single mode ("artist") with no model changes.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import threading
import uuid

# Resolve the default socket next to this file so the owning project is
# self-contained regardless of how OpenBot launches the process.
DEFAULT_SOCKET = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "runtime", "bridge.sock"
)

PROTOCOL_VERSION = 1
AGENT_NAME = "openbot-artist-acp"
AGENT_VERSION = "0.1.0"

MODEL_ID = "artist"
MODE_ID = "artist"
SESSION_ID_PREFIX = "openbot-artist-"

# Bound on a single newline-delimited frame; the bridge never emits more.
MAX_LINE_BYTES = 8 * 1024 * 1024


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class AcpError(Exception):
    """A JSON-RPC request error surfaced to the ACP client."""

    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class DesktopClosedError(RuntimeError):
    """The artist bridge socket is not reachable; DSH Desktop is not running."""


class BridgeError(RuntimeError):
    """The artist bridge socket replied with an error or closed unexpectedly."""


# --------------------------------------------------------------------------- #
# Artist bridge socket client
# --------------------------------------------------------------------------- #


class PromptHandle:
    """Cancellation handle for one in-flight prompt connection.

    Cancelling shuts the connection down so the blocked reader unblocks with
    EOF. This aborts exactly one prompt stream and nothing else.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sock = None
        self._cancelled = False

    @property
    def cancelled(self) -> bool:
        with self._lock:
            return self._cancelled

    def attach(self, sock) -> None:
        with self._lock:
            self._sock = sock
            already = self._cancelled
        if already:
            _shutdown(sock)

    def detach(self) -> None:
        with self._lock:
            self._sock = None

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            sock = self._sock
        if sock is not None:
            _shutdown(sock)


def _shutdown(sock) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        # Already closed or never connected: cancellation is still satisfied.
        pass


class BridgeClient:
    """One-operation client for the artist bridge AF_UNIX socket."""

    def __init__(self, socket_path: str, status_timeout: float = 5.0) -> None:
        self.socket_path = socket_path
        self.status_timeout = status_timeout

    def _connect(self, timeout=None):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            if timeout is not None:
                sock.settimeout(timeout)
            sock.connect(self.socket_path)
            sock.settimeout(None)
        except OSError as exc:
            try:
                sock.close()
            except OSError:
                pass
            raise DesktopClosedError(
                "DSH Desktop is not reachable at %s (%s). "
                "Start DSH Desktop, confirm the artist bridge is running, "
                "then retry." % (self.socket_path, exc)
            ) from exc
        return sock

    @staticmethod
    def _send(sock, payload: dict) -> None:
        data = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )
        sock.sendall(data)

    @staticmethod
    def _readline(reader):
        line = reader.readline(MAX_LINE_BYTES)
        if not line:
            return None
        return line.decode("utf-8", "replace").strip()

    def status(self) -> dict:
        """Probe the bridge and return its bot descriptor, or raise."""
        sock = self._connect(timeout=self.status_timeout)
        reader = None
        try:
            reader = sock.makefile("rb")
            self._send(sock, {"op": "status"})
            line = self._readline(reader)
        finally:
            if reader is not None:
                reader.close()
            sock.close()
        if line is None:
            raise BridgeError("artist bridge closed the status connection without replying")
        try:
            message = json.loads(line)
        except ValueError as exc:
            raise BridgeError("artist bridge returned malformed status JSON: %s" % exc)
        if isinstance(message, dict) and message.get("type") == "error":
            raise BridgeError(str(message.get("message") or "artist bridge status error"))
        bot = message.get("bot") if isinstance(message, dict) else None
        if not isinstance(bot, dict):
            raise BridgeError("artist bridge returned an invalid status reply: %r" % (message,))
        return bot

    def prompt(self, text: str, request_id: str, handle: PromptHandle, on_text) -> str:
        """Run one prompt turn.

        Returns the ACP stop reason ("end_turn" or "cancelled"). ``on_text`` is
        called for each streamed text frame. Raises ``BridgeError`` for a bridge
        error frame or an unexpected close, and ``DesktopClosedError`` when the
        socket cannot be opened.
        """
        sock = self._connect()
        handle.attach(sock)
        reader = None
        try:
            if handle.cancelled:
                return "cancelled"
            reader = sock.makefile("rb")
            self._send(sock, {"op": "prompt", "text": text, "requestId": request_id})
            while True:
                if handle.cancelled:
                    return "cancelled"
                try:
                    line = self._readline(reader)
                except OSError:
                    if handle.cancelled:
                        return "cancelled"
                    raise
                if handle.cancelled:
                    return "cancelled"
                if line is None:
                    raise BridgeError(
                        "artist bridge closed the connection before the turn completed"
                    )
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except ValueError as exc:
                    raise BridgeError("artist bridge sent malformed JSON: %s" % exc)
                if not isinstance(message, dict):
                    raise BridgeError("artist bridge sent a non-object frame: %r" % (message,))
                kind = message.get("type")
                if kind == "text":
                    chunk = message.get("text")
                    if isinstance(chunk, str) and chunk:
                        on_text(chunk)
                elif kind == "done":
                    reason = message.get("stopReason")
                    if reason == "cancelled":
                        return "cancelled"
                    return "end_turn"
                elif kind == "error":
                    raise BridgeError(str(message.get("message") or "artist bridge reported an error"))
                else:
                    # Unknown frame types are forward-compatible noise; skip them.
                    continue
        finally:
            handle.detach()
            if reader is not None:
                try:
                    reader.close()
                except OSError:
                    pass
            try:
                sock.close()
            except OSError:
                pass


# --------------------------------------------------------------------------- #
# Fixed session configuration (single model, single mode)
# --------------------------------------------------------------------------- #


def _model_state() -> dict:
    return {
        "currentModelId": MODEL_ID,
        "availableModels": [{"modelId": MODEL_ID, "name": MODEL_ID}],
    }


def _mode_state() -> dict:
    return {
        "currentModeId": MODE_ID,
        "availableModes": [{"id": MODE_ID, "name": MODE_ID}],
    }


def _config_options() -> list:
    return [
        {
            "id": "model",
            "name": "Model",
            "category": "model",
            "type": "select",
            "currentValue": MODEL_ID,
            "options": [{"value": MODEL_ID, "name": MODEL_ID}],
        }
    ]


def _session_config() -> dict:
    return {
        "modes": _mode_state(),
        "models": _model_state(),
        "configOptions": _config_options(),
    }


# --------------------------------------------------------------------------- #
# Session records
# --------------------------------------------------------------------------- #


class SessionRecord:
    __slots__ = ("session_id", "cwd", "inflight")

    def __init__(self, session_id: str, cwd: str) -> None:
        self.session_id = session_id
        self.cwd = cwd
        self.inflight = None  # type: Optional[PromptHandle]


# --------------------------------------------------------------------------- #
# ACP server
# --------------------------------------------------------------------------- #


class AcpServer:
    def __init__(self, socket_path: str) -> None:
        self.socket_path = socket_path
        self.sessions = {}
        self._out_lock = threading.Lock()
        self._threads = []
        self._stopping = False

    # -- output ------------------------------------------------------------- #

    def _write(self, message: dict) -> None:
        line = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        with self._out_lock:
            try:
                sys.stdout.write(line + "\n")
                sys.stdout.flush()
            except (BrokenPipeError, ValueError):
                # The ACP client is gone; nothing left to deliver to.
                pass

    def _respond(self, request_id, result: dict) -> None:
        self._write({"jsonrpc": "2.0", "id": request_id, "result": result})

    def _respond_error(self, request_id, code: int, message: str) -> None:
        self._write(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": code, "message": message},
            }
        )

    def _notify_text(self, session_id: str, text: str) -> None:
        if not text:
            return
        self._write(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": text},
                    },
                },
            }
        )

    # -- main loop ---------------------------------------------------------- #

    def run(self) -> None:
        try:
            for raw in sys.stdin:
                line = raw.strip()
                if not line:
                    continue
                self._handle_line(line)
        except KeyboardInterrupt:
            pass
        finally:
            self._shutdown()

    def _handle_line(self, line: str) -> None:
        try:
            message = json.loads(line)
        except ValueError as exc:
            self._respond_error(None, -32700, "Parse error: %s" % exc)
            return
        if not isinstance(message, dict):
            self._respond_error(None, -32600, "Invalid request")
            return
        method = message.get("method")
        if method is None:
            # A response to a request this frontend never makes; ignore.
            return
        request_id = message.get("id")
        is_request = request_id is not None
        params = message.get("params")
        if not isinstance(params, dict):
            params = {}
        if not is_request:
            if method == "session/cancel":
                self._cancel(params)
            return
        try:
            result = self._dispatch(method, params, request_id)
        except AcpError as exc:
            self._respond_error(request_id, exc.code, exc.message)
            return
        except Exception as exc:  # noqa: BLE001 - surfaced to the client, never swallowed
            self._respond_error(request_id, -32603, "Internal error: %s" % exc)
            return
        if result is _ASYNC:
            return
        self._respond(request_id, result)

    # -- dispatch ----------------------------------------------------------- #

    def _dispatch(self, method: str, params: dict, request_id):
        if method == "initialize":
            return self._initialize(params)
        if method == "authenticate":
            return {}
        if method == "ping":
            return {}
        if method in ("session/new", "new"):
            return self._new_session(params)
        if method in ("session/load", "load"):
            return self._load_session(params)
        if method in ("session/set_mode", "set_mode"):
            return self._set_mode(params)
        if method in ("session/set_model", "set_model"):
            return self._set_model(params)
        if method in ("session/set_config_option", "set_config_option"):
            return self._set_config_option(params)
        if method == "session/prompt":
            self._start_prompt(request_id, params)
            return _ASYNC
        raise AcpError(-32601, '"Method not found": %s' % method)

    # -- handshake ---------------------------------------------------------- #

    def _initialize(self, params: dict) -> dict:
        return {
            "protocolVersion": PROTOCOL_VERSION,
            "agentInfo": {"name": AGENT_NAME, "version": AGENT_VERSION},
            "agentCapabilities": {
                "loadSession": True,
                "promptCapabilities": {
                    "image": False,
                    "audio": False,
                    "embeddedContext": False,
                },
            },
            "authMethods": [],
        }

    def _probe_desktop(self) -> dict:
        try:
            return BridgeClient(self.socket_path).status()
        except DesktopClosedError as exc:
            raise AcpError(-32603, "Internal error: %s" % exc)
        except BridgeError as exc:
            raise AcpError(-32603, "Internal error: artist bridge status probe failed: %s" % exc)

    @staticmethod
    def _resolve_cwd(params: dict) -> str:
        cwd = params.get("cwd")
        if cwd is None:
            return os.getcwd()
        if not isinstance(cwd, str) or not os.path.isabs(cwd):
            raise AcpError(-32602, "Invalid params: cwd must be an absolute path: %r" % (cwd,))
        return cwd

    def _new_session(self, params: dict) -> dict:
        # Probe before issuing an id: an unreachable Desktop is an actionable
        # failure, not a half-created session.
        self._probe_desktop()
        cwd = self._resolve_cwd(params)
        session_id = SESSION_ID_PREFIX + uuid.uuid4().hex
        self.sessions[session_id] = SessionRecord(session_id, cwd)
        result = {"sessionId": session_id}
        result.update(_session_config())
        return result

    def _load_session(self, params: dict) -> dict:
        """Resume an existing id by mapping it onto the live artist binding.

        ``session/load`` restores only the id -> artist mapping. It does NOT
        replay the prior session: the old transcript (including a legacy DSH
        session's messages) is never re-sent and no historical content is
        injected. Any continuity a caller observes comes from the shared artist
        Bot's own live session, not from the loaded id's history.
        """
        session_id = params.get("sessionId")
        if not isinstance(session_id, str) or not session_id:
            raise AcpError(-32602, "Invalid params: sessionId is required to load a session")
        self._probe_desktop()
        cwd = self._resolve_cwd(params)
        # Accept any prior id (including legacy DSH ids) as a mapping key only;
        # the old conversation is not restored through it.
        if session_id not in self.sessions:
            self.sessions[session_id] = SessionRecord(session_id, cwd)
        result = {"sessionId": session_id}
        result.update(_session_config())
        return result

    def _require_session(self, params: dict) -> SessionRecord:
        session_id = params.get("sessionId")
        record = self.sessions.get(session_id) if isinstance(session_id, str) else None
        if record is None:
            raise AcpError(-32602, "Invalid params: unknown session: %r" % (session_id,))
        return record

    def _set_mode(self, params: dict) -> dict:
        self._require_session(params)
        mode_id = params.get("modeId")
        if mode_id != MODE_ID:
            raise AcpError(
                -32602,
                "Invalid params: unknown mode %r; the only mode is %r" % (mode_id, MODE_ID),
            )
        return {}

    def _set_model(self, params: dict) -> dict:
        self._require_session(params)
        model_id = params.get("modelId")
        if model_id != MODEL_ID:
            raise AcpError(
                -32602,
                "Invalid params: model changes are not supported; the only model is %r"
                % (MODEL_ID,),
            )
        return {}

    def _set_config_option(self, params: dict) -> dict:
        self._require_session(params)
        config_id = params.get("configId")
        if config_id != "model":
            raise AcpError(
                -32602, "Invalid params: unknown config option %r" % (config_id,)
            )
        if params.get("value") != MODEL_ID:
            raise AcpError(
                -32602,
                "Invalid params: model changes are not supported; the only model is %r"
                % (MODEL_ID,),
            )
        return {"configOptions": _config_options()}

    # -- prompt ------------------------------------------------------------- #

    @staticmethod
    def _prompt_to_text(blocks) -> str:
        if not isinstance(blocks, list):
            raise AcpError(-32602, "Invalid params: prompt must be a list of content blocks")
        parts = []
        for block in blocks:
            if not isinstance(block, dict):
                raise AcpError(-32602, "Invalid params: prompt block is not an object")
            kind = block.get("type")
            if kind == "text":
                parts.append(block.get("text") or "")
            elif kind == "resource_link":
                parts.append(
                    '\n[resource_link name=%s uri=%s]\n'
                    % (
                        json.dumps(block.get("name") or ""),
                        json.dumps(block.get("uri") or ""),
                    )
                )
            else:
                raise AcpError(
                    -32602,
                    "Invalid params: only text and resource_link prompt content is "
                    "supported; got %r" % (kind,),
                )
        return "".join(parts)

    def _start_prompt(self, request_id, params: dict) -> None:
        record = self._require_session(params)
        if record.inflight is not None:
            raise AcpError(
                -32602, "Invalid params: a prompt is already in flight for this session"
            )
        text = self._prompt_to_text(params.get("prompt"))
        if not text.strip():
            raise AcpError(-32602, "Invalid params: empty prompt")
        handle = PromptHandle()
        record.inflight = handle
        thread = threading.Thread(
            target=self._run_prompt,
            args=(request_id, record, handle, text),
            daemon=True,
        )
        self._threads.append(thread)
        thread.start()

    def _run_prompt(self, request_id, record: SessionRecord, handle: PromptHandle, text: str) -> None:
        request_key = "prompt-" + uuid.uuid4().hex
        try:
            client = BridgeClient(self.socket_path)
            stop = client.prompt(
                text,
                request_key,
                handle,
                lambda chunk: self._notify_text(record.session_id, chunk),
            )
        except DesktopClosedError as exc:
            self._clear(record, handle)
            self._respond_error(request_id, -32603, "Internal error: %s" % exc)
            return
        except BridgeError as exc:
            self._clear(record, handle)
            self._respond_error(request_id, -32603, "Internal error: %s" % exc)
            return
        except Exception as exc:  # noqa: BLE001
            self._clear(record, handle)
            if handle.cancelled:
                self._respond(request_id, {"stopReason": "cancelled"})
            else:
                self._respond_error(request_id, -32603, "Internal error: %s" % exc)
            return
        self._clear(record, handle)
        self._respond(request_id, {"stopReason": stop})

    @staticmethod
    def _clear(record: SessionRecord, handle: PromptHandle) -> None:
        if record.inflight is handle:
            record.inflight = None

    def _cancel(self, params: dict) -> None:
        session_id = params.get("sessionId")
        record = self.sessions.get(session_id) if isinstance(session_id, str) else None
        if record is not None and record.inflight is not None:
            # Abort only this session's prompt stream; the artist's other work
            # stays untouched.
            record.inflight.cancel()

    # -- teardown ----------------------------------------------------------- #

    def _shutdown(self) -> None:
        self._stopping = True
        for record in list(self.sessions.values()):
            if record.inflight is not None:
                record.inflight.cancel()
        for thread in list(self._threads):
            thread.join(timeout=2.0)


# Sentinel meaning "a worker thread will send the response".
_ASYNC = object()


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="ACP frontend bridging OpenBot Desktop to the artist Bot over AF_UNIX."
    )
    parser.add_argument(
        "--socket",
        default=DEFAULT_SOCKET,
        help="AF_UNIX socket path owned by the artist bridge (default: %(default)s)",
    )
    return parser


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)
    AcpServer(args.socket).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
