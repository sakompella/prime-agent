from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Literal
from unittest import mock

# Three tools copied from T3's live tools/list, plus synthetic naming edge cases.
_T3_TOOLS: list[dict[str, Any]] = json.loads(
    r"""
[
    {
        "name": "t3_thread_send",
        "description": "Send a message to any T3 thread in this environment. The target cannot have broader permission modes than the caller. Do not use a delegated task's childThreadId to start another review round here; use delegate_task with the full review context and a new clientRequestId for that round. Thread messages do not create a new delegated task or reopen a completed task. mode='auto' starts an idle thread, steers a fully active turn, or queues behind a turn that is not yet steerable. Use queue for a separate follow-up turn, steer for an in-flight update, or restart to interrupt-and-restart the active turn. clientRequestId makes retries idempotent.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "threadId": {
                    "type": "string"
                },
                "message": {
                    "type": "string"
                },
                "mode": {
                    "anyOf": [
                        {
                            "type": "string",
                            "enum": [
                                "auto",
                                "queue",
                                "steer",
                                "restart"
                            ]
                        },
                        {
                            "type": "null"
                        }
                    ]
                },
                "clientRequestId": {
                    "anyOf": [
                        {
                            "type": "string"
                        },
                        {
                            "type": "null"
                        }
                    ]
                }
            },
            "required": [
                "threadId",
                "message"
            ],
            "additionalProperties": true
        }
    },
    {
        "name": "t3_project_list",
        "description": "List registered projects in this environment. Pages use the current project snapshot and may shift between calls.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "cursor": {
                    "anyOf": [
                        {
                            "type": "integer",
                            "minimum": 0
                        },
                        {
                            "type": "null"
                        }
                    ]
                },
                "limit": {
                    "anyOf": [
                        {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 100
                        },
                        {
                            "type": "null"
                        }
                    ]
                }
            },
            "additionalProperties": true
        }
    },
    {
        "name": "preview_drag",
        "description": "Drag one element onto another in the tab selected by tabId, or this agent session's current tab when omitted. Server browser tabs only.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {
                    "anyOf": [
                        {
                            "type": "string"
                        },
                        {
                            "type": "null"
                        }
                    ],
                    "description": "Exact collaborative browser tab to target. Omit to use this agent session's current tab."
                },
                "source": {
                    "type": "string",
                    "pattern": "^\\S[\\s\\S]*\\S$|^\\S$|^$",
                    "minLength": 1,
                    "description": "Locator of the element to drag, for example aria-ref=<ref> from the latest snapshot.",
                    "allOf": [
                        {
                            "description": "Locator of the element to drag, for example aria-ref=<ref> from the latest snapshot."
                        }
                    ]
                },
                "target": {
                    "type": "string",
                    "pattern": "^\\S[\\s\\S]*\\S$|^\\S$|^$",
                    "minLength": 1,
                    "description": "Locator of the element to drop onto, for example aria-ref=<ref>.",
                    "allOf": [
                        {
                            "description": "Locator of the element to drop onto, for example aria-ref=<ref>."
                        }
                    ]
                },
                "timeoutMs": {
                    "anyOf": [
                        {
                            "type": "integer",
                            "exclusiveMinimum": 0,
                            "maximum": 60000,
                            "description": "Maximum wait in milliseconds. Defaults to 15000; maximum 60000."
                        },
                        {
                            "type": "null"
                        }
                    ],
                    "description": "Maximum wait in milliseconds. Defaults to 15000; maximum 60000."
                }
            },
            "required": [
                "source",
                "target"
            ],
            "additionalProperties": true,
            "description": "Drags one element and drops it onto another."
        }
    }
]
"""
)
_EDGE_TOOLS = [
    {"name": "t3_refresh", "description": "Collides with t3.refresh.", "inputSchema": {"type": "object"}},
    {"name": "t3_worktree_list", "description": "Stripped.", "inputSchema": {"type": "object"}},
    {"name": "worktree_list", "description": "Already taken.", "inputSchema": {"type": "object"}},
    {"name": "t3_class", "description": "Keyword after stripping.", "inputSchema": {"type": "object"}},
    {
        "name": "t3_keywords",
        "description": "Parameters that are not Python names.",
        "inputSchema": {
            "type": "object",
            "properties": {"from": {"type": "string"}, "dry-run": {"type": "boolean"}},
            "required": ["from"],
        },
    },
]
_TOKEN = "good-token"
_REJECTION = "A valid provider-scoped MCP bearer credential is required."
_THREAD_GONE = {
    "_tag": "OrchestratorMcpFailure",
    "code": "thread_not_found",
    "message": "Thread nope is no longer available.",
}


def _text_result(value: Any, *, is_error: bool = False) -> dict[str, Any]:
    return {"result": {"content": [{"type": "text", "text": json.dumps(value)}], "isError": is_error}}


class _StubT3(ThreadingHTTPServer):
    """T3's MCP endpoint reduced to what the client needs: JSON responses, bearer check."""

    daemon_threads = True

    def __init__(self, tools: list[dict[str, Any]]):
        super().__init__(("127.0.0.1", 0), _StubHandler)
        self.tools = tools
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.reply = lambda name, arguments: _text_result({"tool": name, "arguments": arguments})
        self.url = f"http://127.0.0.1:{self.server_address[1]}/mcp"
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def methods(self) -> list[str]:
        return [method for method, _ in self.requests]

    def calls(self) -> list[tuple[str, dict[str, Any]]]:
        return [
            (params["name"], params.get("arguments") or {})
            for method, params in self.requests
            if method == "tools/call"
        ]

    def stop(self) -> None:
        self.shutdown()
        self.server_close()


class _StubHandler(BaseHTTPRequestHandler):
    server: _StubT3

    def log_message(self, *args: Any) -> None:
        pass

    def _send(self, status: int, body: Any = None, headers: dict[str, str] | None = None) -> None:
        data = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        if body is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._send(405)

    def do_DELETE(self) -> None:
        self._send(200)

    def do_POST(self) -> None:
        message = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.headers.get("Authorization") != f"Bearer {_TOKEN}":
            self._send(401, {"error": "invalid_mcp_credential", "message": _REJECTION})
            return
        method, params = message.get("method"), message.get("params") or {}
        self.server.requests.append((method, params))
        if "id" not in message:
            self._send(202)
            return
        if method == "initialize":
            result = {
                "protocolVersion": params["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "t3-stub", "version": "1"},
            }
            self._send(
                200, {"jsonrpc": "2.0", "id": message["id"], "result": result}, {"mcp-session-id": "stub"}
            )
        elif method == "tools/list":
            self._send(200, {"jsonrpc": "2.0", "id": message["id"], "result": {"tools": self.server.tools}})
        elif method == "tools/call":
            reply = self.server.reply(params["name"], params.get("arguments") or {})
            self._send(200, {"jsonrpc": "2.0", "id": message["id"], **reply})
        else:
            error = {"code": -32601, "message": f"Method not found: {method}"}
            self._send(200, {"jsonrpc": "2.0", "id": message["id"], "error": error})


class T3ModuleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _StubT3(_T3_TOOLS + _EDGE_TOOLS)
        self.addCleanup(self.server.stop)
        env = mock.patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        for key in ("T3_MCP_URL", "T3_MCP_BEARER_TOKEN"):
            os.environ.pop(key, None)
        self.addCleanup(sys.modules.pop, "rlm.t3", None)

    def connect(self, token: str = _TOKEN) -> None:
        os.environ["T3_MCP_URL"] = self.server.url
        os.environ["T3_MCP_BEARER_TOKEN"] = token

    def import_t3(self):
        """Import a fresh module the way the kernel bootstrap does: inside a running loop."""
        sys.modules.pop("rlm.t3", None)

        async def inside_running_loop():
            return importlib.import_module("rlm.t3")

        return asyncio.run(inside_running_loop())

    def test_import_lists_tools_once_and_builds_named_methods(self) -> None:
        self.connect()
        t3 = self.import_t3()

        self.assertEqual(self.server.methods().count("tools/list"), 1)
        self.assertLessEqual(
            {
                "thread_send",
                "project_list",
                "preview_drag",
                "t3_refresh",
                "t3_worktree_list",
                "worktree_list",
                "t3_class",
                "keywords",
            },
            set(dir(t3)),
        )
        self.assertNotIn("class_", dir(t3))
        self.assertTrue(inspect.iscoroutinefunction(t3.thread_send))
        self.assertEqual(
            inspect.signature(t3.thread_send),
            inspect.Signature(
                [
                    inspect.Parameter("threadId", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                    inspect.Parameter("message", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                    inspect.Parameter(
                        "mode",
                        inspect.Parameter.KEYWORD_ONLY,
                        default=None,
                        annotation=Literal["auto", "queue", "steer", "restart"] | None,
                    ),
                    inspect.Parameter(
                        "clientRequestId", inspect.Parameter.KEYWORD_ONLY, default=None, annotation=str | None
                    ),
                ]
            ),
        )
        self.assertEqual(str(inspect.signature(t3.preview_drag)).split(", *")[0], "(source: str, target: str")
        self.assertEqual(str(inspect.signature(t3.keywords)), "(from_: str, *, dry_run_: bool = None)")

    def test_docs_index_every_method_and_describe_each_one(self) -> None:
        self.connect()
        t3 = self.import_t3()

        index = t3.__doc__.split("Methods:", 1)[1]
        self.assertIn(
            "- project_list(*, cursor=None, limit=None): List registered projects in this environment.", index
        )
        self.assertIn("- thread_send(threadId, message, *, mode=None, clientRequestId=None):", index)
        self.assertNotIn("inputSchema", t3.__doc__)
        doc = t3.preview_drag.__doc__
        self.assertIn("source: str, required. Locator of the element to drag", doc)
        self.assertIn("tabId: str | None, optional.", doc)
        self.assertTrue(doc.rstrip().endswith("T3 tool: preview_drag"))

    def test_call_sends_wire_arguments_and_returns_parsed_json(self) -> None:
        self.connect()
        t3 = self.import_t3()

        async def calls():
            sent = await t3.thread_send("thread-1", message="hello", mode=None, clientRequestId="r1")
            renamed = await t3.keywords("main", dry_run_=True)
            return sent, renamed

        sent, renamed = asyncio.run(calls())
        expected_send = {"threadId": "thread-1", "message": "hello", "clientRequestId": "r1"}
        self.assertEqual(sent, {"tool": "t3_thread_send", "arguments": expected_send})
        self.assertEqual(renamed, {"tool": "t3_keywords", "arguments": {"from": "main", "dry-run": True}})
        self.assertEqual(
            self.server.calls(),
            [("t3_thread_send", expected_send), ("t3_keywords", {"from": "main", "dry-run": True})],
        )

    def test_plain_text_results_stay_text(self) -> None:
        self.connect()
        t3 = self.import_t3()
        self.server.reply = lambda name, arguments: {
            "result": {"content": [{"type": "text", "text": "not json"}]}
        }

        self.assertEqual(asyncio.run(t3.project_list()), "not json")

    def test_tool_error_raises_t3_error_with_t3_message_and_code(self) -> None:
        self.connect()
        t3 = self.import_t3()
        self.server.reply = lambda name, arguments: _text_result(_THREAD_GONE, is_error=True)

        with self.assertRaises(t3.T3Error) as raised:
            asyncio.run(t3.thread_send("nope", "hi"))
        error = raised.exception
        self.assertNotIsInstance(error, t3.T3Unavailable)
        self.assertEqual(
            (str(error), error.message, error.code, error.tool),
            (_THREAD_GONE["message"], _THREAD_GONE["message"], "thread_not_found", "t3_thread_send"),
        )

    def test_json_rpc_error_raises_t3_error(self) -> None:
        self.connect()
        t3 = self.import_t3()
        self.server.reply = lambda name, arguments: {
            "error": {"code": -32602, "message": "Invalid params: limit"}
        }

        with self.assertRaises(t3.T3Error) as raised:
            asyncio.run(t3.project_list(limit=500))
        self.assertNotIsInstance(raised.exception, t3.T3Unavailable)
        self.assertEqual((str(raised.exception), raised.exception.code), ("Invalid params: limit", None))

    def test_rejected_token_raises_t3_unavailable_with_t3_message(self) -> None:
        self.connect(token="stale-token")
        t3 = self.import_t3()

        with self.assertRaises(t3.T3Unavailable) as raised:
            _ = t3.project_list
        self.assertIn(_REJECTION, str(raised.exception))
        self.assertIn("await t3.refresh()", str(raised.exception))

        self.connect()
        asyncio.run(t3.refresh())
        os.environ["T3_MCP_BEARER_TOKEN"] = "rotated-token"
        with self.assertRaises(t3.T3Unavailable) as raised:
            asyncio.run(t3.project_list())
        self.assertEqual(str(raised.exception), _REJECTION)

    def test_unreachable_t3_raises_t3_unavailable(self) -> None:
        self.connect()
        t3 = self.import_t3()
        os.environ["T3_MCP_URL"] = "http://127.0.0.1:1/mcp"

        with self.assertRaises(t3.T3Unavailable):
            asyncio.run(t3.project_list())

    def test_absent_env_makes_no_request_and_calls_raise_t3_unavailable(self) -> None:
        os.environ["T3_MCP_URL"] = self.server.url
        t3 = self.import_t3()

        self.assertEqual(self.server.requests, [])
        with self.assertRaises(t3.T3Unavailable):
            _ = t3.project_list
        with self.assertRaises(t3.T3Unavailable):
            asyncio.run(t3.refresh())
        self.assertEqual(self.server.requests, [])
        self.assertFalse(hasattr(t3, "_private"))

    def test_wrong_arguments_raise_type_error_without_a_request(self) -> None:
        self.connect()
        t3 = self.import_t3()
        before = list(self.server.requests)

        for call in (
            lambda: t3.thread_send("thread-1", "hi", bogus=1),
            lambda: t3.thread_send("thread-1"),
            lambda: t3.project_list(5),
        ):
            with self.assertRaises(TypeError):
                asyncio.run(call())
        self.assertEqual(self.server.requests, before)

    def test_unknown_method_is_an_attribute_error(self) -> None:
        self.connect()
        t3 = self.import_t3()

        self.assertFalse(hasattr(t3, "no_such_tool"))

    def test_refresh_rebuilds_methods_and_index(self) -> None:
        self.connect()
        t3 = self.import_t3()
        self.server.tools = [_T3_TOOLS[1]]

        asyncio.run(t3.refresh())

        self.assertTrue(hasattr(t3, "project_list"))
        self.assertFalse(hasattr(t3, "thread_send"))
        self.assertNotIn("thread_send", t3.__doc__)


if __name__ == "__main__":
    unittest.main()
