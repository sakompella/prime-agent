"""T3 Code from the kernel: each T3 tool is an async method of `t3`.

1. Find the method in the index below. T3 tool `t3_<name>` is `t3.<name>`; other tools keep their name (`t3.delegate_task`).
2. Read `help(t3.<method>)` for its parameters and rules before the first call.
3. Call it: `await t3.project_list(limit=5)`. Required parameters may be positional. The step is done when you hold the parsed result.

Errors: `t3.T3Error` carries T3's message and `.code` (for example "thread_not_found"). `t3.T3Unavailable` means no T3 connection or a rejected credential; report it instead of retrying. `await t3.refresh()` reloads the method list.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import keyword
import os
import re
import threading
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any, Literal

from .mcp_base import McpIntegration, McpToolError

__all__ = ["T3Error", "T3Method", "T3Unavailable", "build_methods", "refresh"]

_CONTRACT = __doc__
_IMPORT_LIST_TIMEOUT = 10.0
_JSON_TYPES = {"string": str, "integer": int, "number": float, "boolean": bool, "array": list, "object": dict}


class T3Error(RuntimeError):
    """A T3 tool call failed; `str(error)` is T3's message."""

    def __init__(self, message: str, *, tool: str | None = None, code: str | None = None):
        super().__init__(message)
        self.message = message
        self.tool = tool
        self.code = code


class T3Unavailable(T3Error):
    """No T3 connection: T3 env is missing, T3 is unreachable, or it rejected the credential."""


@dataclass(frozen=True)
class T3Method:
    """One T3 tool as a kernel method: `name` in Python, `tool` on the wire."""

    name: str
    tool: str
    description: str
    input_schema: dict[str, Any]
    signature: inspect.Signature
    wire_names: dict[str, str]


def build_methods(tools: list[dict[str, Any]], reserved: frozenset[str] = frozenset()) -> dict[str, T3Method]:
    """Map a tools/list result to methods keyed by Python name.

    A leading `t3_` is stripped unless the stripped name is another tool's name,
    one of `reserved`, or not a usable identifier; then the wire name stays.
    """
    wire_tool_names = {tool["name"] for tool in tools}
    methods: dict[str, T3Method] = {}
    for tool in tools:
        wire = tool["name"]
        stripped = wire.removeprefix("t3_")
        usable = _is_name(stripped) and stripped not in wire_tool_names and stripped not in reserved
        name = stripped if usable else wire
        schema = tool.get("inputSchema") or {}
        signature, wire_names = _signature(schema)
        methods[name] = T3Method(name, wire, tool.get("description") or "", schema, signature, wire_names)
    return methods


def _is_name(name: str) -> bool:
    return name.isidentifier() and not keyword.iskeyword(name)


def _signature(schema: dict[str, Any]) -> tuple[inspect.Signature, dict[str, str]]:
    properties: dict[str, Any] = schema.get("properties") or {}
    required: list[str] = schema.get("required") or []
    ordered_required = [name for name in properties if name in required]
    ordered_required += [name for name in required if name not in properties]
    optional = [name for name in properties if name not in required]
    parameters: list[inspect.Parameter] = []
    wire_names: dict[str, str] = {}
    for wire, kind in [(name, inspect.Parameter.POSITIONAL_OR_KEYWORD) for name in ordered_required] + [
        (name, inspect.Parameter.KEYWORD_ONLY) for name in optional
    ]:
        name = _parameter_name(wire)
        wire_names[name] = wire
        default = inspect.Parameter.empty if kind is inspect.Parameter.POSITIONAL_OR_KEYWORD else None
        annotation = _annotation(properties.get(wire) or {})
        parameters.append(inspect.Parameter(name, kind, default=default, annotation=annotation))
    return inspect.Signature(parameters), wire_names


def _parameter_name(wire: str) -> str:
    if _is_name(wire):
        return wire
    name = re.sub(r"\W", "_", wire) + "_"
    return name if name.isidentifier() else f"_{name}"


def _annotation(schema: dict[str, Any]) -> Any:
    if isinstance(schema.get("enum"), list) and schema["enum"]:
        return Literal[tuple(schema["enum"])]  # ty: ignore[invalid-type-form]
    if isinstance(schema.get("anyOf"), list):
        branches = [branch for branch in schema["anyOf"] if isinstance(branch, dict)]
        nullable = any(branch.get("type") == "null" for branch in branches)
        options = {_annotation(branch) for branch in branches if branch.get("type") != "null"}
        inner = options.pop() if len(options) == 1 else Any
        return inner | None if nullable and inner is not Any else inner
    return _JSON_TYPES.get(schema.get("type"), Any)


def _method_doc(method: T3Method) -> str:
    properties = method.input_schema.get("properties") or {}
    lines = [method.description.strip(), "", "Parameters:"]
    for name, parameter in method.signature.parameters.items():
        required = "required" if parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD else "optional"
        detail = (properties.get(method.wire_names[name]) or {}).get("description")
        line = f"  {name}: {inspect.formatannotation(parameter.annotation)}, {required}."
        lines.append(f"{line} {detail}" if detail else line)
    if not method.signature.parameters:
        lines.append("  (none)")
    lines += ["", f"T3 tool: {method.tool}"]
    return "\n".join(lines)


def _index_line(method: T3Method) -> str:
    bare = method.signature.replace(
        parameters=[
            parameter.replace(annotation=inspect.Parameter.empty)
            for parameter in method.signature.parameters.values()
        ]
    )
    first_sentence = re.split(r"(?<=\.)\s", method.description.strip(), maxsplit=1)[0]
    return f"- {method.name}{bare}: {first_sentence}"


class _T3Client(McpIntegration):
    """McpIntegration bound to T3's endpoint; notes T3's 401 body, which the SDK drops."""

    server = "t3"

    def __init__(self, url: str, token: str):
        super().__init__()
        self.endpoint = url
        self._bearer = token
        self.rejection: str | None = None

    async def _open_session(self, stack: AsyncExitStack):
        import httpx2
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        async def note_rejection(response: httpx2.Response) -> None:
            if response.status_code == 401:
                await response.aread()
                self.rejection = _t3_failure(response.text)[1]

        # Same timeouts and no-redirect rule as McpIntegration._open_session.
        client = await stack.enter_async_context(
            httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {self._bearer}"},
                timeout=httpx2.Timeout(30.0, read=300.0),
                follow_redirects=False,
                event_hooks={"response": [note_rejection]},
            )
        )
        read, write, *_ = await stack.enter_async_context(
            streamable_http_client(self.endpoint, http_client=client)
        )
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        return session


def _client() -> _T3Client:
    url = os.environ.get("T3_MCP_URL", "").strip()
    token = os.environ.get("T3_MCP_BEARER_TOKEN", "").strip()
    if not url or not token:
        raise T3Unavailable(
            "This kernel has no T3 Code connection: T3_MCP_URL and T3_MCP_BEARER_TOKEN are not set."
        )
    return _T3Client(url, token)


def _t3_failure(text: str) -> tuple[str | None, str]:
    """T3's `(code, message)` from an error body, or the raw text when it is not T3's JSON."""
    try:
        parsed = json.loads(text)
    except ValueError:
        return None, text
    if isinstance(parsed, dict) and isinstance(parsed.get("message"), str):
        code = parsed.get("code")
        return (code if isinstance(code, str) else None), parsed["message"]
    return None, text


def _as_t3_error(error: BaseException, client: _T3Client, tool: str | None) -> BaseException:
    from httpx2 import TransportError
    from mcp import MCPError

    while isinstance(error, BaseExceptionGroup) and error.exceptions:
        error = error.exceptions[0]
    if client.rejection is not None:
        return T3Unavailable(client.rejection, tool=tool)
    if isinstance(error, McpToolError):
        code, message = _t3_failure(str(error))
        return T3Error(message, tool=tool, code=code)
    if isinstance(error, MCPError):
        return T3Error(error.message, tool=tool)
    if isinstance(error, (TransportError, OSError)):
        return T3Unavailable(f"Cannot reach T3 Code at {client.endpoint}: {error}", tool=tool)
    return error


async def _list_tools() -> list[dict[str, Any]]:
    client = _client()
    try:
        return await client.list_tools()
    except Exception as error:  # noqa: BLE001 - the SDK wraps failures in ExceptionGroups; _as_t3_error unwraps
        raise _as_t3_error(error, client, None) from None


async def _call(tool: str, arguments: dict[str, Any]) -> Any:
    client = _client()
    try:
        result = await client.call_tool(tool, arguments)
    except Exception as error:  # noqa: BLE001 - see _list_tools
        raise _as_t3_error(error, client, tool) from None
    if isinstance(result, str):
        try:
            return json.loads(result)
        except ValueError:
            return result
    return result


def _bind(method: T3Method):
    async def call(*args: Any, **kwargs: Any) -> Any:
        bound = method.signature.bind(*args, **kwargs)
        arguments = {
            method.wire_names[name]: value
            for name, value in bound.arguments.items()
            if value is not None
            or method.signature.parameters[name].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        }
        return await _call(method.tool, arguments)

    call.__name__ = call.__qualname__ = method.name
    call.__module__ = __name__
    call.__signature__ = method.signature  # ty: ignore[unresolved-attribute]
    call.__doc__ = _method_doc(method)
    return call


_methods: dict[str, T3Method] = {}
_listing_error: T3Error | None = None


def _install(tools: list[dict[str, Any]]) -> None:
    global _methods, _listing_error, __doc__
    _methods = build_methods(tools, frozenset(globals()))
    _listing_error = None
    index = "\n".join(
        _index_line(method) for method in sorted(_methods.values(), key=lambda method: method.name)
    )
    __doc__ = f"{_CONTRACT}\nMethods:\n{index}\n"


def _install_failure(error: T3Error) -> None:
    global _methods, _listing_error, __doc__
    _methods = {}
    _listing_error = error
    __doc__ = f"{_CONTRACT}\nMethods: unavailable. {error.message}\n"


async def refresh() -> None:
    """Relist T3's tools and rebuild the methods and the `help(t3)` index."""
    try:
        tools = await _list_tools()
    except T3Error as error:
        _install_failure(error)
        raise
    _install(tools)


def _list_at_import() -> None:
    # Import runs inside the kernel's event loop, so the one listing gets its own loop on a thread.
    try:
        _client()
    except T3Unavailable as error:
        _install_failure(error)
        return
    outcome: dict[str, Any] = {}

    def run() -> None:
        try:
            outcome["tools"] = asyncio.run(asyncio.wait_for(_list_tools(), _IMPORT_LIST_TIMEOUT))
        except TimeoutError:
            outcome["error"] = T3Unavailable(
                f"T3 Code did not list its tools within {_IMPORT_LIST_TIMEOUT:g} s."
            )
        except T3Error as error:
            outcome["error"] = error
        except Exception as error:  # noqa: BLE001 - kept for attribute access, never raised at import
            outcome["error"] = T3Unavailable(f"Listing T3 tools failed: {type(error).__name__}: {error}")

    thread = threading.Thread(target=run, name="t3-list-tools", daemon=True)
    thread.start()
    thread.join(_IMPORT_LIST_TIMEOUT + 5)
    if "tools" in outcome:
        _install(outcome["tools"])
    else:
        _install_failure(outcome.get("error") or T3Unavailable("Listing T3 tools did not finish."))


def __getattr__(name: str) -> Any:
    if not name.startswith("_"):
        if name in _methods:
            return _bind(_methods[name])
        if _listing_error is not None:
            raise T3Unavailable(
                f"{_listing_error.message.rstrip('.')}. T3's tools are not loaded; reload them with `await t3.refresh()`.",
                code=_listing_error.code,
            )
    raise AttributeError(f"module 't3' has no method {name!r}; see help(t3) for the method index")


def __dir__() -> list[str]:
    return sorted([*__all__, *_methods])


_list_at_import()
