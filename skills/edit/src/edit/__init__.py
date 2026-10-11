"""Exact single-occurrence string replacement for existing files."""

from __future__ import annotations

from pathlib import Path


async def run(path: str, old_str: str, new_str: str) -> str:
    """Replace a unique string in a file.

    ``old_str`` must appear exactly once in the file at ``path``; that match is
    replaced with ``new_str`` and the file is written back in place. Prefer this
    over rewriting a whole file for targeted edits.

    Args:
        path: File to edit, relative to the working directory, absolute, or
            `~`-prefixed (the leading `~`/`~user` is expanded to the home dir).
        old_str: Exact text to find. Must occur exactly once in the file.
        new_str: Replacement text.

    Returns:
        A short confirmation message.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        ValueError: If ``old_str`` is absent or matches more than once.
    """
    filepath = Path(path).expanduser()
    if not filepath.exists():
        raise FileNotFoundError(f"{path} not found")
    content = filepath.read_text(encoding="utf-8")
    count = content.count(old_str)
    if count == 0:
        raise ValueError(f"string not found in {path}")
    if count > 1:
        raise ValueError(
            f"found {count} occurrences in {path}, need exactly 1 — "
            "widen the snippet to make it unique"
        )
    match_index = content.index(old_str)
    start_line = content.count("\n", 0, match_index) + 1
    filepath.write_text(content.replace(old_str, new_str, 1), encoding="utf-8")
    resolved_path = str(filepath.resolve())
    _emit_diff(resolved_path, old_str, new_str, start_line)
    return f"Edited {resolved_path}"


# Keep in sync with DIFF_DISPLAY_MIME in src/core/kernel/index.ts.
_DIFF_DISPLAY_MIME = "application/vnd.prime-agent.diff+json"
# Even fully \u-escaped (12 JSON chars per char), two clipped sides fit the
# runtime's 16 MiB display frame cap with room to spare.
_DIFF_TEXT_CAP = 64 * 1024


def _clip(text: str) -> str:
    if len(text) <= _DIFF_TEXT_CAP:
        return text
    return f"{text[:_DIFF_TEXT_CAP]}\n[... diff text truncated: {_DIFF_TEXT_CAP} of {len(text)} characters shown ...]"


def _emit_diff(path: str, old_str: str, new_str: str, start_line: int) -> None:
    """Stream a diff to the host as a display event; best-effort outside the kernel.

    A diff over the display frame cap is resent clipped and marked
    ``truncated``, so a successful edit always leaves a record.
    """
    display = {
        _DIFF_DISPLAY_MIME: {
            "path": path,
            "old_str": old_str,
            "new_str": new_str,
            "start_line": start_line,
        },
        "text/plain": f"Edited {path}",
    }
    try:
        from rlm import emit

        try:
            emit(display)
        except ValueError:
            display[_DIFF_DISPLAY_MIME] |= {
                "old_str": _clip(old_str),
                "new_str": _clip(new_str),
                "truncated": True,
            }
            emit(display)
    except Exception:
        pass
