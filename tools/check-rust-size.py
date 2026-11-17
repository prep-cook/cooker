#!/usr/bin/env python3
"""Check Rust file and function lengths against their caps and the ratchet file.

A file may hold 600 physical lines and a function 80 lines, counted from the line with `fn`
through the line with its closing brace. A closure counts toward the function that holds it; a
nested fn is measured on its own and also counts toward its parent. Functions in test modules
and under tests/ have the same caps.

Items that were over a cap when the gate was adopted are listed in tools/rust-size-ratchet.txt
with their size then, one per line: `file <path> <lines>` or `fn <path>::<name> <lines>`. A
function name is qualified by its enclosing modules, traits, impls (`Type`, or `<Type as Trait>`
for a trait impl), fns and `macro_rules!` definitions (`name!`), with `#2`, `#3` ... for later
functions of the same name in the same file; a fn named by a macro metavariable keeps it
(`make!::$name`). A listed item may shrink but not grow, and the entry must follow it down in
the same change: the gate fails on an item over its cap that is not listed, an item larger than its
entry, an item smaller than its entry (it prints the replacement line), an item back within
its cap that is still listed, and an entry naming nothing. `--update` lowers and removes
entries to match the tree; it never adds or raises one, so a new entry is a hand edit that
review sees.

Scans every *.rs under the root except in target, node_modules and vendor directories and
hidden directories, and except files marked `@generated` in their first lines.

Exit codes: 0 clean, 1 violations found, 2 could not run (no Rust file found, the ratchet file
missing or malformed, or a file whose comments, literals or braces do not close).
Usage: python3 -I tools/check-rust-size.py [--update] [--root DIR] [--ratchet FILE]
"""

import argparse
import collections
import os
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
RATCHET = pathlib.PurePosixPath("tools/rust-size-ratchet.txt")
CAPS = {"file": 600, "fn": 80}
SKIP_DIRS = {"target", "node_modules", "vendor"}
GENERATED_HEAD = 5
NOT_A_NAME = {"dyn", "mut", "const", "unsafe", "impl", "for", "where"}


class CannotRun(Exception):
    """The input cannot be measured or compared. Maps to exit code 2."""


def _string_end(text, i, line):
    """Index after the closing quote of a string whose body starts at i."""
    n = len(text)
    while i < n:
        if text[i] == "\\":
            i += 2
        elif text[i] == '"':
            return i + 1
        else:
            i += 1
    raise CannotRun(f"line {line}: unterminated string")


def _char_end(text, i):
    """Index after a char literal opening at the quote i, or None for a lifetime or label."""
    if text.startswith("\\", i + 1):
        end = text.find("'", i + 3)
        return None if end < 0 else end + 1
    if i + 2 < len(text) and text[i + 2] == "'" and text[i + 1] != "\n":
        return i + 3
    return None


def _ident_char(c):
    return c.isalnum() or c == "_"


def tokens(text):
    """Return (kind, value, line) tokens; comments go, literals become one ("lit", "") token."""
    out, i, n, line = [], 0, len(text), 1
    while i < n:
        c = text[i]
        start = i
        if c == "\n":
            line += 1
            i += 1
            continue
        if c.isspace():
            i += 1
            continue
        if text.startswith("//", i):
            end = text.find("\n", i)
            i = n if end < 0 else end
            continue
        if text.startswith("/*", i):
            depth, i = 1, i + 2
            while depth:
                if i >= n:
                    raise CannotRun(f"line {line}: unterminated block comment")
                if text.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif text.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
        elif c == '"':
            i = _string_end(text, i + 1, line)
            out.append(("lit", "", line))
        elif c == "'":
            end = _char_end(text, i)
            if end is None:
                i += 1
                while i < n and _ident_char(text[i]):
                    i += 1
                out.append(("lifetime", text[start:i], line))
            else:
                i = end
                out.append(("lit", "", line))
        elif c.isalpha() or c == "_":
            i += 1
            while i < n and _ident_char(text[i]):
                i += 1
            word = text[start:i]
            hashes = i
            while hashes < n and text[hashes] == "#":
                hashes += 1
            if word in ("r", "br", "cr") and hashes < n and text[hashes] == '"':
                close = '"' + "#" * (hashes - i)
                end = text.find(close, hashes + 1)
                if end < 0:
                    raise CannotRun(f"line {line}: unterminated raw string")
                i = end + len(close)
                out.append(("lit", "", line))
            elif word == "r" and hashes == i + 1 and hashes < n and (text[hashes].isalpha() or text[hashes] == "_"):
                i = hashes + 1
                while i < n and _ident_char(text[i]):
                    i += 1
                out.append(("ident", text[start:i], line))
            elif word in ("b", "c") and i < n and text[i] == '"':
                i = _string_end(text, i + 1, line)
                out.append(("lit", "", line))
            elif word == "b" and i < n and text[i] == "'" and _char_end(text, i):
                i = _char_end(text, i)
                out.append(("lit", "", line))
            else:
                out.append(("ident", word, line))
                continue
        elif c.isdigit():
            i += 1
            while i < n and _ident_char(text[i]):
                i += 1
            out.append(("lit", "", line))
            continue
        else:
            pair = text[i:i + 2]
            i += 2 if pair in ("->", "=>", "::") else 1
            out.append(("punct", text[start:i], line))
            continue
        line += text.count("\n", start, i)
    return out


CLOSERS = {")": "(", "]": "[", "}": "{"}


def _walk(toks):
    """Yield (index, token, depth), depth being the nesting before the token.

    A `<` may be a comparison or shift instead of a generic. Such a `<` or `>` sits inside the
    `[...]`, `{...}` or `(...)` of a const expression, so a `>` closes only a `<` on top of the
    stack and a closing bracket also closes every `<` opened after its own opener.
    """
    stack = []
    for j, token in enumerate(toks):
        yield j, token, len(stack)
        kind, value, _ = token
        if kind != "punct":
            continue
        if value in ("(", "[", "{", "<"):
            stack.append(value)
        elif value == ">" and stack and stack[-1] == "<":
            stack.pop()
        elif value in CLOSERS:
            while stack and stack.pop() != CLOSERS[value]:
                pass


def _header_end(toks, i):
    """Index of the token that ends an item header starting at i: `{` opens a body, `;` ends a
    declaration, and a closing delimiter ends the code around a header with no body (a macro
    matcher such as `fn $name:ident` or a transcriber such as `fn $name() $body`)."""
    for j, (kind, value, _), depth in _walk(toks[i:]):
        if depth == 0 and kind == "punct" and value in ("{", ";", "}", ")", "]"):
            return i + j
    raise CannotRun(f"line {toks[i - 1][2]}: item header never ends")


def _type_name(part):
    """The last identifier outside brackets in a type, or the type's text."""
    name = None
    for _, (kind, value, _), depth in _walk(part):
        if kind == "ident" and depth == 0 and value not in NOT_A_NAME:
            name = value
    return name or "".join(value for _, value, _ in part) or "_"


def _impl_name(header):
    """`Type` for an inherent impl, `<Type as Trait>` for a trait impl."""
    if header and header[0][1] == "<":
        header = next((header[j:] for j, _, depth in _walk(header) if j and not depth), [])
    for j, (kind, value, _), depth in _walk(header):
        if depth == 0 and kind == "ident" and value == "where":
            header = header[:j]
            break
    for j, (_, value, _), depth in _walk(header):
        if depth == 0 and value == "for" and (j + 1 == len(header) or header[j + 1][1] != "<"):
            return f"<{_type_name(header[j + 1:])} as {_type_name(header[:j])}>"
    return _type_name(header)


def _fn_name(toks, i):
    """(name, index after it) for the fn keyword at i; a macro metavariable (`$name`) or
    quote interpolation (`#name`) keeps its sigil. None when no name follows (a fn type)."""
    rest = toks[i + 1:i + 3]
    if rest[:1] and rest[0][0] == "ident":
        return rest[0][1], i + 2
    if len(rest) == 2 and rest[0][1] in ("$", "#") and rest[1][0] == "ident":
        return rest[0][1] + rest[1][1], i + 3
    return None, i + 1


def functions(text):
    """Return (qualified name, first line, last line) for every fn with a body.

    The scopes in a name are modules, traits, impls, fns and `macro_rules! name { ... }`
    definitions (as `name!`)."""
    toks, found, stack, i = tokens(text), [], [], 0
    while i < len(toks):
        kind, value, line = toks[i]
        ahead = [(k, v) for k, v, _ in toks[i + 1:i + 4]]
        if kind == "ident" and value in ("fn", "trait"):
            if value == "fn":
                name, after = _fn_name(toks, i)
            else:
                name, after = (ahead[0][1], i + 2) if ahead[:1] and ahead[0][0] == "ident" else (None, i + 1)
            if name is not None:
                end = _header_end(toks, after)
                if toks[end][1] == "{":
                    stack.append((name, line if value == "fn" else None))
                    end += 1
                i = end
                continue
        elif kind == "ident" and value == "mod" and len(ahead) > 1 and ahead[0][0] == "ident" and ahead[1] == ("punct", "{"):
            stack.append((ahead[0][1], None))
            i += 3
            continue
        elif (kind == "ident" and value == "macro_rules" and len(ahead) > 2 and ahead[0] == ("punct", "!")
              and ahead[1][0] == "ident" and ahead[2] == ("punct", "{")):
            stack.append((ahead[1][1] + "!", None))
            i += 4
            continue
        elif kind == "ident" and value == "impl":
            end = _header_end(toks, i + 1)
            if toks[end][1] == "{":
                stack.append((_impl_name(toks[i + 1:end]), None))
                end += 1
            i = end
            continue
        if value == "{" and kind == "punct":
            stack.append((None, None))
        elif value == "}" and kind == "punct":
            if not stack:
                raise CannotRun(f"line {line}: unmatched closing brace")
            name, start = stack.pop()
            if start is not None:
                scope = [n for n, _ in stack if n is not None]
                found.append(("::".join(scope + [name]), start, line))
        i += 1
    if stack:
        raise CannotRun("unclosed brace at end of file")
    return found


def rust_files(root):
    """Relative POSIX paths of the measured Rust files under root, sorted."""
    paths = []
    for directory, subdirs, names in os.walk(root):
        subdirs[:] = [d for d in subdirs if d not in SKIP_DIRS and not d.startswith(".")]
        for name in names:
            if name.endswith(".rs"):
                paths.append(pathlib.Path(directory, name).relative_to(root).as_posix())
    return sorted(paths)


def generated(text):
    head = text.splitlines()[:GENERATED_HEAD]
    return any(line.lstrip().startswith("//") and "@generated" in line for line in head)


def measure(root):
    """Return {(kind, key): lines} for every measured file and function under root."""
    sizes, scanned = {}, 0
    for rel in rust_files(root):
        text = pathlib.Path(root, rel).read_text(encoding="utf-8")
        if generated(text):
            continue
        scanned += 1
        sizes[("file", rel)] = len(text.splitlines())
        try:
            found = functions(text)
        except CannotRun as error:
            raise CannotRun(f"{rel}: {error}") from None
        seen = collections.Counter()
        for name, start, end in sorted(found, key=lambda f: f[1]):
            seen[name] += 1
            suffix = f"#{seen[name]}" if seen[name] > 1 else ""
            sizes[("fn", f"{rel}::{name}{suffix}")] = end - start + 1
    if not scanned:
        raise CannotRun(f"{root}: no Rust file to measure")
    return sizes


def _entry(line):
    """(kind, key, lines) for a ratchet line, None for a blank or comment line."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split()
    if len(parts) < 3 or parts[0] not in CAPS or not re.fullmatch(r"[0-9]+", parts[-1]):
        raise CannotRun(f"expected `file <path> <lines>` or `fn <path>::<name> <lines>`, got {line!r}")
    return parts[0], " ".join(parts[1:-1]), int(parts[-1])


def parse_ratchet(text):
    """Return {(kind, key): lines} from the ratchet file's text."""
    entries = {}
    for number, line in enumerate(text.splitlines(), 1):
        try:
            entry = _entry(line)
        except CannotRun as error:
            raise CannotRun(f"{RATCHET}:{number}: {error}") from None
        if entry is None:
            continue
        kind, key, lines = entry
        if (kind, key) in entries:
            raise CannotRun(f"{RATCHET}:{number}: {kind} {key} is listed twice")
        entries[(kind, key)] = lines
    return entries


def violations(sizes, ratchet):
    found = []
    for (kind, key), lines in sorted(sizes.items()):
        cap, listed = CAPS[kind], ratchet.get((kind, key))
        if listed is None:
            if lines > cap:
                found.append(f"{kind} {key}: {lines} lines, over the {cap}-line cap; split it")
        elif lines > listed:
            found.append(f"{kind} {key}: {lines} lines, grew past its ratchet entry of {listed}")
        elif lines <= cap:
            found.append(f"{kind} {key}: {lines} lines, within the {cap}-line cap; remove its ratchet entry")
        elif lines < listed:
            found.append(f"{kind} {key}: {lines} lines, below its ratchet entry; lower the entry to `{kind} {key} {lines}`")
    for (kind, key) in ratchet:
        if (kind, key) not in sizes:
            found.append(
                f"{kind} {key}: in the ratchet but not in the tree; remove the entry "
                "(a renamed or moved item needs its entry renamed by hand)")
    return found


def update(text, sizes):
    """The ratchet text with each entry lowered to its item's size or removed; never raised."""
    out = []
    for line in text.splitlines():
        entry = _entry(line)
        if entry is None:
            out.append(line)
            continue
        kind, key, listed = entry
        lines = sizes.get((kind, key))
        if lines is not None and lines > CAPS[kind]:
            out.append(f"{kind} {key} {min(lines, listed)}")
    return "\n".join(out) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=ROOT, type=pathlib.Path, help="the tree to measure")
    parser.add_argument("--ratchet", type=pathlib.Path, help=f"the ratchet file (default: ROOT/{RATCHET})")
    parser.add_argument("--update", action="store_true", help="lower or remove ratchet entries to match the tree")
    args = parser.parse_args(argv)
    path = args.ratchet or args.root / RATCHET
    try:
        text = path.read_text(encoding="utf-8")
        ratchet = parse_ratchet(text)
        sizes = measure(args.root)
    except (OSError, UnicodeDecodeError, CannotRun) as error:
        print(f"check-rust-size: {error}", file=sys.stderr)
        return 2
    if args.update:
        lowered = update(text, sizes)
        if lowered != text:
            path.write_text(lowered, encoding="utf-8")
        ratchet = parse_ratchet(lowered)
    found = violations(sizes, ratchet)
    for line in found:
        print(line, file=sys.stderr)
    if found:
        print(f"{len(found)} size violation(s); caps: file {CAPS['file']}, fn {CAPS['fn']} lines; "
              f"ratchet: {RATCHET}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
