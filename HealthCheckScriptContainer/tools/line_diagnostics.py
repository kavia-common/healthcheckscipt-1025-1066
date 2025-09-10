#!/usr/bin/env python3
"""
HealthCheckScriptContainer - Line Length Diagnostics

This utility scans all Python source files within the HealthCheckScriptContainer
and reports:
- For each function: physical line count (excluding comments and blank lines).
- A list of functions exceeding 15 physical lines.
- Overall percentage of functions with length <= 15 lines.
- Any files exceeding 400 total lines (physical lines) and their lengths.

Run:
  python tools/line_diagnostics.py

Notes:
- "Physical lines" here means actual code lines excluding blank lines and full-line comments.
- For function line counts, we compute from function start to function end (via AST end_lineno
  when available), then filter to code-only lines using the tokenize module to exclude comments
  and blank lines.
"""
import os
import sys
import ast
import io
import tokenize
from typing import Dict, List, Tuple, Optional


BASE_DIR = os.path.dirname(os.path.dirname(__file__))


def _is_python_file(path: str) -> bool:
    return path.endswith(".py")


def _read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _file_physical_length(text: str) -> int:
    # Count non-empty lines (including comments) for the 400-line file limit check uses total physical lines.
    # The requirement states "files exceeding 400 lines", which typically refers to total lines in the file.
    # We'll count all lines physically present in the file (including comments/blank) for this part.
    return len(text.splitlines())


def _build_code_line_set(text: str) -> Tuple[Dict[int, bool], Dict[int, bool]]:
    """
    Build maps for a file marking which lines are comments and which are blank.

    Returns:
        (is_comment_line, is_blank_line) mapping line_number -> bool
    """
    is_comment_line: Dict[int, bool] = {}
    is_blank_line: Dict[int, bool] = {}

    # Initialize blank detection by raw lines.
    lines = text.splitlines()
    for idx, raw in enumerate(lines, start=1):
        if raw.strip() == "":
            is_blank_line[idx] = True
        else:
            is_blank_line[idx] = False

    # Tokenize to find comments; this captures full-line comments and inline comments.
    try:
        tokgen = tokenize.generate_tokens(io.StringIO(text).readline)
        for tok_type, tok_str, start, end, _ in tokgen:
            if tok_type == tokenize.COMMENT:
                line_no = start[0]
                # Mark line that contains a comment. We'll treat a line as comment-only if,
                # after removing the comment token content from the slice, nothing else remains.
                # For simplicity, mark line as having a comment; we still consider code content
                # to decide if it is code or just a comment.
                is_comment_line[line_no] = True
    except tokenize.TokenError:
        # Fallback: best-effort if tokenization fails
        for idx, raw in enumerate(lines, start=1):
            stripped = raw.strip()
            if stripped.startswith("#"):
                is_comment_line[idx] = True
            else:
                is_comment_line.setdefault(idx, False)

    # Ensure presence for all lines
    for i in range(1, len(lines) + 1):
        is_comment_line.setdefault(i, False)

    return is_comment_line, is_blank_line


def _count_function_physical_code_lines(text: str, start_lineno: int, end_lineno: int) -> int:
    """
    Count code-only physical lines within [start_lineno, end_lineno], excluding:
    - blank lines
    - full-line comments
    Inline comments on code lines are still counted as code.
    """
    is_comment_line, is_blank_line = _build_code_line_set(text)
    count = 0
    for ln in range(start_lineno, end_lineno + 1):
        if is_blank_line.get(ln, False):
            continue
        # Consider a line comment-only if it starts with # ignoring whitespace
        line = text.splitlines()[ln - 1] if ln - 1 < len(text.splitlines()) else ""
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        count += 1
    return count


def _get_function_bounds(node: ast.AST) -> Optional[Tuple[int, int]]:
    """
    Return (start_lineno, end_lineno) for a function/class-like node if available.
    Requires Python 3.8+ for end_lineno on AST nodes.
    """
    if not hasattr(node, "lineno"):
        return None
    if hasattr(node, "end_lineno") and node.end_lineno is not None:
        return int(node.lineno), int(node.end_lineno)
    # Fallback: compute naive end lineno by walking child nodes
    end_lineno = node.lineno
    for child in ast.walk(node):
        if hasattr(child, "lineno"):
            end_lineno = max(end_lineno, getattr(child, "lineno"))
        if hasattr(child, "end_lineno") and getattr(child, "end_lineno") is not None:
            end_lineno = max(end_lineno, getattr(child, "end_lineno"))
    return int(node.lineno), int(end_lineno)


def _qualified_name(stack: List[str], name: str) -> str:
    return ".".join([*stack, name]) if stack else name


def analyze_file(path: str) -> Dict[str, any]:
    """
    Analyze a single Python file and return diagnostics:
    - functions: List of dicts {name, start, end, code_lines}
    - total_functions
    - functions_over_15: subset list
    - file_total_lines
    """
    text = _read_file(path)
    file_total_lines = _file_physical_length(text)

    try:
        tree = ast.parse(text, filename=path)
    except SyntaxError:
        return {
            "path": path,
            "error": "syntax_error",
            "functions": [],
            "total_functions": 0,
            "functions_over_15": [],
            "file_total_lines": file_total_lines,
        }

    functions: List[Dict[str, any]] = []
    stack: List[str] = []

    class Visitor(ast.NodeVisitor):
        def visit_ClassDef(self, node: ast.ClassDef):
            stack.append(node.name)
            self.generic_visit(node)
            stack.pop()

        def visit_FunctionDef(self, node: ast.FunctionDef):
            bounds = _get_function_bounds(node)
            if bounds:
                start, end = bounds
                code_lines = _count_function_physical_code_lines(text, start, end)
                functions.append(
                    {
                        "name": _qualified_name(stack, node.name),
                        "start": start,
                        "end": end,
                        "code_lines": code_lines,
                    }
                )
            self.generic_visit(node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
            bounds = _get_function_bounds(node)
            if bounds:
                start, end = bounds
                code_lines = _count_function_physical_code_lines(text, start, end)
                functions.append(
                    {
                        "name": _qualified_name(stack, node.name),
                        "start": start,
                        "end": end,
                        "code_lines": code_lines,
                    }
                )
            self.generic_visit(node)

    Visitor().visit(tree)

    over_15 = [f for f in functions if f["code_lines"] > 15]
    return {
        "path": path,
        "functions": functions,
        "total_functions": len(functions),
        "functions_over_15": over_15,
        "file_total_lines": file_total_lines,
    }


def walk_python_files(root: str) -> List[str]:
    files: List[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Skip virtual envs or hidden folders if any
        if any(seg.startswith(".") for seg in dirpath.split(os.sep)):
            pass
        for fn in filenames:
            if _is_python_file(fn):
                files.append(os.path.join(dirpath, fn))
    return sorted(files)


def main() -> int:
    root = BASE_DIR  # HealthCheckScriptContainer root
    py_files = walk_python_files(root)

    all_results: List[Dict[str, any]] = []
    total_functions = 0
    total_functions_le_15 = 0
    files_over_400: List[Tuple[str, int]] = []

    print("Line Diagnostics Report - HealthCheckScriptContainer")
    print("Scanning directory:", root)
    print("")

    for f in py_files:
        res = analyze_file(f)
        all_results.append(res)
        total_functions += res["total_functions"]
        total_functions_le_15 += sum(1 for fn in res["functions"] if fn["code_lines"] <= 15)
        if res["file_total_lines"] > 400:
            files_over_400.append((res["path"], res["file_total_lines"]))

    # Report files exceeding 400 lines
    print("Files exceeding 400 total lines:")
    if files_over_400:
        for path, n in files_over_400:
            print(f"  - {os.path.relpath(path, root)}: {n} lines")
    else:
        print("  None")
    print("")

    # Report functions exceeding 15 lines
    print("Functions exceeding 15 physical code lines (per file):")
    any_over = False
    for res in all_results:
        if res["functions_over_15"]:
            any_over = True
            rel = os.path.relpath(res["path"], root)
            print(f"- {rel}")
            for fn in sorted(res["functions_over_15"], key=lambda x: (x["code_lines"], x["name"]), reverse=True):
                print(f"    {fn['name']}  lines={fn['code_lines']}  ({fn['start']}-{fn['end']})")
    if not any_over:
        print("  None")
    print("")

    # Percentage of functions within limit
    pct = (100.0 * total_functions_le_15 / total_functions) if total_functions > 0 else 100.0
    print(f"Total functions: {total_functions}")
    print(f"Functions with length <= 15: {total_functions_le_15}")
    print(f"Percentage within limit (<=15): {pct:.2f}%")

    # Brief per-file summary (optional but helpful)
    print("\nPer-file summary:")
    for res in all_results:
        rel = os.path.relpath(res["path"], root)
        le_15 = sum(1 for fn in res["functions"] if fn["code_lines"] <= 15)
        gt_15 = res["total_functions"] - le_15
        print(f"  {rel}: total_functions={res['total_functions']} le_15={le_15} gt_15={gt_15} file_lines={res['file_total_lines']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
