#!/usr/bin/env python3
"""
Convenience runner for line_diagnostics with explicit flushing.

This wraps tools/line_diagnostics.py to ensure output is flushed promptly so CI log collectors
can capture the full diagnostic report. It also sets the working directory to the container root
to avoid any relative path issues.
"""
import os
import sys
import runpy

def main() -> int:
    # Ensure working directory is the container root
    here = os.path.dirname(os.path.abspath(__file__))
    container_root = os.path.dirname(here)
    os.chdir(container_root)
    # Run diagnostics module as a script
    try:
        # Run the diagnostics tool
        runpy.run_path(os.path.join(here, "line_diagnostics.py"), run_name="__main__")
        # Ensure stdout flush
        sys.stdout.flush()
        return 0
    except SystemExit as e:
        # Propagate exit codes from diagnostics
        code = int(e.code) if isinstance(e.code, int) else 1
        sys.stdout.flush()
        return code
    except Exception as exc:
        print(f"[runner] diagnostics failed: {exc}", file=sys.stderr)
        sys.stderr.flush()
        return 2

if __name__ == "__main__":
    sys.exit(main())
