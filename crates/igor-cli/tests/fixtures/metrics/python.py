#!/usr/bin/env python3

import json
import sys
from pathlib import Path


def main() -> None:
    context: dict[str, object] = json.load(sys.stdin)
    if context.get("schema_version") != 1:
        raise ValueError("unsupported extractor input")
    stdout_path = context.get("stdout_path")
    if not isinstance(stdout_path, str):
        raise ValueError("missing stdout path")

    stdout = Path(stdout_path).read_text(encoding="utf-8")
    Path("python-summary.txt").write_text(stdout, encoding="utf-8")
    result = {
        "schema_version": 1,
        "status": "success",
        "metrics": {"python bytes": len(stdout.encode("utf-8")), "python score": 0.625},
        "selection": {"split": "validation", "metric": "python score", "value": 0.625},
        "artifacts": [{"path": "python-summary.txt", "role": "metrics"}],
    }
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
