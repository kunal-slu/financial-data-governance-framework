from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from examples.build_artifact_bundle import main as build_artifact_bundle


def main() -> None:
    output_dir = ROOT / "examples" / "output_samples"
    build_artifact_bundle(output_dir=output_dir)
    print("Output samples refreshed in", output_dir)


if __name__ == "__main__":
    main()
