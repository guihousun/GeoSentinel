from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_SRC = REPO_ROOT / "packages" / "ntl_toolkit" / "src"
if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))

from ntl_toolkit.adapters.mcp.gis_core import main as package_main


def main() -> None:
    package_main()


if __name__ == "__main__":
    main()
