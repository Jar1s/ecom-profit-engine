#!/usr/bin/env python3
"""
Convert supplier / dropshipper «BillDetail» Excel export (.xls) into supplier_costs.csv.

Logic lives in ``bill_detail_import`` (same as Vercel ``POST /import-bill-detail``).

Usage:
  python scripts/import_bill_detail_xls.py /path/to/export.xls -o data/supplier_costs.csv
  python scripts/import_bill_detail_xls.py ~/Downloads/file.xls --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from bill_detail_import import read_bill_detail_sheet, bill_detail_dataframe_to_supplier_costs


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("xls_path", type=Path, help="Path to .xls BillDetail export")
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=_ROOT / "data" / "supplier_costs.csv",
        help="Output CSV path (Product, Cost, SKU)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print first rows only, do not write file",
    )
    args = ap.parse_args()

    path = args.xls_path.expanduser()
    if not path.is_file():
        raise SystemExit(f"File not found: {path}")

    raw = read_bill_detail_sheet(path.read_bytes(), path.name)
    out = bill_detail_dataframe_to_supplier_costs(raw)
    if out.empty:
        raise SystemExit("No product lines parsed.")

    if args.dry_run:
        print(out.head(20).to_string(index=False))
        print(f"... ({len(out)} products)")
        return

    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    print(f"Wrote {len(out)} rows to {args.output}")


if __name__ == "__main__":
    main()
