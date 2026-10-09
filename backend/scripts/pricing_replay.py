#!/usr/bin/env python3
"""Replay saved live dual-extracts through parse/quote/G0–G6/calculator.

No LLM calls. Default fixtures: tests/fixtures/pricing_replay (R27 golden).

Usage
    cd backend && python -m scripts.pricing_replay
    python -m scripts.pricing_replay --run r27 --set golden --json /tmp/r.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.pricing.replay import DEFAULT_FIXTURE_DIR, run_replay  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fixture-dir", type=Path, default=DEFAULT_FIXTURE_DIR)
    p.add_argument("--run", default="r27")
    p.add_argument("--set", dest="set_name", default="golden")
    p.add_argument("--include-unrecovered", action="store_true")
    p.add_argument("--json", type=Path, default=None)
    p.add_argument("--gate-rate", type=float, default=None,
                   help="Exit 1 if accepted-correct/scored < this (e.g. 0.6)")
    p.add_argument("--require-zero-wrong", action="store_true")
    args = p.parse_args(argv)

    report = run_replay(
        fixture_dir=args.fixture_dir,
        run=args.run,
        set_name=args.set_name,
        recovered_only=not args.include_unrecovered,
    )
    d = report.to_dict()
    print(
        f"pricing replay [{args.run}/{args.set_name}]: "
        f"accepted_correct={d['accepted_correct']} "
        f"accepted_wrong={d['accepted_wrong']} "
        f"held={d['held']} skipped={d['skipped']} "
        f"scored={d['scored']} rate={d['accept_correct_rate']:.1%} "
        f"holds={d['hold_reasons']}"
    )
    if args.json:
        args.json.write_text(json.dumps(d, indent=2, ensure_ascii=False))

    if args.require_zero_wrong and d["accepted_wrong"] > 0:
        return 1
    if args.gate_rate is not None and d["scored"] > 0:
        if d["accept_correct_rate"] < args.gate_rate:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
