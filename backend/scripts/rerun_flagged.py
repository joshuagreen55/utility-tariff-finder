"""Re-run the pipeline on flagged utilities from Opus audit."""
from scripts.tariff_pipeline import run_pipeline

IDS = [
    50, 73, 147, 211, 233, 364, 576, 663, 667, 668, 712, 768,
    789, 799, 862, 906, 968, 980, 1022, 1057, 1228, 1276, 1311,
    1571, 1623, 1799, 1804,
]


def main():
    total = len(IDS)
    results = []

    for i, uid in enumerate(IDS, 1):
        print(f"\n===== [{i}/{total}] Utility {uid} =====", flush=True)
        try:
            r = run_pipeline(uid, dry_run=False, comprehensive=False)
            tariffs = r.phase4_validation.get("valid", 0) if r.phase4_validation else 0
            errors = r.errors
            status = "OK" if tariffs > 0 else "FAIL"
            print(f"  Result: {status} -- {tariffs} tariffs, {len(errors)} errors", flush=True)
            results.append((uid, status, tariffs))
        except Exception as e:
            print(f"  EXCEPTION: {e}", flush=True)
            results.append((uid, "ERROR", 0))

    print("\n===== SUMMARY =====")
    ok = sum(1 for _, s, _ in results if s == "OK")
    print(f"Success: {ok}/{total}")
    for uid, status, tariffs in results:
        print(f"  {uid}: {status} ({tariffs} tariffs)")


if __name__ == "__main__":
    main()
