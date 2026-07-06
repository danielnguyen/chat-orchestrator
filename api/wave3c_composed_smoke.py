from __future__ import annotations

import asyncio
import json

from services.orchestration_replay import run_wave3c_smoke_report


async def _main() -> int:
    report = await run_wave3c_smoke_report()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["failed_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
