from __future__ import annotations

import json
import os
from collections import defaultdict
from copy import deepcopy
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field


class SourceState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: str = Field(
        pattern=(
            r"^(ready|unavailable|unavailable_after_first|empty|"
            r"empty_after_first|large|malformed)$"
        )
    )


fixture_app = FastAPI(title="Deterministic composed-smoke source fixture")
_calls: dict[str, list[dict[str, Any]]] = defaultdict(list)
_source_modes: dict[str, str] = {}

_GOOGLE_VALUES: dict[str, list[list[str]]] = {
    "targeted-sheet": [
        ["Record", "Status", "Notes"],
        ["migration", "ready", "The migration record confirms the bounded setting."],
        ["migration follow-up", "ready", "A second retained row prevents count-only proof."],
    ],
    "complete-sheet": [
        ["Entry", "Required", "Status"],
        [
            "alpha",
            "yes",
            "reviewed " + "bounded configured detail. " * 100,
        ],
        [
            "beta",
            "yes",
            "reviewed " + "bounded configured detail. " * 100,
        ],
        [
            "gamma",
            "yes",
            "reviewed " + "bounded configured detail. " * 100,
        ],
    ],
    "followup-sheet": [
        ["Record", "Status", "Notes"],
        *[
            [
                f"follow-up-{index}",
                "ready",
                (
                    f"Bounded follow-up detail {index}. "
                    + "Deterministic supporting context. " * 36
                ),
            ]
            for index in range(1, 9)
        ],
    ],
}

_ICS_VALUES: dict[str, str] = {
    "calendar-alpha": """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Composed Fixture//EN
BEGIN:VEVENT
UID:alpha-event
DTSTART:20260810T090000Z
DTEND:20260810T100000Z
SUMMARY:Migration review alpha
DESCRIPTION:Alpha source records the migration review.
LOCATION:Room A
END:VEVENT
END:VCALENDAR
""",
    "calendar-beta": """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Composed Fixture//EN
BEGIN:VEVENT
UID:beta-event
DTSTART:20260811T090000Z
DTEND:20260811T100000Z
SUMMARY:Migration review beta
DESCRIPTION:Beta source records the migration review.
LOCATION:Room B
END:VEVENT
END:VCALENDAR
""",
}


@fixture_app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@fixture_app.post("/fixture/reset")
async def reset_fixture() -> dict[str, str]:
    _calls.clear()
    _source_modes.clear()
    return {"status": "ok"}


@fixture_app.post("/fixture/sources/{source_name}")
async def configure_source(source_name: str, state: SourceState) -> dict[str, str]:
    if source_name not in {*_GOOGLE_VALUES, *_ICS_VALUES}:
        raise HTTPException(status_code=404, detail="unknown fixture source")
    _source_modes[source_name] = state.mode
    return {"status": "ok", "mode": state.mode}


@fixture_app.get("/fixture/calls")
async def fixture_calls() -> dict[str, Any]:
    return {
        "calls": [
            call
            for source_name in sorted(_calls)
            for call in _calls[source_name]
        ]
    }


@fixture_app.get("/google/{spreadsheet_id}")
async def google_values(spreadsheet_id: str, range_name: str = "") -> dict[str, Any]:
    _record_call(spreadsheet_id, "google_values", range_name=range_name)
    call_ordinal = len(_calls[spreadsheet_id])
    mode = _source_modes.get(spreadsheet_id, "ready")
    if mode == "unavailable":
        raise HTTPException(status_code=503, detail="source unavailable")
    if mode == "malformed":
        return {"values": {"invalid": "PRIVATE MALFORMED CELL SENTINEL"}}
    return_empty = mode == "empty" or (
        mode == "empty_after_first" and call_ordinal > 1
    )
    values = [] if return_empty else _GOOGLE_VALUES.get(spreadsheet_id)
    if values is None:
        raise HTTPException(status_code=404, detail="source not found")
    if mode == "large" and spreadsheet_id == "complete-sheet":
        values = deepcopy(values)
        for row in values[1:]:
            row[2] = "reviewed " + "bounded configured detail. " * 130
    return {"values": deepcopy(values)}


@fixture_app.get("/ics/{source_name}.ics")
async def ics_values(source_name: str) -> Response:
    _record_call(source_name, "ics_get")
    mode = _source_modes.get(source_name, "ready")
    if mode == "unavailable" or (
        mode == "unavailable_after_first" and len(_calls[source_name]) > 1
    ):
        raise HTTPException(status_code=503, detail="source unavailable")
    value = "" if mode == "empty" else _ICS_VALUES.get(source_name)
    if value is None:
        raise HTTPException(status_code=404, detail="source not found")
    return Response(content=value, media_type="text/calendar")


def _record_call(source_name: str, operation: str, **fields: Any) -> None:
    _calls[source_name].append(
        {
            "source": source_name,
            "operation": operation,
            "ordinal": len(_calls[source_name]) + 1,
            **fields,
        }
    )


class FixtureGoogleSheetsClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def get_values(self, spreadsheet_id: str, range_name: str) -> list[list[str]]:
        url = (
            f"{self.base_url}/google/{quote(spreadsheet_id, safe='')}"
            f"?range_name={quote(range_name, safe='')}"
        )
        request = Request(url, headers={"Accept": "application/json"})
        try:
            with urlopen(request, timeout=5) as response:  # noqa: S310
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise RuntimeError("fixture source unavailable") from exc
        values = payload.get("values") if isinstance(payload, dict) else None
        if not isinstance(values, list) or any(not isinstance(row, list) for row in values):
            raise RuntimeError("fixture source returned malformed cells")
        return values


def create_dsa_app():
    from app.connectors import base as connector_base
    from app.connectors.google_sheets import GoogleSheetsConnector
    from app.main import create_app

    fixture_base_url = os.environ["COMPOSED_SOURCE_FIXTURE_BASE_URL"]
    connector_base.CONNECTOR_FACTORIES["google_sheets"] = lambda: GoogleSheetsConnector(
        client_factory=lambda _source: FixtureGoogleSheetsClient(fixture_base_url)
    )
    return create_app()
