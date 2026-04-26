#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_JSON = ROOT / "mz_data_v5.json"
DEFAULT_OUTPUT_JSON = ROOT / "mz_data_v5.json"
DEFAULT_REPORT = ROOT / "reports" / "mz_april_supplement_merge_report.json"
LOCATION_KEYS = ["birth", "residence", "enslavement", "worship", "death"]


def normalize_name(value: str | None) -> str:
    return " ".join((value or "").replace(".", "").split()).strip().upper()


def timeline_fingerprint(person: dict[str, Any]) -> tuple[Any, ...]:
    timeline = person.get("timeline") or {}
    return (
        normalize_name(person.get("name")),
        timeline.get("birthYear"),
        timeline.get("birthDate"),
        timeline.get("deathYear"),
        timeline.get("deathDate"),
        timeline.get("ageAtDeathYears"),
    )


def compute_timeline_meta(people: list[dict[str, Any]]) -> dict[str, int | None]:
    meta = {
        "min_year": None,
        "max_year": None,
        "people_with_any_timeline": 0,
        "people_with_start_year": 0,
        "people_with_lifespan_range": 0,
        "estimated_birth_year": 0,
    }
    for person in people:
        timeline = person.get("timeline")
        if not timeline:
            continue
        meta["people_with_any_timeline"] += 1
        if timeline.get("startYear") is not None:
            meta["people_with_start_year"] += 1
        if timeline.get("startYear") is not None and timeline.get("endYear") is not None:
            meta["people_with_lifespan_range"] += 1
        if timeline.get("estimatedBirthYear"):
            meta["estimated_birth_year"] += 1
        for year in (
            timeline.get("startYear"),
            timeline.get("endYear"),
            timeline.get("birthYear"),
            timeline.get("deathYear"),
        ):
            if year is None:
                continue
            if meta["min_year"] is None or year < meta["min_year"]:
                meta["min_year"] = year
            if meta["max_year"] is None or year > meta["max_year"]:
                meta["max_year"] = year
    return meta


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge the April 2026 supplement into the published MZ-FUBS site dataset."
    )
    parser.add_argument("--input-json", type=Path, default=DEFAULT_INPUT_JSON)
    parser.add_argument("--supplement", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()

    base = json.loads(args.input_json.read_text())
    supplement = json.loads(args.supplement.read_text())

    people = deepcopy(base.get("people") or [])
    supplement_people = supplement.get("people") or []

    match_queues: dict[tuple[Any, ...], list[int]] = defaultdict(list)
    for idx, person in enumerate(people):
        match_queues[timeline_fingerprint(person)].append(idx)

    matched_records = 0
    new_records = 0
    backfilled_layers = Counter()
    backfilled_examples: list[dict[str, Any]] = []

    for person in supplement_people:
        fingerprint = timeline_fingerprint(person)
        if match_queues[fingerprint]:
            matched_records += 1
            target = people[match_queues[fingerprint].pop(0)]
            for key in LOCATION_KEYS:
                incoming = person.get(key)
                if incoming and not target.get(key):
                    target[key] = deepcopy(incoming)
                    backfilled_layers[key] += 1
                    if len(backfilled_examples) < 25:
                        backfilled_examples.append(
                            {
                                "name": person.get("name"),
                                "timeline": (person.get("timeline") or {}).get("label"),
                                "layer": key,
                            }
                        )
        else:
            people.append(deepcopy(person))
            new_records += 1

    output = deepcopy(base)
    output["people"] = people
    output["timeline"] = compute_timeline_meta(people)
    output.setdefault("cleaning", {})
    output["cleaning"]["supplement_merge"] = {
        "script": Path(__file__).name,
        "supplement": args.supplement.name,
        "source": supplement.get("source"),
        "note": supplement.get("note"),
        "merged_at_utc": datetime.now(timezone.utc).isoformat(),
        "matched_records": matched_records,
        "new_records_added": new_records,
        "backfilled_layers_by_kind": dict(backfilled_layers),
    }

    args.output_json.write_text(json.dumps(output, indent=2))

    report = {
        "input_json": args.input_json.name,
        "supplement_json": args.supplement.name,
        "output_json": args.output_json.name,
        "existing_people": len(base.get("people") or []),
        "supplement_people": len(supplement_people),
        "matched_records": matched_records,
        "new_records_added": new_records,
        "backfilled_layers_by_kind": dict(backfilled_layers),
        "output_people": len(people),
        "timeline": output["timeline"],
        "backfilled_examples": backfilled_examples,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True))

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
