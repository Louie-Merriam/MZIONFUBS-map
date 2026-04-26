#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = ROOT / "mz_data_v5.json"
DEFAULT_BEFORE_REV = "e61c268"
LOCATION_KEYS = ["birth", "residence", "enslavement", "worship", "death"]
COUNTRY_TOKENS = {"UNITED STATES", "UNITED STATES OF AMERICA", "USA", "US"}

STATE_NAME_TO_CODE = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
}
STATE_CODES = set(STATE_NAME_TO_CODE.values())
STATE_CENTROIDS = {
    "AL": (32.806671, -86.79113),
    "AK": (61.370716, -152.404419),
    "AZ": (33.729759, -111.431221),
    "AR": (34.969704, -92.373123),
    "CA": (36.116203, -119.681564),
    "CO": (39.059811, -105.311104),
    "CT": (41.597782, -72.755371),
    "DE": (39.318523, -75.507141),
    "DC": (38.9072, -77.0369),
    "FL": (27.766279, -81.686783),
    "GA": (33.040619, -83.643074),
    "HI": (21.094318, -157.498337),
    "ID": (44.240459, -114.478828),
    "IL": (40.349457, -88.986137),
    "IN": (39.849426, -86.258278),
    "IA": (42.011539, -93.210526),
    "KS": (38.5266, -96.726486),
    "KY": (37.66814, -84.670067),
    "LA": (31.169546, -91.867805),
    "ME": (44.693947, -69.381927),
    "MD": (39.045755, -76.641271),
    "MA": (42.230171, -71.530106),
    "MI": (43.326618, -84.536095),
    "MN": (45.694454, -93.900192),
    "MS": (32.741646, -89.678696),
    "MO": (38.456085, -92.288368),
    "MT": (46.921925, -110.454353),
    "NE": (41.12537, -98.268082),
    "NV": (38.313515, -117.055374),
    "NH": (43.452492, -71.563896),
    "NJ": (40.298904, -74.521011),
    "NM": (34.840515, -106.248482),
    "NY": (42.165726, -74.948051),
    "NC": (35.630066, -79.806419),
    "ND": (47.528912, -99.784012),
    "OH": (40.388783, -82.764915),
    "OK": (35.565342, -96.928917),
    "OR": (44.572021, -122.070938),
    "PA": (40.590752, -77.209755),
    "RI": (41.680893, -71.51178),
    "SC": (33.856892, -80.945007),
    "SD": (44.299782, -99.438828),
    "TN": (35.747845, -86.692345),
    "TX": (31.054487, -97.563461),
    "UT": (40.150032, -111.862434),
    "VT": (44.045876, -72.710686),
    "VA": (37.769337, -78.169968),
    "WA": (47.400902, -121.490494),
    "WV": (38.491226, -80.954453),
    "WI": (44.268543, -89.616508),
    "WY": (42.755966, -107.30249),
}

# Exact coordinate repairs for supplement-added rows. Some are copied from the
# existing Georgetown cleaner tables; the remainder are conservative
# representative points within the correct DC/Georgetown block or street.
EXACT_ADDRESS_FIXES = {
    "49 Beall St, Georgetown, DC, United States": (38.9084951, -77.0593482),
    "51 Beall St, Georgetown, DC, United States": (38.9084951, -77.0593482),
    "53 Poplar Alley, Georgetown, DC, United States": (38.90890225, -77.05654947),
    "72 West Street (P Street NW), Georgetown, DC, United States": (38.9086667, -77.0715563),
    "115 High Street, Georgetown, DC, United States": (38.9158362, -77.0678491),
    "225 3rd St NW, Georgetown, DC, United States": (38.90922344, -77.05667237),
    "1018 Jefferson St NW, Georgetown, DC, United States": (38.9035719, -77.0602296),
    "1073 Jefferson Ave NW, Georgetown, DC, United States": (38.9035719, -77.0602296),
    "1121 Reed's Court, Washington, DC, United States": (38.875289, -77.010084),
    "1223 7th St NW, Georgetown, DC, United States": (38.912574, -77.068697),
    "1418 Montgomery Street, near Beall Street, Georgetown, DC, United States": (38.9087494, -77.0572017),
    "16 Greene Alley 18 and 19 F and M, Washington, DC, United States": (38.90115, -77.04345),
    "2723 28 St NW, Georgetown, DC, United States": (38.9091952, -77.05645571),
    "Alley btw Congress and Washington Bridge and Canal Sts, Georgetown, DC, United States": (38.904861, -77.060918),
    "Alley btw M and N 25 and 26th Sts, Georgetown, DC, United States": (38.9045678671, -77.05402556),
    "Alley off Beall below Greene and Montgomery, Georgetown, DC, United States": (38.9084951, -77.0593482),
    "Ally btw U and Canal and 33 and Potomoc, Georgetown, DC, United States": (38.9056702, -77.0653833),
    "Browns Alley bet High and Valley Sts, Georgetown, DC, United States": (38.9137971, -77.06599685),
    "Frederick btw 4th and 5th Sts, Georgetown, DC, United States": (38.9104643, -77.0678609),
    "Frederick btw 6th and 7th Sts, Georgetown, DC, United States": (38.9104643, -77.0678609),
    "Freemans Alley, Btw N and O, 6 and 7th NW, Georgetown, DC, United States": (38.9076, -77.0669),
    "Paxtons Alley Btw Dumbarton Alley and D St 28 and 29 Streets NW, Georgetown, DC, United States": (38.9078212, -77.0578676),
}

NO_LOCALITY_FIXES = {
    "21st St btw L and M Sts": (38.904414, -77.046646),
    "J St Alley": (38.8850799683, -76.97722119),
    "Monroe St btw Dumbarton and Beall Sts": (38.9073769366, -77.06113425),
    "New Hampshire Ave btw F and G Sts": (38.898114, -77.048842),
}

# Historical broad-region fixes that should be preserved in the published data.
# Wikidata lists the Kingdom of Prussia with coordinate location 53N, 14E.
PERSON_LOCATION_FIXES = {
    ("John William Frederick Heibner", "birth"): (53.0, 14.0),
}

PUBLIC_REMOVALS = {
    "Mr. Louis Louie Merriam",
}


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip(" ,")


def normalize_name(value: Any) -> str:
    return clean_text(value)


def person_fingerprint(person: dict[str, Any]) -> tuple[Any, ...]:
    timeline = person.get("timeline") or {}
    return (
        normalize_name(person.get("name")).upper().replace(".", ""),
        timeline.get("birthYear"),
        timeline.get("birthDate"),
        timeline.get("deathYear"),
        timeline.get("deathDate"),
        timeline.get("ageAtDeathYears"),
    )


def load_rev_json(rev: str, path: str) -> dict[str, Any]:
    payload = subprocess.check_output(["git", "show", f"{rev}:{path}"], text=True)
    return json.loads(payload)


def address_parts(address: str) -> list[str]:
    parts = [part.strip() for part in clean_text(address).upper().replace(".", "").split(",") if part.strip()]
    while parts and parts[-1] in COUNTRY_TOKENS:
        parts.pop()
    return parts


def state_code_for_token(token: str) -> str | None:
    cleaned = clean_text(token).upper().replace(".", "")
    cleaned = re.sub(r"^STATE OF\s+", "", cleaned).strip()
    if not cleaned or cleaned == "WASHINGTON":
        return None
    if cleaned in STATE_CODES:
        return cleaned
    return STATE_NAME_TO_CODE.get(cleaned)


def explicit_state_only(address: str) -> str | None:
    parts = address_parts(address)
    if len(parts) != 1:
        return None
    return state_code_for_token(parts[0])


def collect_added_locations(before: dict[str, Any], after: dict[str, Any]) -> list[tuple[int, str]]:
    queues: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for person in before.get("people") or []:
        queues[person_fingerprint(person)].append(person)

    added: list[tuple[int, str]] = []
    for index, person in enumerate(after.get("people") or []):
        fingerprint = person_fingerprint(person)
        if queues[fingerprint]:
            old = queues[fingerprint].pop(0)
            for kind in LOCATION_KEYS:
                if person.get(kind) and not old.get(kind):
                    added.append((index, kind))
        else:
            for kind in LOCATION_KEYS:
                if person.get(kind):
                    added.append((index, kind))
    return added


def set_coords(location: dict[str, Any], lat: float, lon: float) -> bool:
    changed = location.get("lat") != lat or location.get("lon") != lon
    location["lat"] = lat
    location["lon"] = lon
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair erroneous geocodes introduced by the April 2026 supplement.")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--before-rev", default=DEFAULT_BEFORE_REV)
    args = parser.parse_args()

    after = json.loads(args.dataset.read_text())
    before = load_rev_json(args.before_rev, args.dataset.name)
    added_refs = collect_added_locations(before, after)

    changed = 0
    by_rule: dict[str, int] = defaultdict(int)

    for person_index, kind in added_refs:
        location = after["people"][person_index].get(kind)
        if not location:
            continue

        address = clean_text(location.get("address"))
        state_code = explicit_state_only(address)

        if state_code and state_code in STATE_CENTROIDS:
            lat, lon = STATE_CENTROIDS[state_code]
            if set_coords(location, lat, lon):
                changed += 1
                by_rule["state_centroid"] += 1
            continue

        if address in NO_LOCALITY_FIXES:
            lat, lon = NO_LOCALITY_FIXES[address]
            if set_coords(location, lat, lon):
                changed += 1
                by_rule["localized_peer"] += 1
            continue

        if address in EXACT_ADDRESS_FIXES:
            lat, lon = EXACT_ADDRESS_FIXES[address]
            if set_coords(location, lat, lon):
                changed += 1
                by_rule["exact_override"] += 1
            continue

    for person in after.get("people") or []:
        for kind in LOCATION_KEYS:
            target = PERSON_LOCATION_FIXES.get((person.get("name") or "", kind))
            if not target or not person.get(kind):
                continue
            lat, lon = target
            if set_coords(person[kind], lat, lon):
                changed += 1
                by_rule["person_specific_fix"] += 1

    original_count = len(after.get("people") or [])
    after["people"] = [
        person for person in (after.get("people") or [])
        if (person.get("name") or "") not in PUBLIC_REMOVALS
    ]
    removed = original_count - len(after["people"])
    if removed:
        by_rule["public_removal"] += removed

    args.dataset.write_text(json.dumps(after, indent=2))
    print(json.dumps({"changed_locations": changed, "rule_counts": dict(sorted(by_rule.items()))}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
