from __future__ import annotations

import csv
import io
from math import floor
from pathlib import Path


ANCHOR_CSV = """Holeid,depth,dip,azimuth,Wolfpass GM
synth_001,0.0,52.075185,172.646562,Colluvium
synth_002,0.0,48.493099,225.47002,Recent
synth_003,0.0,64.94365,216.239782,Recent
synth_004,0.0,76.923558,293.598184,Colluvium
synth_005,0.0,53.281276,165.27637,Colluvium
synth_006,0.0,66.375381,132.989445,Recent
synth_007,0.0,79.647714,97.620315,Recent
synth_008,0.0,68.119751,250.542394,Colluvium
synth_009,0.0,61.525144,256.661819,Colluvium
synth_010,0.0,79.28049,255.778429,Recent
synth_001,128.2,51.642952,173.535128,Volcaniclastic
synth_002,109.3,48.652194,222.667475,Weathered Andesite
synth_003,143.7,64.370195,218.832821,Saprolite
synth_004,130.7,77.31532,294.716459,Saprolite
synth_005,81.9,53.938109,167.307903,Volcaniclastic
synth_006,131.9,67.248673,134.81539,Weathered Andesite
synth_007,169.6,79.449178,98.423828,Saprolite
synth_008,175.1,68.716871,248.343497,Saprolite
synth_009,185.9,60.873696,257.157722,Volcaniclastic
synth_010,94.6,79.217052,253.577251,Weathered Andesite
synth_001,256.3,51.030603,174.740791,Dacite
synth_002,218.5,48.799481,225.008337,Andesite
synth_003,287.3,64.139692,218.309939,Dacite
synth_004,261.4,77.102982,296.531662,Andesite
synth_005,163.8,53.614349,166.010048,Dacite
synth_006,263.8,67.033724,132.706638,Andesite
synth_007,339.2,79.308485,99.214142,Dacite
synth_008,350.1,68.040759,249.186573,Andesite
synth_009,371.8,60.82705,256.99013,Dacite
synth_010,189.2,79.861503,254.223587,Andesite
synth_001,384.5,51.079323,174.958877,Porphyry
synth_002,327.8,48.361092,225.202218,Diorite
synth_003,431.0,64.058333,218.966794,Monzonite
synth_004,392.2,77.595085,297.115067,Diorite
synth_005,245.7,53.062322,166.482528,Porphyry
synth_006,395.7,67.113888,133.180041,Diorite
synth_007,508.9,79.154829,99.074028,Monzonite
synth_008,525.2,67.851008,248.5254,Diorite
synth_009,557.7,60.151531,256.262799,Porphyry
synth_010,283.9,78.853495,253.711697,Diorite
synth_001,512.7,50.774184,174.61435,Quartz Vein
synth_002,437.0,48.915881,226.028581,Magnetite Skarn
synth_003,574.6,63.491198,218.483866,Quartz Vein
synth_004,522.9,77.236343,296.454958,Quartz Vein
synth_005,327.6,54.231266,165.525381,Quartz Vein
synth_006,527.6,66.274561,135.274842,Magnetite Skarn
synth_007,678.5,79.861712,99.957469,Quartz Vein
synth_008,700.3,67.917932,248.787941,Quartz Vein
synth_009,743.7,61.264964,257.482457,Quartz Vein
synth_010,378.5,79.319833,254.339836,Magnetite Skarn
synth_001,640.9,51.798185,173.455435,Quartz Vein
synth_002,546.3,49.389312,224.442445,Magnetite Skarn
synth_003,718.3,63.229702,217.503257,Quartz Vein
synth_004,653.6,76.106366,297.541516,Quartz Vein
synth_005,409.6,53.181488,163.848488,Quartz Vein
synth_006,659.4,65.804077,135.893012,Magnetite Skarn
synth_010,473.1,80.181912,252.239199,Magnetite Skarn
"""

FIELDNAMES = ["Holeid", "depth", "dip", "azimuth", "Wolfpass GM"]
POINTS_PER_HOLE = 11


def parse_anchor_rows() -> tuple[list[str], dict[str, list[dict[str, float | str]]]]:
    holes: list[str] = []
    grouped: dict[str, list[dict[str, float | str]]] = {}

    for row in csv.DictReader(io.StringIO(ANCHOR_CSV)):
        hole_id = row["Holeid"]
        if hole_id not in grouped:
            holes.append(hole_id)
            grouped[hole_id] = []
        grouped[hole_id].append(
            {
                "depth": float(row["depth"]),
                "dip": float(row["dip"]),
                "azimuth": float(row["azimuth"]),
                "Wolfpass GM": row["Wolfpass GM"],
            }
        )

    return holes, grouped


def distribute_points(interval_lengths: list[float], extra_points: int) -> list[int]:
    if extra_points <= 0:
        return [0] * len(interval_lengths)

    counts = [0] * len(interval_lengths)
    remaining = extra_points

    if extra_points >= len(interval_lengths):
        counts = [1] * len(interval_lengths)
        remaining -= len(interval_lengths)

    if remaining <= 0:
        return counts

    total_length = sum(interval_lengths)
    weighted = [remaining * length / total_length for length in interval_lengths]
    floors = [floor(value) for value in weighted]
    counts = [count + extra for count, extra in zip(counts, floors)]

    used = sum(floors)
    remainders = sorted(
        ((weighted[index] - floors[index], index) for index in range(len(interval_lengths))),
        reverse=True,
    )
    for _, index in remainders[: remaining - used]:
        counts[index] += 1

    return counts


def build_rows() -> list[dict[str, float | str]]:
    holes, grouped = parse_anchor_rows()
    progress_rows: list[dict[str, float | str]] = []

    for hole_index, hole_id in enumerate(holes):
        anchors = grouped[hole_id]
        interval_lengths = [
            anchors[index + 1]["depth"] - anchors[index]["depth"]
            for index in range(len(anchors) - 1)
        ]
        inserted_per_interval = distribute_points(interval_lengths, POINTS_PER_HOLE - len(anchors))
        last_depth = anchors[-1]["depth"]

        hole_rows: list[dict[str, float | str]] = []
        for index, left in enumerate(anchors[:-1]):
            hole_rows.append(dict(left))
            right = anchors[index + 1]
            inserts = inserted_per_interval[index]
            for insert_index in range(1, inserts + 1):
                ratio = insert_index / (inserts + 1)
                hole_rows.append(
                    {
                        "depth": round(left["depth"] + (right["depth"] - left["depth"]) * ratio, 1),
                        "dip": left["dip"] + (right["dip"] - left["dip"]) * ratio,
                        "azimuth": left["azimuth"] + (right["azimuth"] - left["azimuth"]) * ratio,
                        "Wolfpass GM": left["Wolfpass GM"],
                    }
                )
        hole_rows.append(dict(anchors[-1]))

        if len(hole_rows) != POINTS_PER_HOLE:
            raise ValueError(f"Expected {POINTS_PER_HOLE} rows for {hole_id}, got {len(hole_rows)}")

        for row in hole_rows:
            progress_rows.append(
                {
                    "Holeid": hole_id,
                    "depth": row["depth"],
                    "dip": row["dip"],
                    "azimuth": row["azimuth"],
                    "Wolfpass GM": row["Wolfpass GM"],
                    "_progress": 0.0 if last_depth == 0 else row["depth"] / last_depth,
                    "_hole_index": hole_index,
                }
            )

    progress_rows.sort(key=lambda row: (row["_progress"], row["_hole_index"], row["depth"]))
    return progress_rows


def format_value(fieldname: str, value: float | str) -> str:
    if fieldname == "depth":
        return f"{value:.1f}"
    if fieldname in {"dip", "azimuth"}:
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def write_csv(path: Path, rows: list[dict[str, float | str]]) -> None:
    with path.open("w", newline="") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: format_value(field, row[field]) for field in FIELDNAMES})


def build_snapshot_counts(total_rows: int) -> list[int]:
    counts = [0, 10]
    for index in range(18):
        counts.append(counts[-1] + (6 if index < 10 else 5))
    if counts[-1] != total_rows:
        raise ValueError(f"Snapshot counts do not cover {total_rows} rows")
    return counts


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    progressive_dir = base_dir / "interim-progressive"
    rows = build_rows()

    write_csv(base_dir / "interim.csv", rows)

    snapshot_counts = build_snapshot_counts(len(rows))
    for index, count in enumerate(snapshot_counts):
        write_csv(progressive_dir / f"interim_{index:02d}.csv", rows[:count])


if __name__ == "__main__":
    main()