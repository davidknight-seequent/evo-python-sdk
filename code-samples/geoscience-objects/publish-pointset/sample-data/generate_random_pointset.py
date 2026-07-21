"""Generate a deterministic non-drilling sample pointset for this notebook."""

import csv
import math
import random
from pathlib import Path


POINT_COUNT = 1_000
RANDOM_SEED = 20_260_721
CU_NULL_RATE = 0.8
OUTPUT_PATH = Path(__file__).with_name("random_pointset.csv")
ANOMALIES = [
    (445_280, 494_250, 2_960, 150, 80, 1.0),
    (445_660, 494_520, 2_910, 100, 60, 0.75),
    (445_450, 494_050, 3_070, 120, 90, 0.6),
]
POINT_CLUSTERS = [
    (445_280, 494_250, 2_960, 90, 55, 45),
    (445_660, 494_520, 2_910, 70, 45, 35),
    (445_450, 494_050, 3_070, 100, 60, 30),
    (445_780, 494_180, 3_020, 80, 50, 20),
]


def anomaly_strength(x: float, y: float, z: float) -> float:
    """Return the combined influence of the synthetic mineralisation pockets."""
    return sum(
        strength
        * math.exp(
            -0.5
            * (((x - centre_x) / horizontal_radius) ** 2
               + ((y - centre_y) / horizontal_radius) ** 2
               + ((z - centre_z) / vertical_radius) ** 2)
        )
        for centre_x, centre_y, centre_z, horizontal_radius, vertical_radius, strength in ANOMALIES
    )


def lithology(pocket_strength: float, random_source: random.Random) -> str:
    """Assign mineralised and host-rock categories according to anomaly strength."""
    if pocket_strength > 0.5:
        return "Quartz Vein"
    if pocket_strength > 0.15:
        return random_source.choice(["Sericite Alteration", "Chlorite Alteration"])
    return random_source.choice(["Andesite", "Basalt", "Granodiorite", "Rhyolite"])


def random_location(random_source: random.Random) -> tuple[float, float, float]:
    """Sample dense clusters plus a small diffuse population between them."""
    if random_source.random() < 0.9:
        centre_x, centre_y, centre_z, horizontal_spread, vertical_spread, weight = random_source.choices(
            POINT_CLUSTERS, weights=[cluster[-1] for cluster in POINT_CLUSTERS]
        )[0]
        return (
            random_source.gauss(centre_x, horizontal_spread),
            random_source.gauss(centre_y, horizontal_spread * 0.75),
            random_source.gauss(centre_z, vertical_spread),
        )

    angle = random_source.uniform(0, 2 * math.pi)
    radius = 550 * math.sqrt(random_source.random())
    return (
        445_500 + radius * math.cos(angle),
        494_400 + 0.75 * radius * math.sin(angle),
        random_source.uniform(2_850, 3_150),
    )


def main() -> None:
    random_source = random.Random(RANDOM_SEED)
    null_source = random.Random(RANDOM_SEED + 1)

    with OUTPUT_PATH.open("w", newline="") as output_file:
        writer = csv.writer(output_file, lineterminator="\n")
        writer.writerow(["X", "Y", "Z", "Lithology", "CU_pct", "AU_gpt", "DENSITY"])

        for _ in range(POINT_COUNT):
            x, y, z = random_location(random_source)
            pocket_strength = anomaly_strength(x, y, z)
            cu_pct = round(0.1 + 8 * pocket_strength + random_source.lognormvariate(-1, 0.8), 2)
            is_cu_missing = null_source.random() < CU_NULL_RATE
            is_density_missing = random_source.random() < 0.02
            writer.writerow(
                [
                    round(x, 6),
                    round(y, 6),
                    round(z, 6),
                    lithology(pocket_strength, random_source),
                    "" if is_cu_missing else cu_pct,
                    round(0.02 + 25 * pocket_strength**1.25 + random_source.lognormvariate(-1.8, 1), 2),
                    "" if is_density_missing else round(2.4 + 0.8 * pocket_strength + random_source.gauss(0, 0.12), 2),
                ]
            )


if __name__ == "__main__":
    main()