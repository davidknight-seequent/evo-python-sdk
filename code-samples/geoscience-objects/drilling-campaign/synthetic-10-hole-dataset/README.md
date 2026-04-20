# Synthetic 10-Hole Drilling Campaign Dataset

This dataset provides a synthetic drilling campaign that matches the CSV formats used by the drilling campaign notebooks in this directory.

It was generated from a fixed random seed so the values look realistic while remaining reproducible.

The interim survey files are generated deterministically by `generate_interim_dataset.py` from the original sparse anchor picks.

## Files

- `planned.csv`: 10 collar and planned rows with coordinates, orientation, deviation rates, extension, and target depth.
- `interim.csv`: 110 downhole survey rows for the same 10 holes, with 11 survey points per hole.
- `interim-progressive/`: 20 cumulative interim snapshots named `interim_00.csv` to `interim_19.csv`.

## Snapshot Pattern

- `interim_00.csv` contains only the header row.
- `interim_01.csv` contains the collar row for each hole.
- Each subsequent file adds 5 or 6 survey rows while remaining cumulative.
- `interim_19.csv` matches the full `interim.csv` dataset.

## Notebook Usage

From either notebook folder below, use `../synthetic-10-hole-dataset` as the input directory:

- `create-a-drilling-campaign/`
- `update-a-drilling-campaign/`

For the progressive update notebook, set `progress_input_path` to `../synthetic-10-hole-dataset/interim-progressive`.

To regenerate the interim survey files, run `python generate_interim_dataset.py` from this directory.