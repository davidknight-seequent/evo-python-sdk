# Publish Pointset

This example demonstrates a complete workflow for publishing a Pointset geoscience object to Evo using the Evo Python SDK:

1. **Authenticate** with Evo using your app credentials
2. **Load CSV sample data** and define object metadata
3. **Build Pointset components** for coordinates and attributes
4. **Upload referenced Parquet data** and create the object in Evo
5. **Open the object in the Evo Portal** to verify the result

## Overview

The notebook converts `sample-data/WP_assay.csv` into a Pointset object that can be stored and viewed in an Evo workspace. The workflow shows how to:

- Create authenticated SDK clients (`ObjectAPIClient` and data client)
- Define Pointset metadata (name, path, tags, CRS)
- Build coordinates and bounding box components
- Convert CSV attribute columns into schema-compatible attribute components
- Upload referenced data and publish the final object JSON

## Dataset Characteristics

The sample dataset in this folder contains assay-style point data with:

- **Coordinate columns**: `X`, `Y`, `Z`
- **Attribute columns used in the notebook**: `Hole ID`, `CU_pct`, `AU_gpt`, `DENSITY`
- **Input file**: `sample-data/WP_assay.csv`

## Object Schema

This notebook creates a Pointset object using the `Pointset_V1_2_0` schema model from `evo-schemas`.

Key schema-aligned elements created in the workflow include:

- Coordinate array (`FloatArray3_V1_0_1`)
- Spatial extent (`BoundingBox_V1_0_1`)
- Continuous and categorical attributes
- Optional CRS metadata (EPSG code)

## Requirements

- Python 3.10+
- Seequent account with Evo entitlement
- Evo application credentials (client ID and redirect URL)

## Quick Start

1. Open `publish-pointset.ipynb` in Jupyter
2. Update `client_id` and `redirect_url` with your Evo app credentials
3. Run all notebook cells to create and publish the Pointset object
4. Use the generated link to open the object in the Evo Portal
