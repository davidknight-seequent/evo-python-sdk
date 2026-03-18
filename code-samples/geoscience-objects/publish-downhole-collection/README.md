# Publish Downhole Collection

This example demonstrates a complete workflow for publishing a Downhole Collection geoscience object to Evo using the Evo Python SDK:

1. **Authenticate** with Evo using your app credentials
2. **Load drilling CSV datasets** for collar, survey, depths, assay, and lithology
3. **Build location components** (hole IDs, coordinates, path, holes, distances)
4. **Build collection tables** (distance and interval collections with attributes)
5. **Upload referenced Parquet data** and create the object in Evo
6. **Open the object in the Evo Portal** to verify the result

## Overview

The notebook converts multiple drilling input tables in `sample-data` into a single Downhole Collection object that can be stored and viewed in an Evo workspace. The workflow shows how to:

- Create authenticated SDK clients (`ObjectAPIClient` and data client)
- Define object metadata (name, path, tags, CRS, hole ID field)
- Build schema-compatible location components from collar and survey data
- Build distance and interval collections from depths, assay, and lithology data
- Convert referenced data to Parquet, upload it, and publish the final object JSON

## Dataset Characteristics

This sample uses five CSV input files:

- `sample-data/Wolfpass_collar.csv`
- `sample-data/Wolfpass_survey.csv`
- `sample-data/Wolfpass_depths.csv`
- `sample-data/Wolfpass_WP_assay.csv`
- `sample-data/Wolfpass_WP_lith.csv`

The notebook maps these into:

- A **location** section (collar/survey geometry and path)
- A **distance table collection** (`Wolfpass_depths`)
- Two **interval table collections** (`Wolfpass_WP_assay`, `Wolfpass_WP_lith`)

## Object Schema

This notebook creates a Downhole Collection object using `DownholeCollection_V1_3_0` from `evo-schemas`.

Key schema-aligned components created in the workflow include:

- Hole ID lookup and values mapping
- Bounding box and collar coordinate arrays
- Survey path and hole offsets/count mapping
- Distance table components for depth-based data
- Interval table components for assay and lithology data

## Requirements

- Python 3.10+
- Seequent account with Evo entitlement
- Evo application credentials (client ID and redirect URL)

## Quick Start

1. Open `publish-downhole-collection.ipynb` in Jupyter
2. Update `client_id` and `redirect_url` with your Evo app credentials
3. Run all notebook cells to create and publish the Downhole Collection object
4. Use the generated link to open the object in the Evo Portal
