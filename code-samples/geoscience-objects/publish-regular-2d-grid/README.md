# Publish Regular 2D Grid

This example demonstrates a complete workflow for publishing a Regular 2D Grid geoscience object to Evo, and associating an extracted colormap with that object:

1. **Authenticate** with Evo using your app credentials
2. **Read a Geosoft grid** (`.grd`) file
3. **Build and publish** a Regular 2D Grid object to Evo
4. **Extract and publish** a colormap from the grid
5. **Associate the colormap** with the published object
6. **Open the object in the Evo Portal** to verify the result

## Overview

The notebook uses Geosoft GX for Python together with the Evo Python SDK to convert `sample-data/Magnetics.grd` into an Evo geoscience object, then create and attach a matching Evo colormap.

The workflow shows how to:

- Create authenticated SDK clients (`ObjectAPIClient` and data client)
- Read grid metadata and values from a Geosoft grid file
- Build a `Regular2DGrid` object from grid dimensions and values
- Publish a colormap via the Evo Colormap API
- Associate the new colormap with an object attribute

## Dataset Characteristics

This sample uses the following input files:

- `sample-data/Magnetics.grd`
- `sample-data/Magnetics.grd.gi`
- `sample-data/Magnetics.grd.xml`

The notebook publishes:

- A **Regular 2D Grid** geoscience object
- A **continuous colormap** derived from the grid color map
- An **object-colormap association** in the target workspace

## Object Schema

This notebook builds a Regular 2D Grid object using the `evo-schemas` models and also demonstrates direct Evo Colormap API integration.

Key components in the workflow include:

- Grid dimensions and cell spacing metadata
- Grid value arrays saved as referenced Parquet data
- Colormap conversion (`attribute_controls`, `colors`, `gradient_controls`)
- Colormap association by attribute key

## Requirements

- Python 3.10+
- Seequent account with Evo entitlement
- Evo application credentials (client ID and redirect URL)
- Geosoft GX for Python

## Platform Notes

Geosoft GX for Python is currently Windows-only. If you are on macOS or Linux, run this notebook in a Windows environment.

Tip for Apple silicon macOS users: you can run Windows in a VM, for example with VMware Fusion.

## Quick Start

1. Open `publish-regular-2d-grid.ipynb` in Jupyter
2. Update `client_id` and `redirect_url` with your Evo app credentials
3. Confirm Geosoft GX is available in your Python environment
4. Run all notebook cells to publish the grid object and colormap
5. Use the generated link to open the object in the Evo Portal


