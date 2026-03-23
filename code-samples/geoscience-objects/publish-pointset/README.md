# Publish Pointset

This example demonstrates how to publish a Pointset geoscience object to Evo using the typed objects API in the Evo Python SDK.

The notebook workflow is:

1. Authenticate with Evo using your app credentials.
2. Load a CSV file containing coordinates and mixed attribute types.
3. Prepare the dataframe for the typed `PointSet` API.
4. Create a `PointSetData` object.
5. Publish the pointset directly with `PointSet.create(...)`.
6. Open the created object in Evo.

## Overview

The notebook converts `sample-data/mixed_attributes.csv` into a Pointset object that can be stored and viewed in an Evo workspace.

This example uses the typed SDK to:

- infer supported attribute types from pandas dtypes
- compute the pointset bounding box automatically
- upload the referenced data needed by the object
- create the final geoscience object with a single typed API call

## Dataset

The sample dataset in this folder is `sample-data/mixed_attributes.csv`.

It contains scattered point locations and a mix of attribute types:

- Coordinate columns: `X`, `Y`, `Z`
- Integer attribute: `Integer_Attr`
- Floating-point attribute: `Float_Attr`
- Boolean attribute: `Bool_Attr`
- String attribute: `String_Attr`
- Categorical attribute: `Category_Attr`

The categorical column uses repeated values such as `ore`, `waste`, `cover`, and `mixed` so the SDK can publish it as a category attribute with a lookup table.

## How The Typed SDK Works

The notebook uses:

- `PointSetData` to describe the object to be created
- `PointSet.create(...)` to upload the data and create the object
- `EpsgCode(...)` to define an EPSG-based coordinate reference system

The typed API expects the coordinate columns in the dataframe passed to `PointSetData` to be named `x`, `y`, and `z`, so the notebook renames `X`, `Y`, and `Z` before object creation.

For non-coordinate columns, the SDK infers attribute types from pandas dtypes. In this example the notebook explicitly casts:

- `Bool_Attr` to pandas `bool`
- `String_Attr` to pandas `string`
- `Category_Attr` to pandas `category`

This ensures the typed API publishes the attributes as the intended Evo attribute types.

## Supported Attribute Types In This Workflow

The typed pointset workflow used here supports these inferred attribute types:

- integer
- scalar (floating point)
- bool
- string
- category

If you want a categorical Evo attribute, the source pandas column must use `category` dtype rather than plain string dtype.

## Object Schema Version

The typed `PointSet` class in the current SDK creates pointsets using schema version `1.2.0`.

Pointset `1.3.0` schemas may be available in the installed schema package, but the typed `PointSet` wrapper currently targets `1.2.0` for object creation.

## Requirements

- Python 3.10+
- Seequent account with Evo entitlement
- Evo application credentials (client ID and redirect URL)

## Quick Start

1. Open `sdk-examples.ipynb` in Jupyter.
2. Update `client_id` and `redirect_url` with your Evo app credentials.
3. Run the notebook cells in order.
4. Use the generated Viewer or Portal link to inspect the created object in Evo.
