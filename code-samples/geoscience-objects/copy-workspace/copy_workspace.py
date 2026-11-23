#  Copyright © 2026 Bentley Systems, Incorporated
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Archive and copy the latest objects between two Evo workspaces."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from itertools import groupby
from pathlib import Path, PurePosixPath
from typing import Any, TypeVar
from urllib.parse import urlparse
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import dotenv_values
from pydantic import BaseModel

from evo.aio import AioTransport
from evo.blockmodels import QUALIFIED_TITLE_SEPARATOR, BlockModelAPIClient
from evo.blockmodels.data import (
    BlockModel,
    FlexibleGridDefinition,
    SubBlockedGridDefinition,
    Version,
    get_qualified_title,
)
from evo.blockmodels.endpoints import models as bm_models
from evo.blockmodels.endpoints.api import ColumnOperationsApi, JobsApi
from evo.blockmodels.exceptions import JobFailedException
from evo.common import APIConnector, Environment, ITransport
from evo.common.exceptions import BaseTypedError
from evo.common.utils import Cache
from evo.discovery import DiscoveryAPIClient
from evo.oauth import AuthorizationCodeAuthorizer, EvoScopes, OAuthConnector
from evo.objects import ObjectAPIClient, ObjectMetadata
from evo.objects.exceptions import ObjectAlreadyExistsError
from evo.workspaces import WorkspaceAPIClient, WorkspaceRole

DEFAULT_DISCOVERY = "https://discover.api.seequent.com"
DEFAULT_IMS = "https://ims.bentley.com"
DEFAULT_REDIRECT = "http://localhost:3000/signin-callback"
PREVIEW_HEADERS = {"API-Preview": "opt-in"}
SYSTEM_COLUMNS = {
    "i",
    "j",
    "k",
    "sidx",
    "start_si",
    "start_sj",
    "start_sk",
    "end_si",
    "end_sj",
    "end_sk",
    "x",
    "y",
    "z",
    "dx",
    "dy",
    "dz",
    "version_id",
    "sub_block_derivation",
}
PLACEHOLDER_COLUMN = "__evo_copy_geometry__"
SHA256_RE = re.compile(r"[0-9a-fA-F]{64}\Z")
UUID_RE = re.compile(r"(?:[0-9a-fA-F]{32}|[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})\Z")

T = TypeVar("T")


@dataclass(frozen=True)
class BlockPlan:
    columns: list[bm_models.Column]
    groups: list[bm_models.GroupLite]
    titles: dict[str, str]
    separator: str


@dataclass(frozen=True)
class ArchivedBlockModel:
    model: BlockModel
    version: Version
    plan: BlockPlan
    data_path: Path | None


@dataclass(frozen=True)
class ArchivedObject:
    metadata: ObjectMetadata
    path: str
    document_path: Path
    data_refs: list[str]
    block_model: ArchivedBlockModel | None = None


def json_default(value: Any) -> Any:
    if isinstance(value, (UUID, datetime)):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    raise TypeError(f"Cannot serialize {type(value).__name__} to JSON")


def write_json(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as file:
        json.dump(value, file, indent=2, default=json_default)
        file.write("\n")


def record(archive: Path, event: dict[str, Any]) -> None:
    with (archive / "manifest.jsonl").open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, default=json_default) + "\n")


def canonical_path(metadata: ObjectMetadata) -> str:
    parent = metadata.parent.strip("/")
    if parent == ".":
        parent = ""
    return f"{parent}/{metadata.name}" if parent else metadata.name


def data_references(document: Any) -> list[str]:
    """Find the same nested `data` references used by the SDK's object-data client."""
    references: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "data" and isinstance(child, (str, UUID)):
                    name = str(child)
                    if not (SHA256_RE.fullmatch(name) or UUID_RE.fullmatch(name)):
                        raise ValueError(f"Invalid object data reference: {name!r}")
                    references.add(name)
                else:
                    visit(child)
        elif isinstance(value, (list, tuple)):
            for child in value:
                visit(child)

    visit(document)
    return sorted(references)


def file_sha256(path: Path) -> str:
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def select(label: str, values: Sequence[T], describe: Callable[[T], str]) -> T:
    if not values:
        raise ValueError(f"No {label} available to this login")
    print(f"\nSelect {label}:")
    for index, value in enumerate(values, 1):
        print(f"  {index}. {describe(value)}")
    while True:
        answer = input("Number: ").strip()
        if answer.isdigit() and 1 <= int(answer) <= len(values):
            return values[int(answer) - 1]
        print("Enter one of the displayed numbers.")


async def select_environment(
    label: str, discovery_url: str, transport: ITransport, authorizer: AuthorizationCodeAuthorizer
) -> Environment:
    async with APIConnector(discovery_url, transport, authorizer) as connector:
        organizations = await DiscoveryAPIClient(connector).list_organizations()
    organization = select(f"{label} instance", organizations, lambda item: item.display_name)
    hub = select(f"{label} hub", organization.hubs, lambda item: f"{item.display_name} ({item.url})")
    async with APIConnector(hub.url, transport, authorizer) as connector:
        workspaces = await WorkspaceAPIClient(connector, organization.id).list_all_workspaces()
    workspace = select(
        f"{label} workspace",
        workspaces,
        lambda item: f"{item.display_name} ({item.user_role.name if item.user_role else 'role unknown'})",
    )
    if label == "destination" and workspace.user_role is not None and WorkspaceRole.editor not in workspace.user_role:
        raise PermissionError("The destination workspace requires an editor or owner role")
    return workspace.get_environment()


def block_plan(version: Version) -> BlockPlan:
    titles = [group.title for group in version.groups] + [column.title for column in version.columns]
    separator = QUALIFIED_TITLE_SEPARATOR if all(QUALIFIED_TITLE_SEPARATOR not in title for title in titles) else "/"
    if any(separator in title for title in titles):
        raise ValueError("Cannot find a safe separator for block-model column and group titles")

    groups_by_id = {group.group_uuid: group for group in version.groups}
    for group in version.groups:
        seen: set[UUID] = set()
        current = group
        while current.parent_group_uuid is not None:
            if current.group_uuid in seen or current.parent_group_uuid not in groups_by_id:
                raise ValueError("Block model has a cyclic or missing column-group parent")
            seen.add(current.group_uuid)
            current = groups_by_id[current.parent_group_uuid]

    group_paths = {
        group.group_uuid: version.qualified_group_title(group, separator=separator) for group in version.groups
    }
    groups = [
        bm_models.GroupLite(
            title=group.title,
            parent_group=group_paths[group.parent_group_uuid] if group.parent_group_uuid is not None else None,
            missing_column_policy=group.missing_column_policy,
            is_hidden=group.is_hidden,
            tags=group.tags,
        )
        for group in version.groups
    ]
    groups.sort(key=lambda group: 0 if group.parent_group is None else 1 + group.parent_group.count(separator))

    columns: list[bm_models.Column] = []
    qualified_titles: dict[str, str] = {}
    seen_titles: set[str] = set()
    for column in version.columns:
        if column.col_id in SYSTEM_COLUMNS:
            continue
        try:
            UUID(column.col_id)
        except ValueError as exc:
            raise ValueError(f"Unrecognized block-model column ID: {column.col_id!r}") from exc
        if column.group_uuid is not None and column.group_uuid not in group_paths:
            raise ValueError(f"Missing group for block-model column {column.title!r}")
        group_path = group_paths.get(column.group_uuid) if column.group_uuid is not None else None
        title = get_qualified_title(group_path, column.title, separator=separator)
        if title in seen_titles:
            raise ValueError(f"Duplicate qualified block-model column title: {title!r}")
        columns.append(column)
        qualified_titles[column.col_id] = title
        seen_titles.add(title)
    return BlockPlan(columns=columns, groups=groups, titles=qualified_titles, separator=separator)


def validate_block_data(path: Path, model: BlockModel, plan: BlockPlan) -> None:
    schema = pq.ParquetFile(path).schema_arrow
    if len(set(schema.names)) != len(schema.names):
        raise ValueError(f"Duplicate columns in block-model export: {path}")
    geometry = {"i", "j", "k"}
    if isinstance(model.grid_definition, FlexibleGridDefinition):
        geometry.update({"start_si", "start_sj", "start_sk", "end_si", "end_sj", "end_sk"})
    elif isinstance(model.grid_definition, SubBlockedGridDefinition):
        geometry.add("sidx")
    missing = (geometry | set(plan.titles)) - set(schema.names)
    if missing:
        raise ValueError(f"Block-model export is missing columns: {sorted(missing)}")
    for name in geometry:
        data_type = schema.field(name).type
        expected = pa.uint8() if name.startswith(("start_s", "end_s")) else pa.uint32()
        if data_type != expected:
            raise ValueError(f"Block-model geometry column {name!r} has type {data_type}, expected {expected}")


async def archive_block_model(
    document: dict[str, Any],
    metadata: ObjectMetadata,
    path: str,
    client: BlockModelAPIClient,
    archive: Path,
) -> ArchivedBlockModel:
    model_id = UUID(str(document["block_model_uuid"]))
    model = await client.get_block_model(model_id)
    if model.geoscience_object_id != metadata.id:
        raise ValueError(f"{path}: block-model reference is not the model's service-managed reference object")
    if PurePosixPath(path).name != f"{model.name}.json":
        raise ValueError(f"{path}: the block model name cannot reproduce its reference-object path")

    version_id = document.get("block_model_version_uuid")
    if version_id is None:
        versions = await client.list_versions(model_id)
        if not versions:
            raise ValueError(f"{path}: the block model has no available version")
        version_uuid = versions[0].version_uuid
        print(f"  {path}: reference has no version; using the latest block-model version.")
    else:
        version_uuid = UUID(str(version_id))
    version = await client.get_version(model_id, version_uuid)
    plan = block_plan(version)

    model_dir = archive / "block-models"
    model_dir.mkdir(exist_ok=True)
    write_json(model_dir / f"{metadata.id}.json", {"model": asdict(model), "version": asdict(version)})

    data_path: Path | None = None
    if plan.columns or isinstance(model.grid_definition, SubBlockedGridDefinition):
        cached_path = await client.query_block_model_to_cache(
            bm_id=model_id,
            columns=[column.col_id for column in plan.columns],
            version_uuid=version_uuid,
            geometry_columns=bm_models.GeometryColumns.indices,
            column_headers=bm_models.ColumnHeaderType.id,
            exclude_null_rows=False,
            separator=plan.separator,
        )
        data_path = model_dir / f"{metadata.id}.parquet"
        cached_path.rename(data_path)
        validate_block_data(data_path, model, plan)

    return ArchivedBlockModel(model=model, version=version, plan=plan, data_path=data_path)


async def archive_objects(
    objects: Sequence[ObjectMetadata],
    object_client: ObjectAPIClient,
    block_client: BlockModelAPIClient,
    transport: ITransport,
    archive: Path,
) -> list[ArchivedObject]:
    objects_dir = archive / "objects"
    blobs_dir = archive / "blobs"
    objects_dir.mkdir()
    blobs_dir.mkdir()
    blob_hashes: dict[str, str] = {}
    result: list[ArchivedObject] = []
    for metadata in objects:
        path = canonical_path(metadata)
        print(f"Archiving {path.removesuffix('.json')}")
        downloaded = await object_client.download_object_by_id(metadata.id, version=metadata.version_id)
        document = downloaded.as_dict()
        references = data_references(document)
        document_path = objects_dir / f"{metadata.id}.json"
        write_json(document_path, document)

        for download in downloaded.prepare_data_download(references):
            if download.name not in references:
                raise ValueError(f"Unexpected data reference returned for {path}")
            if download.name not in blob_hashes:
                blob_path = blobs_dir / download.name
                await download.download_to_path(blob_path, transport, max_workers=4)
                digest = file_sha256(blob_path)
                if SHA256_RE.fullmatch(download.name) and digest != download.name.lower():
                    raise ValueError(f"Downloaded blob {download.name} failed its SHA-256 integrity check")
                blob_hashes[download.name] = digest

        block_model = None
        if metadata.schema_id.classification == "objects/block-model":
            block_model = await archive_block_model(document, metadata, path, block_client, archive)

        item = ArchivedObject(metadata, path, document_path, references, block_model)
        result.append(item)
        record(
            archive,
            {
                "status": "archived",
                "path": path,
                "object_id": metadata.id,
                "version_id": metadata.version_id,
                "object_file": str(document_path.relative_to(archive)),
                "blobs": {name: blob_hashes[name] for name in references},
                "block_model_file": f"block-models/{metadata.id}.json" if block_model else None,
                "block_data_file": (
                    str(block_model.data_path.relative_to(archive)) if block_model and block_model.data_path else None
                ),
            },
        )
    return result


async def create_groups(environment: Environment, connector: APIConnector, model_id: UUID, plan: BlockPlan) -> None:
    if not plan.groups:
        return
    operations = ColumnOperationsApi(connector)
    jobs = JobsApi(connector)
    for _, groups in groupby(
        plan.groups, key=lambda group: 0 if group.parent_group is None else 1 + group.parent_group.count(plan.separator)
    ):
        update = bm_models.UpdateDataLite(
            bm_models.UpdateDataLite1(
                columns=bm_models.UpdateColumnsLite(new=[], update=[], delete=[], rename=[]),
                groups=bm_models.UpdateGroupsLite(new=list(groups)),
                qualified_title_separator=plan.separator,
            )
        )
        response = await operations.update_block_model_from_latest_version(
            org_id=str(environment.org_id),
            workspace_id=str(environment.workspace_id),
            bm_id=str(model_id),
            update_data_lite=update,
            additional_headers=PREVIEW_HEADERS,
        )
        await operations.notify_upload_complete(
            org_id=str(environment.org_id),
            workspace_id=str(environment.workspace_id),
            bm_id=str(model_id),
            job_id=str(response.job_uuid),
            additional_headers=PREVIEW_HEADERS,
        )
        while True:
            job = await jobs.get_job_status(
                org_id=str(environment.org_id),
                workspace_id=str(environment.workspace_id),
                bm_id=str(model_id),
                job_id=str(response.job_uuid),
                additional_headers=PREVIEW_HEADERS,
            )
            if job.job_status == bm_models.JobStatus.COMPLETE:
                break
            if job.job_status == bm_models.JobStatus.FAILED:
                if isinstance(job.payload, bm_models.JobErrorPayload):
                    raise JobFailedException(response.job_uuid, job.payload)
                raise RuntimeError(f"Block-model group creation failed: {job.payload}")
            await asyncio.sleep(1)


def add_geometry_placeholder(source: Path, target: Path) -> None:
    parquet = pq.ParquetFile(source)
    schema = parquet.schema_arrow.append(pa.field(PLACEHOLDER_COLUMN, pa.bool_()))
    with pq.ParquetWriter(target, schema, compression="zstd") as writer:
        for batch in parquet.iter_batches(batch_size=65536):
            column = pa.array([False] * batch.num_rows, type=pa.bool_())
            writer.write_batch(batch.append_column(PLACEHOLDER_COLUMN, column))


async def upload_block_data(
    block: ArchivedBlockModel,
    model_id: UUID,
    environment: Environment,
    connector: APIConnector,
    client: BlockModelAPIClient,
    archive: Path,
) -> None:
    if block.data_path is None:
        return
    plan = block.plan
    groups = {
        column.col_id: (
            plan.titles[column.col_id].rsplit(plan.separator, 1)[0] if column.group_uuid is not None else None
        )
        for column in plan.columns
    }
    columns = [
        bm_models.ColumnLite(
            title=column.title,
            data_type=column.data_type,
            unit_id=column.unit_id,
            tags=column.tags,
            group=groups[column.col_id],
        )
        for column in plan.columns
    ]
    mapping = [
        bm_models.ColumnNameMappingGeneric(file_column_name=column.col_id, column_title=plan.titles[column.col_id])
        for column in plan.columns
    ]

    upload_path = block.data_path
    placeholder_path = archive / ".cache" / f"{block.model.id}-geometry.parquet" if not columns else None
    try:
        if placeholder_path is not None:
            add_geometry_placeholder(block.data_path, placeholder_path)
            upload_path = placeholder_path
            columns.append(bm_models.ColumnLite(title=PLACEHOLDER_COLUMN, data_type=bm_models.DataType.Boolean))
        update = bm_models.UpdateDataLite(
            bm_models.UpdateDataLite1(
                columns=bm_models.UpdateColumnsLite(new=columns, update=[], delete=[], rename=[]),
                input_options=bm_models.InputOptionsParquet(file_format="parquet", column_name_mapping=mapping),
                update_type=bm_models.UpdateType.replace,
                geometry_change=True if isinstance(block.model.grid_definition, SubBlockedGridDefinition) else None,
                qualified_title_separator=plan.separator,
            )
        )
        response = await ColumnOperationsApi(connector).update_block_model_from_latest_version(
            org_id=str(environment.org_id),
            workspace_id=str(environment.workspace_id),
            bm_id=str(model_id),
            update_data_lite=update,
            additional_headers=PREVIEW_HEADERS,
        )
        if response.upload_url is None:
            raise RuntimeError("The block-model data update did not return an upload URL")
        await client.upload_block_model(model_id, response.job_uuid, str(response.upload_url), upload_path)
        if placeholder_path is not None:
            await client.delete_block_model_columns(model_id, [PLACEHOLDER_COLUMN])
    finally:
        if placeholder_path is not None:
            placeholder_path.unlink(missing_ok=True)


def strip_epsg_codes(value: Any) -> Any:
    """Replace every EPSG coordinate reference system in an object document with ``"unspecified"``."""
    if isinstance(value, dict):
        return {
            key: "unspecified"
            if key == "coordinate_reference_system" and isinstance(item, dict) and "epsg_code" in item
            else strip_epsg_codes(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [strip_epsg_codes(item) for item in value]
    return value


def strip_block_model_epsg(crs: str | None) -> str | None:
    if crs is not None and crs.strip().upper().startswith("EPSG:"):
        return None
    return crs


def root_path(path: str) -> str:
    """Return the destination path for ``path`` in the workspace root, discarding any source folders."""
    return PurePosixPath(path).name


async def publish_block_model(
    item: ArchivedObject, environment: Environment, connector: APIConnector, client: BlockModelAPIClient, archive: Path
) -> UUID | None:
    block = item.block_model
    if block is None:
        raise ValueError("Expected an archived block model")
    try:
        model, _ = await client.create_block_model(
            name=block.model.name,
            description=block.model.description,
            grid_definition=block.model.grid_definition,
            object_path=None,
            coordinate_reference_system=strip_block_model_epsg(block.model.coordinate_reference_system),
            size_unit_id=block.model.size_unit_id,
            fill_subblocks=block.model.fill_subblocks,
        )
    except BaseTypedError as exc:
        if exc.status == 409 and exc.type_.endswith(("/name-conflict", "/geoscience-object-path-conflict")):
            print(f"Skipping {item.path}: destination block-model name or object path already exists.")
            return None
        print(
            f"Block-model creation failed for {item.path}; check the destination for a partial model.", file=sys.stderr
        )
        raise
    except Exception:
        print(
            f"Block-model creation failed for {item.path}; check the destination for a partial model.", file=sys.stderr
        )
        raise

    try:
        await create_groups(environment, connector, model.id, block.plan)
        await upload_block_data(block, model.id, environment, connector, client, archive)
    except Exception:
        print(
            f"Block model {item.path} was created but is incomplete. Inspect or remove it before retrying; "
            "existing models are skipped.",
            file=sys.stderr,
        )
        raise
    return model.id


async def publish_objects(
    items: Sequence[ArchivedObject],
    object_client: ObjectAPIClient,
    block_client: BlockModelAPIClient,
    environment: Environment,
    connector: APIConnector,
    archive: Path,
    existing_paths: set[str],
    existing_model_names: set[str],
) -> tuple[int, int]:
    copied = skipped = 0
    for item in items:
        target = root_path(item.path)
        print(f"Processing {item.path.removesuffix('.json')}")
        record(
            archive,
            {
                "status": "publishing",
                "path": item.path,
                "destination_path": target,
                "source_object_id": item.metadata.id,
            },
        )
        destination_id = None
        if target in existing_paths:
            print(f"Skipping {item.path}: destination path {target} already exists.")
            success = False
        elif item.block_model and item.block_model.model.name in existing_model_names:
            print(f"Skipping {item.path}: destination block-model name already exists.")
            success = False
        elif item.block_model:
            destination_id = await publish_block_model(item, environment, connector, block_client, archive)
            success = destination_id is not None
            if success:
                existing_model_names.add(item.block_model.model.name)
        else:
            async for upload in object_client.prepare_data_upload(item.data_refs):
                if upload.name not in item.data_refs:
                    raise ValueError(f"Unexpected upload reference returned for {item.path}")
                await upload.upload_from_path(archive / "blobs" / upload.name, connector.transport, max_workers=4)
            document = strip_epsg_codes(json.loads(item.document_path.read_text(encoding="utf-8")))
            document["uuid"] = None
            try:
                created = await object_client.create_geoscience_object(target, document)
            except ObjectAlreadyExistsError:
                print(f"Skipping {item.path}: destination path {target} was created during the copy.")
                success = False
            else:
                success = True
                destination_id = created.id
        if success:
            existing_paths.add(target)
            copied += 1
            print(f"Published {item.path.removesuffix('.json')} as {target.removesuffix('.json')}")
        else:
            skipped += 1
        record(
            archive,
            {
                "status": "published" if success else "skipped",
                "path": item.path,
                "destination_path": target,
                "source_object_id": item.metadata.id,
                "destination_id": destination_id,
                "destination_id_type": "block_model" if item.block_model else "object",
            },
        )
    return copied, skipped


def https_url(value: str) -> str:
    url = urlparse(value)
    if url.scheme != "https" or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise argparse.ArgumentTypeError("Expected an HTTPS base URL without credentials, query, or fragment")
    return value.rstrip("/")


def redirect_url(value: str) -> str:
    url = urlparse(value)
    try:
        port = url.port
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Redirect URI must have a valid port") from exc
    if (
        url.scheme != "http"
        or url.hostname not in {"localhost", "127.0.0.1", "::1"}
        or port is None
        or not url.path
        or url.query
        or url.fragment
        or url.username
        or url.password
    ):
        raise argparse.ArgumentTypeError("Redirect URI must be an HTTP localhost URL with a port and path")
    return value


def nonempty(value: str) -> str:
    if not value.strip():
        raise argparse.ArgumentTypeError("must not be empty")
    return value


def output_path(value: str) -> Path:
    return Path(nonempty(value))


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="Options may also be set using EVO_COPY_* variables in .env in the current directory, "
        "or next to this script if the current directory has no .env. "
        "Command-line options override environment variables, which override .env.",
    )
    env_file = Path.cwd() / ".env"
    if not env_file.exists():
        env_file = Path(__file__).resolve().parent / ".env"
    if env_file.exists() and not env_file.is_file():
        parser.error(f"{env_file} is not a regular file")
    file_values = dotenv_values(env_file) if env_file.exists() else {}

    def setting(name: str, default: str | None = None) -> str | None:
        key = f"EVO_COPY_{name}"
        if key in os.environ:
            return os.environ[key]
        if key in file_values:
            return file_values[key] or ""
        return default

    source_id = setting("SOURCE_CLIENT_ID")
    destination_id = setting("DESTINATION_CLIENT_ID")
    destination_ims = setting("DESTINATION_IMS_URL")
    destination_discovery = setting("DESTINATION_DISCOVERY_URL")
    archive_path = setting("OUTPUT_DIR")
    parser.add_argument(
        "--source-client-id",
        type=nonempty,
        default=source_id,
        required=source_id is None,
        help="Production native OAuth application client ID (EVO_COPY_SOURCE_CLIENT_ID)",
    )
    parser.add_argument(
        "--destination-client-id",
        type=nonempty,
        default=destination_id,
        required=destination_id is None,
        help="Development native OAuth application client ID (EVO_COPY_DESTINATION_CLIENT_ID)",
    )
    parser.add_argument(
        "--source-redirect-uri",
        type=redirect_url,
        default=setting("SOURCE_REDIRECT_URI", DEFAULT_REDIRECT),
        help="Source callback URI (EVO_COPY_SOURCE_REDIRECT_URI)",
    )
    parser.add_argument(
        "--destination-redirect-uri",
        type=redirect_url,
        default=setting("DESTINATION_REDIRECT_URI", DEFAULT_REDIRECT),
        help="Destination callback URI (EVO_COPY_DESTINATION_REDIRECT_URI)",
    )
    parser.add_argument(
        "--source-ims-url",
        type=https_url,
        default=setting("SOURCE_IMS_URL", DEFAULT_IMS),
        help="Source IMS base URL (EVO_COPY_SOURCE_IMS_URL)",
    )
    parser.add_argument(
        "--destination-ims-url",
        type=https_url,
        default=destination_ims,
        required=destination_ims is None,
        help="Destination IMS base URL (EVO_COPY_DESTINATION_IMS_URL)",
    )
    parser.add_argument(
        "--source-discovery-url",
        type=https_url,
        default=setting("SOURCE_DISCOVERY_URL", DEFAULT_DISCOVERY),
        help="Source Discovery base URL (EVO_COPY_SOURCE_DISCOVERY_URL)",
    )
    parser.add_argument(
        "--destination-discovery-url",
        type=https_url,
        default=destination_discovery,
        required=destination_discovery is None,
        help="Destination Discovery base URL (EVO_COPY_DESTINATION_DISCOVERY_URL)",
    )
    parser.add_argument(
        "--output-dir",
        type=output_path,
        default=archive_path,
        required=archive_path is None,
        help="Parent directory for archives; each run creates a new timestamped subfolder (EVO_COPY_OUTPUT_DIR)",
    )
    return parser.parse_args()


def create_run_directory(parent: Path, now: datetime | None = None) -> Path:
    """Create a new, uniquely named run folder inside ``parent``, which may already exist."""
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    stem = f"run-{(now or datetime.now()).strftime('%Y%m%d-%H%M%S')}"
    for attempt in range(1, 1000):
        run = parent / (stem if attempt == 1 else f"{stem}-{attempt}")
        try:
            run.mkdir(mode=0o700)
        except FileExistsError:
            continue
        return run
    raise FileExistsError(f"Could not create a new run folder in {parent}")


async def main(args: argparse.Namespace) -> None:
    output_root = args.output_dir.expanduser().resolve()
    if output_root.exists() and not output_root.is_dir():
        raise NotADirectoryError(f"Output path exists and is not a directory: {output_root}")
    transport = AioTransport(user_agent="Evo workspace object copy")
    scopes = EvoScopes.default | EvoScopes.evo_object | EvoScopes.evo_blocksync | EvoScopes.offline_access
    source_auth = AuthorizationCodeAuthorizer(
        OAuthConnector(transport=transport, client_id=args.source_client_id, base_uri=args.source_ims_url),
        redirect_url=args.source_redirect_uri,
        scopes=scopes,
    )
    destination_auth = AuthorizationCodeAuthorizer(
        OAuthConnector(transport=transport, client_id=args.destination_client_id, base_uri=args.destination_ims_url),
        redirect_url=args.destination_redirect_uri,
        scopes=scopes,
    )
    print("Sign in to the source environment in your browser.")
    await source_auth.login()
    source = await select_environment("source", args.source_discovery_url, transport, source_auth)
    print("\nSign in to the destination environment in your browser.")
    await destination_auth.login()
    destination = await select_environment("destination", args.destination_discovery_url, transport, destination_auth)
    if (source.org_id, source.workspace_id) == (destination.org_id, destination.workspace_id):
        raise ValueError("Source and destination must not be the same workspace")
    if source.hub_url.rstrip("/").casefold() == destination.hub_url.rstrip("/").casefold():
        raise ValueError("Source and destination resolve to the same hub; refusing to write in the source environment")

    async with (
        APIConnector(source.hub_url, transport, source_auth) as source_connector,
        APIConnector(destination.hub_url, transport, destination_auth) as destination_connector,
    ):
        source_client = ObjectAPIClient(source, source_connector)
        destination_client = ObjectAPIClient(destination, destination_connector)
        objects = await source_client.list_all_objects()
        destination_objects = await destination_client.list_all_objects()
        destination_block_client = BlockModelAPIClient(destination, destination_connector, preview=True)
        destination_models = await destination_block_client.list_all_block_models()
        print(
            f"\nSource: {source.hub_url} / {source.org_id} / {source.workspace_id}\n"
            f"Destination: {destination.hub_url} / {destination.org_id} / {destination.workspace_id}\n"
            f"Objects: {len(objects)}\nArchive: new run folder in {output_root}"
        )
        if input("Type COPY to archive these objects and publish missing paths: ").strip() != "COPY":
            print("Cancelled; nothing was copied.")
            return

        archive = create_run_directory(output_root)
        print(f"Archiving to {archive}")
        cache = Cache(archive / ".cache", mkdir=True)
        record(archive, {"status": "started", "source": asdict(source), "destination": asdict(destination)})
        source_block_client = BlockModelAPIClient(source, source_connector, cache=cache, preview=True)
        items = await archive_objects(objects, source_client, source_block_client, transport, archive)
        record(archive, {"status": "archive-complete", "archived": len(items)})
        copied, skipped = await publish_objects(
            items,
            destination_client,
            destination_block_client,
            destination,
            destination_connector,
            archive,
            {canonical_path(item) for item in destination_objects},
            {item.name for item in destination_models},
        )
        record(archive, {"status": "complete", "archived": len(items), "published": copied, "skipped": skipped})
        print(f"\nArchived {len(items)}; published {copied}; skipped {skipped}. Archive: {archive}")


if __name__ == "__main__":
    asyncio.run(main(arguments()))
