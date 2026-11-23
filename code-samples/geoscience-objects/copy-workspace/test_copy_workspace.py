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

import hashlib
import json
import sys
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import copy_workspace as copy
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from evo.blockmodels.data import BlockModel, FlexibleGridDefinition, RegularGridDefinition, Version
from evo.blockmodels.endpoints import models
from evo.common import Environment, ServiceUser
from evo.objects import ObjectMetadata, ObjectSchema
from evo.objects.exceptions import ObjectAlreadyExistsError

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
USER = ServiceUser(id=uuid4(), name="Test", email=None)
ENV = Environment(hub_url="https://example.test", org_id=uuid4(), workspace_id=uuid4())


@pytest.fixture
def copy_settings(monkeypatch, tmp_path):
    settings = {
        "EVO_COPY_SOURCE_CLIENT_ID": "production-client",
        "EVO_COPY_SOURCE_REDIRECT_URI": "http://localhost:3000/source",
        "EVO_COPY_SOURCE_IMS_URL": "https://source-ims.example.test",
        "EVO_COPY_SOURCE_DISCOVERY_URL": "https://source-discovery.example.test",
        "EVO_COPY_DESTINATION_CLIENT_ID": "development-client",
        "EVO_COPY_DESTINATION_REDIRECT_URI": "http://localhost:3001/destination",
        "EVO_COPY_DESTINATION_IMS_URL": "https://destination-ims.example.test",
        "EVO_COPY_DESTINATION_DISCOVERY_URL": "https://destination-discovery.example.test",
        "EVO_COPY_OUTPUT_DIR": str(tmp_path / "archive with spaces"),
    }
    monkeypatch.chdir(tmp_path)
    for key in settings:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(sys, "argv", ["copy_workspace.py"])
    monkeypatch.setattr(copy, "__file__", str(tmp_path / "sample" / "copy_workspace.py"))
    (tmp_path / ".env").write_text("\n".join(f'{key}="{value}"' for key, value in settings.items()))
    return settings


def test_arguments_read_every_setting_from_env_file(copy_settings):
    args = copy.arguments()
    assert vars(args) == {
        "source_client_id": copy_settings["EVO_COPY_SOURCE_CLIENT_ID"],
        "source_redirect_uri": copy_settings["EVO_COPY_SOURCE_REDIRECT_URI"],
        "source_ims_url": copy_settings["EVO_COPY_SOURCE_IMS_URL"],
        "source_discovery_url": copy_settings["EVO_COPY_SOURCE_DISCOVERY_URL"],
        "destination_client_id": copy_settings["EVO_COPY_DESTINATION_CLIENT_ID"],
        "destination_redirect_uri": copy_settings["EVO_COPY_DESTINATION_REDIRECT_URI"],
        "destination_ims_url": copy_settings["EVO_COPY_DESTINATION_IMS_URL"],
        "destination_discovery_url": copy_settings["EVO_COPY_DESTINATION_DISCOVERY_URL"],
        "output_dir": Path(copy_settings["EVO_COPY_OUTPUT_DIR"]),
    }


def test_arguments_find_env_file_next_to_script_from_another_directory(copy_settings, tmp_path, monkeypatch):
    script_dir = tmp_path / "sample"
    script_dir.mkdir()
    (tmp_path / ".env").rename(script_dir / ".env")
    monkeypatch.setattr(copy, "__file__", str(script_dir / "copy_workspace.py"))

    args = copy.arguments()
    assert args.source_client_id == copy_settings["EVO_COPY_SOURCE_CLIENT_ID"]
    assert args.destination_client_id == copy_settings["EVO_COPY_DESTINATION_CLIENT_ID"]
    assert args.destination_redirect_uri == copy_settings["EVO_COPY_DESTINATION_REDIRECT_URI"]
    assert args.output_dir == Path(copy_settings["EVO_COPY_OUTPUT_DIR"])


def test_current_directory_env_takes_precedence_over_script_env(copy_settings, tmp_path, monkeypatch):
    script_dir = tmp_path / "sample"
    script_dir.mkdir()
    (script_dir / ".env").write_text("EVO_COPY_SOURCE_CLIENT_ID=other-source\n")
    monkeypatch.setattr(copy, "__file__", str(script_dir / "copy_workspace.py"))

    assert copy.arguments().source_client_id == copy_settings["EVO_COPY_SOURCE_CLIENT_ID"]


def test_argument_precedence_is_command_line_then_environment_then_file(copy_settings, monkeypatch):
    monkeypatch.setenv("EVO_COPY_SOURCE_CLIENT_ID", "environment-client")
    monkeypatch.setenv("EVO_COPY_DESTINATION_CLIENT_ID", "environment-destination")
    monkeypatch.setattr(sys, "argv", ["copy_workspace.py", "--source-client-id", "command-line-client"])
    args = copy.arguments()
    assert args.source_client_id == "command-line-client"
    assert args.destination_client_id == "environment-destination"
    assert args.destination_redirect_uri == copy_settings["EVO_COPY_DESTINATION_REDIRECT_URI"]


def test_environment_variables_work_without_an_env_file(copy_settings, monkeypatch, tmp_path):
    (tmp_path / ".env").unlink()
    for key, value in copy_settings.items():
        monkeypatch.setenv(key, value)
    args = copy.arguments()
    assert args.source_client_id == copy_settings["EVO_COPY_SOURCE_CLIENT_ID"]
    assert args.destination_discovery_url == copy_settings["EVO_COPY_DESTINATION_DISCOVERY_URL"]
    assert args.output_dir == Path(copy_settings["EVO_COPY_OUTPUT_DIR"])


@pytest.mark.parametrize(
    ("name", "value", "option"),
    [
        ("EVO_COPY_SOURCE_CLIENT_ID", "", "--source-client-id"),
        ("EVO_COPY_SOURCE_REDIRECT_URI", "https://example.test/callback", "--source-redirect-uri"),
        ("EVO_COPY_DESTINATION_IMS_URL", "http://example.test", "--destination-ims-url"),
        ("EVO_COPY_OUTPUT_DIR", "", "--output-dir"),
    ],
)
def test_invalid_env_file_setting_is_rejected(copy_settings, tmp_path, capsys, name, value, option):
    copy_settings[name] = value
    (tmp_path / ".env").write_text("\n".join(f'{key}="{item}"' for key, item in copy_settings.items()))
    with pytest.raises(SystemExit) as exc:
        copy.arguments()
    assert exc.value.code == 2
    assert f"argument {option}:" in capsys.readouterr().err


def test_command_line_can_override_invalid_env_file_setting(copy_settings, tmp_path, monkeypatch):
    copy_settings["EVO_COPY_SOURCE_REDIRECT_URI"] = "https://invalid.example.test"
    (tmp_path / ".env").write_text("\n".join(f'{key}="{item}"' for key, item in copy_settings.items()))
    monkeypatch.setattr(sys, "argv", ["copy_workspace.py", "--source-redirect-uri", "http://localhost:3005/callback"])
    assert copy.arguments().source_redirect_uri == "http://localhost:3005/callback"


def test_existing_command_line_usage_works_without_env_file(copy_settings, tmp_path, monkeypatch):
    (tmp_path / ".env").unlink()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "copy_workspace.py",
            "--source-client-id",
            "source",
            "--destination-client-id",
            "destination",
            "--destination-ims-url",
            "https://development-ims.example.test",
            "--destination-discovery-url",
            "https://development-discovery.example.test",
            "--output-dir",
            "~/archive",
        ],
    )
    args = copy.arguments()
    assert args.source_client_id == "source"
    assert args.destination_client_id == "destination"
    assert args.source_redirect_uri == args.destination_redirect_uri == copy.DEFAULT_REDIRECT
    assert args.source_ims_url == copy.DEFAULT_IMS
    assert args.source_discovery_url == copy.DEFAULT_DISCOVERY
    assert args.output_dir == Path("~/archive")


def object_metadata(name="model.json", parent="", classification="pointset"):
    return ObjectMetadata(
        environment=ENV,
        id=uuid4(),
        name=name,
        parent=parent,
        created_at=NOW,
        created_by=USER,
        modified_at=NOW,
        modified_by=USER,
        schema_id=ObjectSchema.from_id(f"/objects/{classification}/1.0.0/{classification}.schema.json"),
        version_id="source-version",
        stage=None,
    )


def version(columns=None, groups=None):
    return Version(
        bm_uuid=uuid4(),
        version_id=2,
        version_uuid=uuid4(),
        parent_version_id=1,
        base_version_id=1,
        geoscience_version_id="source-version",
        created_at=NOW,
        created_by=USER,
        comment="",
        columns=columns or [],
        groups=groups or [],
    )


def block_model(object_id, grid=None):
    span = models.FloatRange(min=0.0, max=1.0)
    return BlockModel(
        environment=ENV,
        id=uuid4(),
        name="model",
        created_at=NOW,
        created_by=USER,
        geoscience_object_id=object_id,
        description="description",
        grid_definition=grid
        or RegularGridDefinition(
            model_origin=[0.0, 0.0, 0.0], rotations=[], n_blocks=[1, 1, 1], block_size=[1.0, 1.0, 1.0]
        ),
        coordinate_reference_system="EPSG:4326",
        size_unit_id="m",
        bbox=models.BBoxXYZ(x_minmax=span, y_minmax=span, z_minmax=span),
        last_updated_at=NOW,
        last_updated_by=USER,
        fill_subblocks=False,
    )


def groups_and_columns():
    root_id, child_id, column_id = uuid4(), uuid4(), uuid4()
    root = models.ResolvedGroup(
        group_uuid=root_id,
        title="Assays",
        resolved_missing_column_policy=models.MissingColumnPolicy.SET_NULL,
        missing_column_policy=models.MissingColumnPolicy.SET_NULL,
        tags={"source": "analysis"},
    )
    child = models.ResolvedGroup(
        group_uuid=child_id,
        title="Hidden",
        parent_group_uuid=root_id,
        is_hidden=True,
        missing_column_policy=models.MissingColumnPolicy.INHERIT,
        resolved_missing_column_policy=models.MissingColumnPolicy.SET_NULL,
    )
    column = models.Column(
        col_id=str(column_id),
        title="Cu",
        data_type=models.DataType.Float64,
        group_uuid=child_id,
        unit_id="ppm",
        tags={"quality": "measured"},
    )
    system = models.Column(col_id="i", title="i", data_type=models.DataType.UInt32)
    return [child, root], [system, column]


def write_indices(path, extra=None):
    values = {
        "i": pa.array([0, 1], type=pa.uint32()),
        "j": pa.array([0, 0], type=pa.uint32()),
        "k": pa.array([0, 0], type=pa.uint32()),
    }
    values.update(extra or {})
    pq.write_table(pa.table(values), path)


def test_data_references_are_validated_and_deduplicated():
    digest = hashlib.sha256(b"data").hexdigest()
    identifier = str(uuid4())
    document = {"nested": [{"data": digest}, {"data": identifier}, {"data": digest}]}
    assert copy.data_references(document) == sorted([digest, identifier])
    with pytest.raises(ValueError, match="Invalid object data reference"):
        copy.data_references({"data": "../outside"})


@pytest.mark.asyncio
async def test_archive_pins_versions_and_downloads_shared_blobs_once(tmp_path):
    contents = b"example blob"
    digest = hashlib.sha256(contents).hexdigest()
    metadata = [object_metadata("first.json"), object_metadata("second.json")]
    calls = []

    class Blob:
        name = digest

        async def download_to_path(self, path, transport, max_workers):
            calls.append(path)
            path.write_bytes(contents)

    class Downloaded:
        def as_dict(self):
            return {"uuid": str(uuid4()), "nested": {"data": digest}}

        def prepare_data_download(self, refs):
            assert refs == [digest]
            return [Blob()]

    client = SimpleNamespace(download_object_by_id=AsyncMock(side_effect=[Downloaded(), Downloaded()]))
    items = await copy.archive_objects(metadata, client, None, None, tmp_path)

    assert client.download_object_by_id.await_args_list[0].kwargs == {"version": "source-version"}
    assert client.download_object_by_id.await_args_list[1].kwargs == {"version": "source-version"}
    assert len(calls) == 1
    assert (tmp_path / "blobs" / digest).read_bytes() == contents
    assert all(item.data_refs == [digest] for item in items)
    assert json.loads((tmp_path / "manifest.jsonl").read_text().splitlines()[0])["blobs"] == {digest: digest}


@pytest.mark.asyncio
async def test_archive_rejects_bad_blob_digest(tmp_path):
    digest = hashlib.sha256(b"expected").hexdigest()

    class Blob:
        name = digest

        async def download_to_path(self, path, transport, max_workers):
            path.write_bytes(b"corrupted")

    downloaded = SimpleNamespace(as_dict=lambda: {"data": digest}, prepare_data_download=lambda refs: [Blob()])
    client = SimpleNamespace(download_object_by_id=AsyncMock(return_value=downloaded))
    with pytest.raises(ValueError, match="SHA-256 integrity"):
        await copy.archive_objects([object_metadata()], client, None, None, tmp_path)


@pytest.mark.asyncio
async def test_publish_skips_existing_and_uploads_before_new_object(tmp_path):
    (tmp_path / "blobs").mkdir()
    digest = hashlib.sha256(b"payload").hexdigest()
    (tmp_path / "blobs" / digest).write_bytes(b"payload")
    document_path = tmp_path / "object.json"
    original = {
        "uuid": str(uuid4()),
        "data": digest,
        "coordinate_reference_system": {"epsg_code": 2193},
        "parts": [
            {"coordinate_reference_system": {"epsg_code": 4326}},
            {"coordinate_reference_system": {"ogc_wkt": "WKT"}},
        ],
    }
    expected = {
        **original,
        "uuid": None,
        "coordinate_reference_system": "unspecified",
        "parts": [{"coordinate_reference_system": "unspecified"}, {"coordinate_reference_system": {"ogc_wkt": "WKT"}}],
    }
    document_path.write_text(json.dumps(original))
    skip = copy.ArchivedObject(
        object_metadata("exists.json", parent="nested"), "nested/exists.json", document_path, [digest]
    )
    new = copy.ArchivedObject(object_metadata("new.json", parent="folder"), "folder/new.json", document_path, [digest])
    duplicate = copy.ArchivedObject(
        object_metadata("new.json", parent="other"), "other/new.json", document_path, [digest]
    )
    events = []
    created_id = uuid4()

    class Upload:
        name = digest

        async def upload_from_path(self, path, transport, max_workers):
            assert path.read_bytes() == b"payload"
            events.append("blob")

    class Client:
        async def prepare_data_upload(self, refs):
            assert refs == [digest]
            yield Upload()

        async def create_geoscience_object(self, path, document):
            assert path == "new.json"
            assert document == expected
            events.append("object")
            return SimpleNamespace(id=created_id)

    copied, skipped = await copy.publish_objects(
        [skip, new, duplicate], Client(), None, ENV, SimpleNamespace(transport=None), tmp_path, {"exists.json"}, set()
    )
    assert (copied, skipped) == (1, 2)
    assert events == ["blob", "object"]
    assert json.loads(document_path.read_text()) == original
    records = [json.loads(line) for line in (tmp_path / "manifest.jsonl").read_text().splitlines()]
    assert [entry["status"] for entry in records] == [
        "publishing",
        "skipped",
        "publishing",
        "published",
        "publishing",
        "skipped",
    ]
    assert records[3]["destination_id"] == str(created_id)
    assert [(entry["path"], entry["destination_path"]) for entry in records[1::2]] == [
        ("nested/exists.json", "exists.json"),
        ("folder/new.json", "new.json"),
        ("other/new.json", "new.json"),
    ]


@pytest.mark.asyncio
async def test_publish_handles_conflict_created_during_transfer(tmp_path):
    document_path = tmp_path / "object.json"
    document_path.write_text('{"uuid": "old"}')
    item = copy.ArchivedObject(object_metadata(), "model.json", document_path, [])

    class Client:
        async def prepare_data_upload(self, refs):
            for ref in refs:
                yield ref

        async def create_geoscience_object(self, path, document):
            raise ObjectAlreadyExistsError(409, "Conflict", {"title": "Already exists"}, None)

    assert await copy.publish_objects([item], Client(), None, ENV, None, tmp_path, set(), set()) == (0, 1)


def test_block_plan_preserves_hidden_groups_and_parent_order():
    groups, columns = groups_and_columns()
    plan = copy.block_plan(version(columns, groups))
    assert [group.title for group in plan.groups] == ["Assays", "Hidden"]
    assert plan.groups[1].parent_group == "Assays"
    assert plan.groups[1].is_hidden
    assert plan.groups[0].missing_column_policy == models.MissingColumnPolicy.SET_NULL
    assert plan.groups[0].tags == {"source": "analysis"}
    assert plan.titles == {columns[1].col_id: f"Assays{plan.separator}Hidden{plan.separator}Cu"}


def test_block_plan_rejects_group_cycles_and_unknown_columns():
    groups, columns = groups_and_columns()
    groups[1].parent_group_uuid = groups[0].group_uuid
    with pytest.raises(ValueError, match="cyclic or missing"):
        copy.block_plan(version(columns, groups))
    unknown = models.Column(col_id="unknown-system-column", title="unknown", data_type=models.DataType.UInt32)
    with pytest.raises(ValueError, match="Unrecognized"):
        copy.block_plan(version([unknown]))


@pytest.mark.asyncio
async def test_block_export_pins_version_and_includes_hidden_columns(tmp_path):
    metadata = object_metadata(classification="block-model")
    model = block_model(metadata.id)
    groups, columns = groups_and_columns()
    bm_version = version(columns, groups)
    cached = tmp_path / "cached.parquet"
    write_indices(cached, {columns[1].col_id: pa.array([1.2, None], type=pa.float64())})
    client = SimpleNamespace(
        get_block_model=AsyncMock(return_value=model),
        get_version=AsyncMock(return_value=bm_version),
        query_block_model_to_cache=AsyncMock(return_value=cached),
    )

    result = await copy.archive_block_model(
        {"block_model_uuid": str(model.id), "block_model_version_uuid": str(bm_version.version_uuid)},
        metadata,
        "model.json",
        client,
        tmp_path,
    )
    args = client.query_block_model_to_cache.await_args.kwargs
    assert args["columns"] == [columns[1].col_id]
    assert args["version_uuid"] == bm_version.version_uuid
    assert args["exclude_null_rows"] is False
    assert args["geometry_columns"] == models.GeometryColumns.indices
    assert pq.read_table(result.data_path).column(columns[1].col_id).to_pylist() == [1.2, None]
    assert (tmp_path / "block-models" / f"{metadata.id}.json").exists()


@pytest.mark.asyncio
async def test_groups_are_created_by_depth(monkeypatch):
    groups, columns = groups_and_columns()
    plan = copy.block_plan(version(columns, groups))
    operations = SimpleNamespace(
        update_block_model_from_latest_version=AsyncMock(
            side_effect=[SimpleNamespace(job_uuid=uuid4()), SimpleNamespace(job_uuid=uuid4())]
        ),
        notify_upload_complete=AsyncMock(),
    )
    jobs = SimpleNamespace(get_job_status=AsyncMock(return_value=SimpleNamespace(job_status=models.JobStatus.COMPLETE)))
    monkeypatch.setattr(copy, "ColumnOperationsApi", lambda connector: operations)
    monkeypatch.setattr(copy, "JobsApi", lambda connector: jobs)
    await copy.create_groups(ENV, None, uuid4(), plan)
    requests = [
        call.kwargs["update_data_lite"].root
        for call in operations.update_block_model_from_latest_version.await_args_list
    ]
    assert [[group.title for group in request.groups.new] for request in requests] == [["Assays"], ["Hidden"]]
    assert requests[1].groups.new[0].parent_group == "Assays"


@pytest.mark.asyncio
async def test_block_upload_maps_ids_to_qualified_titles(monkeypatch, tmp_path):
    groups, columns = groups_and_columns()
    bm_version = version(columns, groups)
    model = block_model(uuid4())
    data = tmp_path / "block.parquet"
    write_indices(data, {columns[1].col_id: pa.array([1.2, None], type=pa.float64())})
    block = copy.ArchivedBlockModel(model, bm_version, copy.block_plan(bm_version), data)
    operations = SimpleNamespace(
        update_block_model_from_latest_version=AsyncMock(
            return_value=SimpleNamespace(job_uuid=uuid4(), upload_url="https://example.test/upload")
        )
    )
    monkeypatch.setattr(copy, "ColumnOperationsApi", lambda connector: operations)
    client = SimpleNamespace(upload_block_model=AsyncMock(), delete_block_model_columns=AsyncMock())
    destination_id = uuid4()

    await copy.upload_block_data(block, destination_id, ENV, None, client, tmp_path)
    request = operations.update_block_model_from_latest_version.await_args.kwargs["update_data_lite"].root
    assert request.columns.new[0].unit_id == "ppm"
    assert request.columns.new[0].tags == {"quality": "measured"}
    assert request.columns.new[0].group == f"Assays{block.plan.separator}Hidden"
    assert request.input_options.column_name_mapping[0].file_column_name == columns[1].col_id
    assert request.input_options.column_name_mapping[0].column_title == block.plan.titles[columns[1].col_id]
    assert request.model_dump(mode="json", exclude_unset=True)["input_options"]["file_format"] == "parquet"
    assert client.upload_block_model.await_args.args[-1] == data
    client.delete_block_model_columns.assert_not_awaited()


@pytest.mark.asyncio
async def test_columnless_subblocked_geometry_uses_temporary_column(monkeypatch, tmp_path):
    grid = FlexibleGridDefinition(
        model_origin=[0.0, 0.0, 0.0],
        rotations=[],
        n_parent_blocks=[2, 1, 1],
        n_subblocks_per_parent=[2, 2, 2],
        parent_block_size=[1.0, 1.0, 1.0],
    )
    model = block_model(uuid4(), grid)
    bm_version = version()
    data = tmp_path / "geometry.parquet"
    geometry = {
        name: pa.array([0, 1], type=pa.uint8())
        for name in ("start_si", "start_sj", "start_sk", "end_si", "end_sj", "end_sk")
    }
    write_indices(data, geometry)
    (tmp_path / ".cache").mkdir()
    block = copy.ArchivedBlockModel(model, bm_version, copy.block_plan(bm_version), data)
    copy.validate_block_data(data, model, block.plan)
    operations = SimpleNamespace(
        update_block_model_from_latest_version=AsyncMock(
            return_value=SimpleNamespace(job_uuid=uuid4(), upload_url="https://example.test/upload")
        )
    )
    monkeypatch.setattr(copy, "ColumnOperationsApi", lambda connector: operations)

    async def upload(model_id, job_id, url, path):
        table = pq.read_table(path)
        assert table.column(copy.PLACEHOLDER_COLUMN).to_pylist() == [False, False]

    client = SimpleNamespace(upload_block_model=AsyncMock(side_effect=upload), delete_block_model_columns=AsyncMock())
    model_id = uuid4()
    await copy.upload_block_data(block, model_id, ENV, None, client, tmp_path)
    request = operations.update_block_model_from_latest_version.await_args.kwargs["update_data_lite"].root
    assert request.geometry_change is True
    client.delete_block_model_columns.assert_awaited_once_with(model_id, [copy.PLACEHOLDER_COLUMN])
    assert list((tmp_path / ".cache").iterdir()) == []


@pytest.mark.asyncio
async def test_failed_block_import_warns_and_propagates(monkeypatch, tmp_path, capsys):
    metadata = object_metadata(classification="block-model")
    model = block_model(metadata.id)
    bm_version = version()
    block = copy.ArchivedBlockModel(model, bm_version, copy.block_plan(bm_version), None)
    item = copy.ArchivedObject(metadata, "model.json", tmp_path / "object.json", [], block)
    client = SimpleNamespace(create_block_model=AsyncMock(return_value=(model, bm_version)))
    monkeypatch.setattr(copy, "create_groups", AsyncMock(side_effect=RuntimeError("group update failed")))
    with pytest.raises(RuntimeError, match="group update failed"):
        await copy.publish_block_model(item, ENV, None, client, tmp_path)
    assert "created but is incomplete" in capsys.readouterr().err
    assert client.create_block_model.call_args.kwargs["coordinate_reference_system"] is None
    assert client.create_block_model.call_args.kwargs["object_path"] is None


@pytest.mark.parametrize(
    ("crs", "expected"), [("EPSG:4326", None), (" epsg:2193", None), ('PROJCS["x"]', 'PROJCS["x"]'), (None, None)]
)
def test_block_model_epsg_is_stripped_but_wkt_is_kept(crs, expected):
    assert copy.strip_block_model_epsg(crs) == expected


@pytest.fixture
def offline_main(monkeypatch, tmp_path):
    source = ENV
    destination = Environment(hub_url="https://dev.example.test", org_id=uuid4(), workspace_id=uuid4())
    monkeypatch.setattr(copy, "AioTransport", lambda **kwargs: Mock())
    monkeypatch.setattr(copy, "OAuthConnector", lambda **kwargs: Mock())
    monkeypatch.setattr(copy, "AuthorizationCodeAuthorizer", lambda *args, **kwargs: SimpleNamespace(login=AsyncMock()))
    selection = AsyncMock(side_effect=[source, destination])
    monkeypatch.setattr(copy, "select_environment", selection)

    class Connector:
        def __init__(self, base_url, transport, authorizer):
            self.transport = transport

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(copy, "APIConnector", Connector)
    monkeypatch.setattr(
        copy, "ObjectAPIClient", lambda *args: SimpleNamespace(list_all_objects=AsyncMock(return_value=[]))
    )
    monkeypatch.setattr(
        copy,
        "BlockModelAPIClient",
        lambda *args, **kwargs: SimpleNamespace(list_all_block_models=AsyncMock(return_value=[])),
    )
    monkeypatch.setattr("builtins.input", lambda prompt: "COPY")
    export = AsyncMock()
    publish = AsyncMock()
    monkeypatch.setattr(copy, "archive_objects", export)
    monkeypatch.setattr(copy, "publish_objects", publish)
    args = Namespace(
        output_dir=tmp_path / "new-archive",
        source_client_id="source",
        destination_client_id="destination",
        source_ims_url="https://ims.example.test",
        destination_ims_url="https://ims.dev.example.test",
        source_discovery_url="https://discovery.example.test",
        destination_discovery_url="https://discovery.dev.example.test",
        source_redirect_uri=copy.DEFAULT_REDIRECT,
        destination_redirect_uri=copy.DEFAULT_REDIRECT,
    )
    return SimpleNamespace(args=args, selection=selection, export=export, publish=publish)


@pytest.mark.asyncio
async def test_export_failure_prevents_all_publication(offline_main):
    offline_main.export.side_effect = RuntimeError("export failed")
    with pytest.raises(RuntimeError, match="export failed"):
        await copy.main(offline_main.args)
    offline_main.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_each_run_archives_into_a_new_subfolder_of_an_existing_output_dir(offline_main):
    dev_env = Environment(hub_url="https://dev.example.test", org_id=uuid4(), workspace_id=uuid4())
    offline_main.args.output_dir.mkdir()
    (offline_main.args.output_dir / "previous.txt").write_text("keep")
    offline_main.export.return_value = []
    offline_main.publish.return_value = (0, 0)
    offline_main.selection.side_effect = [ENV, dev_env, ENV, dev_env]
    await copy.main(offline_main.args)
    await copy.main(offline_main.args)
    archives = [call.args[4] for call in offline_main.export.await_args_list]
    assert len(set(archives)) == 2
    assert all(path.parent == offline_main.args.output_dir.resolve() and path.is_dir() for path in archives)
    assert (offline_main.args.output_dir / "previous.txt").read_text() == "keep"


def test_run_directory_names_are_unique_within_the_same_second(tmp_path):
    now = datetime(2026, 11, 23, 15, 14, 11)
    first = copy.create_run_directory(tmp_path / "archives", now)
    second = copy.create_run_directory(tmp_path / "archives", now)
    assert (first.name, second.name) == ("run-20261123-151411", "run-20261123-151411-2")


@pytest.mark.asyncio
async def test_output_path_that_is_a_file_is_rejected(offline_main):
    offline_main.args.output_dir.write_text("not a folder")
    with pytest.raises(NotADirectoryError):
        await copy.main(offline_main.args)
    offline_main.export.assert_not_awaited()


@pytest.mark.asyncio
async def test_confirmation_is_required_before_export(offline_main, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda prompt: "no")
    await copy.main(offline_main.args)
    offline_main.export.assert_not_awaited()
    offline_main.publish.assert_not_awaited()
    assert not offline_main.args.output_dir.exists()


@pytest.mark.asyncio
async def test_same_workspace_is_rejected_before_export(offline_main):
    offline_main.selection.side_effect = [ENV, ENV]
    with pytest.raises(ValueError, match="must not be the same workspace"):
        await copy.main(offline_main.args)
    offline_main.export.assert_not_awaited()
    offline_main.publish.assert_not_awaited()
    assert not offline_main.args.output_dir.exists()


@pytest.mark.asyncio
async def test_same_hub_is_rejected_even_for_a_different_workspace(offline_main):
    destination = Environment(hub_url=ENV.hub_url + "/", org_id=uuid4(), workspace_id=uuid4())
    offline_main.selection.side_effect = [ENV, destination]
    with pytest.raises(ValueError, match="same hub"):
        await copy.main(offline_main.args)
    offline_main.export.assert_not_awaited()
    offline_main.publish.assert_not_awaited()
    assert not offline_main.args.output_dir.exists()
