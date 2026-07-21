// evo_viz front-end module (anywidget ESM).
//
// Renders Evo geoscience objects delivered as OGC 3D Tiles, plus a 3D axis frame with
// numbered tick labels. Bytes for every tile come from Python via a single binary blob, so
// the browser never needs an Evo token and there are no CORS issues.
//
// Rendering stack. The build script bundles these dependencies into widget.bundle.js so
// notebook front-ends never need to load scripts from an external CDN.
//   * three           — scene graph + WebGL renderer
//   * OrbitControls    — mouse/touch camera navigation
//   * CSS2DRenderer    — DOM-based axis + tick labels that always face the camera
//   * 3d-tiles-renderer — streams the tileset

import * as THREE from "three";
import { OrbitControls } from "three/addons/controls/OrbitControls.js";
import { CSS2DObject, CSS2DRenderer } from "three/addons/renderers/CSS2DRenderer.js";
import { TilesRenderer } from "3d-tiles-renderer";

const dependencies = { THREE, OrbitControls, CSS2DRenderer, CSS2DObject, TilesRenderer };
//
// NOTE ON VERSIONS: the exact hook used to feed in-memory bytes to 3d-tiles-renderer depends
// on the library version. This module uses `tiles.manager.setURLModifier(...)` plus a scoped
// `fetch` patch, which covers the 0.3.x line. If you upgrade, check the README for the
// equivalent option.

// --- Global virtual-file registry -------------------------------------------
// Maps a full virtual URL (e.g. https://evo.local/<id>/tileset.json) to a blob: URL.
const g = globalThis;
if (!g.__evoVizRegistry) {
  g.__evoVizRegistry = new Map();
  g.__evoVizPathRegistry = new Map();

  // Scoped fetch patch: serve virtual URLs from the registry, defer everything else.
  const originalFetch = g.fetch.bind(g);
  g.fetch = (input, init) => {
    const url = typeof input === "string" ? input : input && input.url;
    const shouldTrack =
      !!url && (url.includes("/visualization/") || url.includes("/content/") || url.includes("evo.local"));
    if (url) {
      let blobUrl = g.__evoVizRegistry.get(url) || g.__evoVizRegistry.get(url.split("?")[0]);
      if (!blobUrl) {
        try {
          const path = new URL(url).pathname;
          blobUrl = g.__evoVizPathRegistry.get(path);
        } catch {
          // Non-URL or relative path that URL() can't parse.
        }
      }
      if (blobUrl) {
        if (shouldTrack && g.__evoVizActiveStats) g.__evoVizActiveStats.fetchHits += 1;
        return originalFetch(blobUrl, init);
      }
    }
    if (shouldTrack && g.__evoVizActiveStats) g.__evoVizActiveStats.fetchMisses += 1;
    return originalFetch(input, init);
  };
}

function contentTypeFor(path) {
  if (path.endsWith(".json")) return "application/json";
  if (path.endsWith(".glb") || path.endsWith(".b3dm")) return "model/gltf-binary";
  if (path.endsWith(".gltf")) return "model/gltf+json";
  return "application/octet-stream";
}

function toUint8Array(blobValue) {
  if (!blobValue) return null;
  if (blobValue instanceof Uint8Array) return blobValue;
  if (blobValue instanceof ArrayBuffer) return new Uint8Array(blobValue);
  if (ArrayBuffer.isView(blobValue)) {
    return new Uint8Array(blobValue.buffer, blobValue.byteOffset, blobValue.byteLength);
  }
  if (blobValue.buffer && typeof blobValue.byteOffset === "number") {
    return new Uint8Array(blobValue.buffer, blobValue.byteOffset, blobValue.byteLength);
  }
  return null;
}

// --- d3-style "nice" ticks (matches the app's scaleLinear().nice(5).ticks(5)) ----------
function niceTicks(min, max, count) {
  if (!isFinite(min) || !isFinite(max) || min === max) return [min];
  const span = max - min;
  const step0 = span / count;
  const mag = Math.pow(10, Math.floor(Math.log10(step0)));
  const norm = step0 / mag;
  let step;
  if (norm >= 7.5) step = 10 * mag;
  else if (norm >= 3.5) step = 5 * mag;
  else if (norm >= 1.5) step = 2 * mag;
  else step = mag;
  const start = Math.ceil(min / step) * step;
  const ticks = [];
  for (let v = start; v <= max + step * 1e-6; v += step) ticks.push(v);
  return ticks;
}

function formatTick(v) {
  // Always show the full number (no scientific/abbreviated notation), trimming
  // insignificant trailing decimals.
  return Number(v.toFixed(3)).toLocaleString("en-US", {
    useGrouping: false,
    maximumFractionDigits: 3,
  });
}

// --- Attribute-driven colouring ---------------------------------------------------------
// Vertex attributes that are geometry/rendering data, not user-selectable scalars.
const SKIP_ATTRS = new Set([
  "position",
  "normal",
  "tangent",
  "color",
  "uv",
  "uv1",
  "uv2",
  "skinindex",
  "skinweight",
]);

// Colour maps as [position(0..1), r, g, b(0..255)] control stops, linearly interpolated.
const COLORMAPS = {
  viridis: [
    [0.0, 68, 1, 84], [0.13, 71, 44, 122], [0.25, 59, 81, 139], [0.38, 44, 113, 142],
    [0.5, 33, 144, 141], [0.63, 39, 173, 129], [0.75, 92, 200, 99], [0.88, 170, 220, 50],
    [1.0, 253, 231, 37],
  ],
  plasma: [
    [0.0, 13, 8, 135], [0.14, 84, 2, 163], [0.29, 139, 10, 165], [0.43, 185, 50, 137],
    [0.57, 219, 92, 104], [0.71, 244, 136, 73], [0.86, 254, 188, 43], [1.0, 240, 249, 33],
  ],
  inferno: [
    [0.0, 0, 0, 4], [0.14, 31, 12, 72], [0.29, 85, 15, 109], [0.43, 136, 34, 106],
    [0.57, 186, 54, 85], [0.71, 227, 89, 51], [0.86, 249, 140, 10], [1.0, 252, 255, 164],
  ],
  magma: [
    [0.0, 0, 0, 4], [0.14, 28, 16, 68], [0.29, 79, 18, 123], [0.43, 129, 37, 129],
    [0.57, 181, 54, 122], [0.71, 229, 80, 100], [0.86, 251, 135, 97], [1.0, 252, 253, 191],
  ],
  turbo: [
    [0.0, 48, 18, 59], [0.13, 65, 69, 171], [0.25, 57, 118, 229], [0.38, 27, 168, 222],
    [0.5, 43, 206, 163], [0.63, 122, 231, 105], [0.75, 199, 229, 55], [0.88, 250, 181, 50],
    [1.0, 122, 4, 3],
  ],
  coolwarm: [
    [0.0, 59, 76, 192], [0.5, 221, 221, 221], [1.0, 180, 4, 38],
  ],
};

const COLORMAP_NAMES = ["viridis", "plasma", "inferno", "magma", "turbo", "coolwarm", "grayscale"];

// Sample a colour map at t in [0, 1], returning normalized [r, g, b] in 0..1.
function sampleColormap(name, t) {
  t = Math.max(0, Math.min(1, isFinite(t) ? t : 0));
  if (name === "grayscale") return [t, t, t];
  const stops = COLORMAPS[name] || COLORMAPS.viridis;
  for (let i = 0; i < stops.length - 1; i++) {
    const a = stops[i];
    const b = stops[i + 1];
    if (t >= a[0] && t <= b[0]) {
      const f = (t - a[0]) / (b[0] - a[0] || 1);
      return [
        (a[1] + (b[1] - a[1]) * f) / 255,
        (a[2] + (b[2] - a[2]) * f) / 255,
        (a[3] + (b[3] - a[3]) * f) / 255,
      ];
    }
  }
  const last = stops[stops.length - 1];
  return [last[1] / 255, last[2] / 255, last[3] / 255];
}

// A CSS linear-gradient string for the legend bar.
function colormapCss(name) {
  if (name === "grayscale") return "linear-gradient(to right, #000, #fff)";
  const stops = COLORMAPS[name] || COLORMAPS.viridis;
  const parts = stops.map(
    (s) => `rgb(${s[1]},${s[2]},${s[3]}) ${(s[0] * 100).toFixed(0)}%`
  );
  return `linear-gradient(to right, ${parts.join(", ")})`;
}

// --- Evo colormap gradients (fetched per attribute from the colormap service) --------
// Each stop is { position: 0..1, color: [r, g, b] in 0..1 }, sorted by position.
function sampleStops(t, stops) {
  t = Math.max(0, Math.min(1, isFinite(t) ? t : 0));
  if (!stops || !stops.length) return [t, t, t];
  if (t <= stops[0].position) return stops[0].color;
  for (let i = 1; i < stops.length; i++) {
    const a = stops[i - 1];
    const b = stops[i];
    if (t <= b.position) {
      const f = (t - a.position) / (b.position - a.position || 1);
      return [
        a.color[0] + (b.color[0] - a.color[0]) * f,
        a.color[1] + (b.color[1] - a.color[1]) * f,
        a.color[2] + (b.color[2] - a.color[2]) * f,
      ];
    }
  }
  return stops[stops.length - 1].color;
}

// A CSS linear-gradient string built from Evo colormap stops.
function stopsCss(stops) {
  if (!stops || !stops.length) return "linear-gradient(to right, #000, #fff)";
  const parts = stops.map((s) => {
    const r = Math.round(s.color[0] * 255);
    const g = Math.round(s.color[1] * 255);
    const b = Math.round(s.color[2] * 255);
    return `rgb(${r},${g},${b}) ${(s.position * 100).toFixed(1)}%`;
  });
  return `linear-gradient(to right, ${parts.join(", ")})`;
}

// Fallback gradient used when an attribute has no Evo colormap association: viridis,
// expressed as {position, color:[r, g, b] 0..1} stops.
const DEFAULT_STOPS = COLORMAPS.viridis.map((s) => ({
  position: s[0],
  color: [s[1] / 255, s[2] / 255, s[3] / 255],
}));

// User-facing attribute name: drop the glTF custom-attribute underscore prefix.
function displayAttrName(key) {
  return key.replace(/^_+/, "");
}

// --- glb structural-metadata decoding -------------------------------------------------
// three.js / 3d-tiles-renderer 0.3.36 do NOT surface EXT_structural_metadata property
// tables, which is where the Visualisation service stores the real per-vertex attribute
// VALUES. We parse the glb ourselves to recover them, mapping each opaque `attribute_<hash>`
// id (hash == the attribute's stored `values.data` content hash) to a friendly name that
// Python supplied in the manifest.

const GLB_MAGIC = 0x46546c67; // "glTF"
const GLB_CHUNK_JSON = 0x4e4f534a; // "JSON"
const GLB_CHUNK_BIN = 0x004e4942; // "BIN\0"

// EXT_structural_metadata component types -> typed-array constructors.
const META_COMPONENT_ARRAYS = {
  INT8: Int8Array,
  UINT8: Uint8Array,
  INT16: Int16Array,
  UINT16: Uint16Array,
  INT32: Int32Array,
  UINT32: Uint32Array,
  INT64: typeof BigInt64Array !== "undefined" ? BigInt64Array : null,
  UINT64: typeof BigUint64Array !== "undefined" ? BigUint64Array : null,
  FLOAT32: Float32Array,
  FLOAT64: Float64Array,
};

// Evo uses float64-max / infinity as "no data" sentinels. Treat those (and any non-finite
// value) as missing so they neither skew the colour range nor get a colour.
function isNoData(v) {
  return !isFinite(v) || Math.abs(v) >= 1e308;
}

// Split a glb Uint8Array into its parsed JSON document and its BIN chunk.
function parseGlb(bytes) {
  if (!bytes || bytes.length < 12) return null;
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (dv.getUint32(0, true) !== GLB_MAGIC) return null;
  const total = dv.getUint32(8, true);
  let offset = 12;
  let json = null;
  let bin = null;
  while (offset + 8 <= total && offset + 8 <= bytes.length) {
    const chunkLen = dv.getUint32(offset, true);
    const chunkType = dv.getUint32(offset + 4, true);
    const start = offset + 8;
    const chunk = bytes.subarray(start, start + chunkLen);
    if (chunkType === GLB_CHUNK_JSON) {
      json = JSON.parse(new TextDecoder().decode(chunk));
    } else if (chunkType === GLB_CHUNK_BIN) {
      bin = chunk;
    }
    offset = start + chunkLen;
  }
  if (!json) return null;
  return { json, bin };
}

// Copy a bufferView's bytes into a fresh, alignment-safe ArrayBuffer.
function bufferViewCopy(gltf, bin, index) {
  const bv = gltf.bufferViews[index];
  const start = bv.byteOffset || 0;
  return bin.slice(start, start + bv.byteLength);
}

// Read a structural-metadata scalar column as a plain number[] of length `count`.
function readMetaScalarColumn(gltf, bin, prop, componentType, count) {
  const Arr = META_COMPONENT_ARRAYS[componentType];
  if (!Arr || prop == null || prop.values == null) return null;
  const copy = bufferViewCopy(gltf, bin, prop.values);
  const typed = new Arr(copy.buffer, copy.byteOffset, count);
  const isBig = Arr === META_COMPONENT_ARRAYS.INT64 || Arr === META_COMPONENT_ARRAYS.UINT64;
  const out = new Array(count);
  for (let i = 0; i < count; i++) out[i] = isBig ? Number(typed[i]) : typed[i];
  return out;
}

// Read a structural-metadata STRING column (value bytes + stringOffsets) as string[].
function readMetaStringColumn(gltf, bin, prop, count, offsetType) {
  if (prop == null || prop.values == null || prop.stringOffsets == null) return null;
  const OffArr = META_COMPONENT_ARRAYS[offsetType || "UINT32"] || Uint32Array;
  const offCopy = bufferViewCopy(gltf, bin, prop.stringOffsets);
  const offsets = new OffArr(offCopy.buffer, offCopy.byteOffset, count + 1);
  const valBytes = bufferViewCopy(gltf, bin, prop.values);
  const dec = new TextDecoder();
  const out = new Array(count);
  for (let i = 0; i < count; i++) {
    out[i] = dec.decode(valBytes.subarray(Number(offsets[i]), Number(offsets[i + 1])));
  }
  return out;
}

// Per-element reader for a glTF accessor componentType (little-endian).
function accessorGetter(dv, componentType) {
  switch (componentType) {
    case 5120: return (o) => dv.getInt8(o);
    case 5121: return (o) => dv.getUint8(o);
    case 5122: return (o) => dv.getInt16(o, true);
    case 5123: return (o) => dv.getUint16(o, true);
    case 5125: return (o) => dv.getUint32(o, true);
    case 5126: return (o) => dv.getFloat32(o, true);
    default: return () => 0;
  }
}

const GLTF_COMPONENT_BYTES = { 5120: 1, 5121: 1, 5122: 2, 5123: 2, 5125: 4, 5126: 4 };

// Decode a scalar glTF accessor (e.g. _FEATURE_ID_0) into a number[]; honours byteStride.
function readAccessorScalar(gltf, bin, accessorIndex) {
  const acc = gltf.accessors[accessorIndex];
  if (!acc || acc.bufferView == null) return null;
  const elemSize = GLTF_COMPONENT_BYTES[acc.componentType];
  if (!elemSize) return null;
  const bv = gltf.bufferViews[acc.bufferView];
  const bvStart = bv.byteOffset || 0;
  const stride = bv.byteStride || elemSize;
  const copy = bin.slice(bvStart, bvStart + bv.byteLength);
  const dv = new DataView(copy.buffer, copy.byteOffset, copy.byteLength);
  const getter = accessorGetter(dv, acc.componentType);
  const base = acc.byteOffset || 0;
  const out = new Array(acc.count);
  for (let i = 0; i < acc.count; i++) out[i] = getter(base + i * stride);
  return out;
}

// Decode a glb's EXT_structural_metadata into per-vertex attribute arrays.
// `nameByHash` maps `<hash>` -> {name, kind}. Returns { pointCount, attrs: [...] } where
// each attr is {name, kind:"continuous", values:Float32Array, min, max} or
// {name, kind:"category", indices:number[], categories:string[]}.
function decodeGlbAttributes(bytes, nameByHash) {
  const parsed = parseGlb(bytes);
  if (!parsed || !parsed.bin) return null;
  const gltf = parsed.json;
  const bin = parsed.bin;
  const ext = gltf.extensions && gltf.extensions.EXT_structural_metadata;
  if (!ext || !ext.schema || !Array.isArray(ext.propertyTables)) return null;
  const classes = ext.schema.classes || {};

  // Vertex count from the first primitive's POSITION accessor (matches three.js geometry).
  let pointCount = 0;
  for (const mesh of gltf.meshes || []) {
    for (const prim of mesh.primitives || []) {
      const posIdx = prim.attributes && prim.attributes.POSITION;
      if (posIdx != null && gltf.accessors[posIdx]) {
        pointCount = gltf.accessors[posIdx].count;
        break;
      }
    }
    if (pointCount) break;
  }

  // Map each property-table index -> the feature-id accessor that indexes into it (if any).
  const tableFeatureAccessor = {};
  for (const mesh of gltf.meshes || []) {
    for (const prim of mesh.primitives || []) {
      const mf = prim.extensions && prim.extensions.EXT_mesh_features;
      if (!mf || !Array.isArray(mf.featureIds)) continue;
      for (const fid of mf.featureIds) {
        if (fid.propertyTable == null || fid.propertyTable in tableFeatureAccessor) continue;
        let accessorIndex = null;
        if (fid.attribute != null && prim.attributes) {
          accessorIndex =
            prim.attributes["_FEATURE_ID_" + fid.attribute] ??
            prim.attributes["FEATURE_ID_" + fid.attribute] ??
            null;
        }
        tableFeatureAccessor[fid.propertyTable] = accessorIndex;
      }
    }
  }

  const attrs = [];
  const tables = ext.propertyTables;
  for (let ti = 0; ti < tables.length; ti++) {
    const table = tables[ti];
    const cls = classes[table.class] || {};
    const clsProps = cls.properties || {};
    const count = table.count;

    // Category attribute: the attribute hash equals the property table's *class* name.
    if (typeof table.class === "string" && table.class.startsWith("attribute_")) {
      const meta = nameByHash[table.class.slice("attribute_".length)];
      if (meta && meta.kind === "category") {
        const valueDef = clsProps.value || {};
        const categories = readMetaStringColumn(
          gltf, bin, table.properties && table.properties.value, count, valueDef.stringOffsetType
        );
        const accessorIndex = tableFeatureAccessor[ti];
        const indices = accessorIndex != null ? readAccessorScalar(gltf, bin, accessorIndex) : null;
        if (categories && indices) {
          attrs.push({
            name: meta.name,
            kind: "category",
            indices,
            categories,
            colormap: meta.colormap || null,
          });
        }
        continue;
      }
    }

    // Continuous attributes: columns named attribute_<hash>, indexed by vertex id.
    for (const propName of Object.keys(table.properties || {})) {
      if (!propName.startsWith("attribute_")) continue;
      const meta = nameByHash[propName.slice("attribute_".length)];
      if (!meta || meta.kind !== "continuous") continue;
      const propDef = clsProps[propName] || {};
      const column = readMetaScalarColumn(
        gltf, bin, table.properties[propName], propDef.componentType, count
      );
      if (!column) continue;
      const values = new Float32Array(count);
      let min = Infinity;
      let max = -Infinity;
      for (let i = 0; i < count; i++) {
        const v = column[i];
        if (isNoData(v)) {
          values[i] = NaN;
          continue;
        }
        values[i] = v;
        if (v < min) min = v;
        if (v > max) max = v;
      }
      attrs.push({ name: meta.name, kind: "continuous", values, min, max, colormap: meta.colormap || null });
    }
  }

  return { pointCount, attrs };
}

// --- Tile content diagnostics --------------------------------------------------------
// Feature-ID / batch-index accessors are 3D Tiles plumbing, not real Evo attributes.
function isFeatureIdAttr(key) {
  const k = key.toLowerCase().replace(/^_+/, "");
  return /^feature_id(_\d+)?$/.test(k) || k === "batchid" || k === "_batchid";
}

// Best-effort description of an EXT_structural_metadata object as attached by
// 3d-tiles-renderer. Returns the class -> property-name lists it can find.
function describeStructuralMetadata(sm) {
  const out = {};
  try {
    const schema = sm.schema || (sm.metadata && sm.metadata.schema) || null;
    if (schema && schema.classes) {
      out.classes = {};
      for (const cname of Object.keys(schema.classes)) {
        const cls = schema.classes[cname];
        out.classes[cname] = cls && cls.properties ? Object.keys(cls.properties) : [];
      }
    }
    if (typeof sm.getPropertyTableData === "function") out.hasPropertyTables = true;
    if (Array.isArray(sm.tables)) out.tableCount = sm.tables.length;
  } catch (err) {
    out.error = String((err && err.message) || err);
  }
  return out;
}

// Compact structural summary of an arbitrary value: keys, array lengths and typed-array
// shapes, without dumping large binary buffers. Used to reveal raw glTF extension contents.
function shallowShape(value, depth) {
  if (depth < 0) return "\u2026";
  if (value == null) return value;
  if (typeof value !== "object") return value;
  if (ArrayBuffer.isView(value)) {
    return `${value.constructor.name}(${value.length})`;
  }
  if (Array.isArray(value)) {
    if (value.length > 8) return `Array(${value.length})`;
    return value.map((v) => shallowShape(v, depth - 1));
  }
  const out = {};
  for (const k of Object.keys(value)) {
    out[k] = shallowShape(value[k], depth - 1);
  }
  return out;
}

// Walk a loaded tile scene and describe every geometry node: its vertex accessors and
// any metadata extensions (structural metadata, mesh features, legacy batch tables).
function collectModelDiagnostics(modelScene) {
  const report = { nodes: [], sceneUserDataKeys: [] };
  try {
    report.sceneUserDataKeys = modelScene.userData
      ? Object.keys(modelScene.userData)
      : [];
  } catch {
    // ignore
  }
  modelScene.traverse((node) => {
    if (!node || !node.geometry || !node.geometry.attributes) return;
    const geom = node.geometry;
    const attributes = {};
    for (const key of Object.keys(geom.attributes)) {
      const a = geom.attributes[key];
      attributes[key] = {
        itemSize: a.itemSize,
        count: a.count,
        type: a.array && a.array.constructor ? a.array.constructor.name : "?",
      };
    }
    const info = {
      name: node.name || "(unnamed)",
      kind: node.isPoints ? "Points" : node.isMesh ? "Mesh" : node.type,
      attributes,
      userDataKeys: node.userData ? Object.keys(node.userData) : [],
    };
    const sm = node.userData && node.userData.structuralMetadata;
    if (sm) info.structuralMetadata = describeStructuralMetadata(sm);
    if (node.userData && node.userData.meshFeatures) info.meshFeatures = true;
    // Raw glTF extensions three.js did not parse (may hold EXT_mesh_features /
    // EXT_structural_metadata property tables with the real attribute values).
    if (node.userData && node.userData.gltfExtensions) {
      info.gltfExtensionNames = Object.keys(node.userData.gltfExtensions);
      info.gltfExtensions = shallowShape(node.userData.gltfExtensions, 5);
    }
    // Legacy 3D Tiles batch table lives on an ancestor.
    let p = node;
    while (p && !p.batchTable) p = p.parent;
    if (p && p.batchTable && typeof p.batchTable.getKeys === "function") {
      try {
        info.batchTableKeys = p.batchTable.getKeys();
      } catch {
        // ignore
      }
    }
    report.nodes.push(info);
  });
  return report;
}

function makeLabel(text, className, CSS2DObject) {
  const div = document.createElement("div");
  div.className = className;
  div.textContent = text;
  const obj = new CSS2DObject(div);
  obj.center.set(0.5, 0.5);
  return obj;
}

function boxFromBoundingVolumeBox(values, THREE) {
  if (!Array.isArray(values) || values.length !== 12) return null;
  const c = new THREE.Vector3(values[0], values[1], values[2]);
  const ux = new THREE.Vector3(values[3], values[4], values[5]);
  const uy = new THREE.Vector3(values[6], values[7], values[8]);
  const uz = new THREE.Vector3(values[9], values[10], values[11]);
  const e = new THREE.Vector3(
    Math.abs(ux.x) + Math.abs(uy.x) + Math.abs(uz.x),
    Math.abs(ux.y) + Math.abs(uy.y) + Math.abs(uz.y),
    Math.abs(ux.z) + Math.abs(uy.z) + Math.abs(uz.z)
  );
  return new THREE.Box3(c.clone().sub(e), c.clone().add(e));
}

function stripGzSuffix(uri) {
  return typeof uri === "string" ? uri.replace(/\.gz(\?.*)?$/i, "$1") : uri;
}

function normalizeTileUris(tile) {
  if (!tile || typeof tile !== "object") return;
  if (tile.content && tile.content.uri) {
    tile.content.uri = stripGzSuffix(tile.content.uri);
  }
  if (Array.isArray(tile.contents)) {
    for (const c of tile.contents) {
      if (c && c.uri) c.uri = stripGzSuffix(c.uri);
    }
  }
  if (Array.isArray(tile.children)) {
    for (const child of tile.children) normalizeTileUris(child);
  }
}

// --- Axis frame + tick labels, built from a bounding box -------------------------------
// Mirrors the app: three axis lines from the min corner, axis titles at the mid-point of
// each axis, and numbered tick labels along each axis.
function buildAxes(box, axisLabels, tickCount, THREE, CSS2DObject) {
  const group = new THREE.Group();
  const { min, max } = box;

  const colors = { x: 0xff5555, y: 0x55ff55, z: 0x5588ff };
  const axisDefs = [
    { key: "x", to: new THREE.Vector3(max.x, min.y, min.z) },
    { key: "y", to: new THREE.Vector3(min.x, max.y, min.z) },
    { key: "z", to: new THREE.Vector3(min.x, min.y, max.z) },
  ];
  const originV = new THREE.Vector3(min.x, min.y, min.z);

  for (const { key, to } of axisDefs) {
    const geom = new THREE.BufferGeometry().setFromPoints([originV, to]);
    const line = new THREE.Line(
      geom,
      new THREE.LineBasicMaterial({ color: colors[key] })
    );
    group.add(line);
  }

  // Axis titles at the far (max) end of each axis line, nudged slightly past the tip.
  const pad = {
    x: (max.x - min.x) * 0.06 || 1,
    y: (max.y - min.y) * 0.06 || 1,
    z: (max.z - min.z) * 0.06 || 1,
  };
  const ends = {
    x: new THREE.Vector3(max.x + pad.x, min.y, min.z),
    y: new THREE.Vector3(min.x, max.y + pad.y, min.z),
    z: new THREE.Vector3(min.x, min.y, max.z + pad.z),
  };
  const titles = { x: axisLabels[0] || "X", y: axisLabels[1] || "Y", z: axisLabels[2] || "Z" };
  for (const key of ["x", "y", "z"]) {
    const label = makeLabel(titles[key], "evo-axis-label", CSS2DObject);
    label.position.copy(ends[key]);
    group.add(label);
  }

  // Tick labels along each axis, at the min corner of the other two dimensions.
  const addTicks = (values, positionFn) => {
    for (const v of values) {
      const label = makeLabel(formatTick(v), "evo-tick-label", CSS2DObject);
      label.position.copy(positionFn(v));
      group.add(label);
    }
  };
  addTicks(niceTicks(min.x, max.x, tickCount), (v) => new THREE.Vector3(v, min.y, min.z));
  addTicks(niceTicks(min.y, max.y, tickCount), (v) => new THREE.Vector3(min.x, v, min.z));
  addTicks(niceTicks(min.z, max.z, tickCount), (v) => new THREE.Vector3(min.x, min.y, v));

  return group;
}

// --- anywidget entry point --------------------------------------------------------------
export default {
  async render({ model, el }) {
    // VS Code can re-invoke render for the same host element; reset it to avoid stacked viewers.
    el.replaceChildren();

    // Always-visible status banner: proves the frontend module executed and shows what it
    // actually received, independent of the debug overlay or later rendering failures.
    const statusBanner = document.createElement("div");
    statusBanner.className = "evo-viz-status";
    statusBanner.textContent = "Evo Viz: loading dependencies\u2026";
    el.appendChild(statusBanner);

    const setStatus = (text, isError) => {
      statusBanner.textContent = text;
      statusBanner.classList.toggle("evo-viz-status-error", !!isError);
      // Hide the banner entirely when there is nothing to report.
      statusBanner.style.display = text ? "" : "none";
    };

    let deps;
    try {
      deps = dependencies;
    } catch (err) {
      setStatus(`Evo Viz: failed to load JS dependencies \u2014 ${String(err)}`, true);
      return () => {};
    }
    const { THREE, OrbitControls, CSS2DRenderer, CSS2DObject, TilesRenderer } = deps;
    setStatus("Evo Viz: dependencies loaded, waiting for data\u2026");

    const height = model.get("height") || 600;

    // Container with a WebGL canvas + a transparent CSS2D overlay for labels.
    const container = document.createElement("div");
    container.className = "evo-viz-container";
    container.style.height = `${height}px`;
    el.appendChild(container);

    const scene = new THREE.Scene();
    scene.background = new THREE.Color(model.get("background_color") || "#1e1e1e");

    const renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setPixelRatio(window.devicePixelRatio);
    container.appendChild(renderer.domElement);

    const labelRenderer = new CSS2DRenderer();
    labelRenderer.domElement.className = "evo-viz-labels";
    container.appendChild(labelRenderer.domElement);

    const debugPanel = document.createElement("pre");
    debugPanel.className = "evo-viz-debug";
    container.appendChild(debugPanel);

    // --- Attribute-driven colouring controls (hidden until scalars are found) ---------
    const colorPanel = document.createElement("div");
    colorPanel.className = "evo-viz-colors";
    colorPanel.style.display = "none";

    const attrRow = document.createElement("label");
    attrRow.className = "evo-viz-colors-row";
    attrRow.append("Attribute");
    const attrSelect = document.createElement("select");
    attrSelect.className = "evo-viz-select";
    attrRow.appendChild(attrSelect);

    const legend = document.createElement("div");
    legend.className = "evo-viz-legend";
    legend.style.display = "none";
    const legendBar = document.createElement("div");
    legendBar.className = "evo-viz-legend-bar";
    const legendScale = document.createElement("div");
    legendScale.className = "evo-viz-legend-scale";
    const legendMin = document.createElement("span");
    const legendMax = document.createElement("span");
    legendScale.appendChild(legendMin);
    legendScale.appendChild(legendMax);
    const legendCategories = document.createElement("div");
    legendCategories.className = "evo-viz-legend-categories";
    legend.appendChild(legendBar);
    legend.appendChild(legendScale);
    legend.appendChild(legendCategories);

    colorPanel.appendChild(attrRow);
    colorPanel.appendChild(legend);
    container.appendChild(colorPanel);

    const camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, 1e7);
    // Geoscience data uses Z as elevation, so make Z the up axis (three.js defaults to Y-up).
    camera.up.set(0, 0, 1);
    camera.position.set(-1, -1, 0.8);

    // Attach controls to the WebGL canvas, not the label overlay: the overlay has
    // pointer-events:none, so events pass through to the canvas below.
    const controls = new OrbitControls(camera, renderer.domElement);
    // Damping adds inertial ease-in/ease-out; disable it for immediate 1:1 rotation.
    controls.enableDamping = false;

    scene.add(new THREE.AmbientLight(0xffffff, 0.9));
    const dir = new THREE.DirectionalLight(0xffffff, 0.6);
    dir.position.set(1, 2, 3);
    scene.add(dir);

    // `world` is offset by -center so large geoscience coordinates stay within float32 range.
    const world = new THREE.Group();
    scene.add(world);

    const state = {
      tiles: [],
      registeredUrls: [],
      fallbackBoxes: [],
      axesGroup: null,
      framed: false,
      running: true,
      // Attribute-driven colouring.
      loadedNodes: [],
      attributes: new Map(),
      colorAttribute: model.get("color_attribute") || "",
      colormap: model.get("colormap") || "viridis",
      // Tile content diagnostics (deduped across streaming tiles).
      diag: {
        attrs: new Set(),
        featureIds: new Set(),
        metaProps: new Set(),
        extNames: new Set(),
        userDataKeys: new Set(),
        batchKeys: new Set(),
        sceneUserDataKeys: new Set(),
        nodeCount: 0,
      },
      stats: {
        objectCount: 0,
        fileCount: 0,
        gzFileCount: 0,
        normalizedUriCount: 0,
        tilesCount: 0,
        modelLoads: 0,
        pointNodes: 0,
        frameSource: "none",
        urlMapHits: 0,
        urlMapMisses: 0,
        fetchHits: 0,
        fetchMisses: 0,
        lastMissUrl: "",
        lastError: "",
      },
    };

    function setError(msg) {
      state.stats.lastError = String(msg || "");
    }

    function refreshDebugPanel() {
      const enabled = !!model.get("debug");
      debugPanel.style.display = enabled ? "block" : "none";
      if (!enabled) return;

      const lines = [
        "Evo Viz Debug",
        `objects: ${state.stats.objectCount}`,
        `files: ${state.stats.fileCount} (gz: ${state.stats.gzFileCount})`,
        `normalized_uris: ${state.stats.normalizedUriCount}`,
        `tiles: ${state.stats.tilesCount}`,
        `model_loads: ${state.stats.modelLoads}`,
        `point_nodes: ${state.stats.pointNodes}`,
        `framed: ${state.framed} (${state.stats.frameSource})`,
        `url_map_hits: ${state.stats.urlMapHits}`,
        `url_map_misses: ${state.stats.urlMapMisses}`,
        `fetch_hits: ${state.stats.fetchHits}`,
        `fetch_misses: ${state.stats.fetchMisses}`,
      ];
      if (state.stats.lastMissUrl) {
        lines.push(`last_miss: ${state.stats.lastMissUrl}`);
      }
      if (state.stats.lastError) {
        lines.push(`last_error: ${state.stats.lastError}`);
      }

      // Tile content diagnostics: where do the real attribute values live?
      const d = state.diag;
      const fmt = (set) => (set.size ? Array.from(set).sort().join(", ") : "(none)");
      lines.push("-- tile diagnostics --");
      lines.push(`geom_nodes: ${d.nodeCount}`);
      lines.push(`scalar_attrs: ${fmt(d.attrs)}`);
      lines.push(`feature_ids: ${fmt(d.featureIds)}`);
      lines.push(`meta_props: ${fmt(d.metaProps)}`);
      lines.push(`gltf_ext: ${fmt(d.extNames)}`);
      lines.push(`batch_keys: ${fmt(d.batchKeys)}`);
      lines.push(`userData_keys: ${fmt(d.userDataKeys)}`);
      lines.push(`scene_userData: ${fmt(d.sceneUserDataKeys)}`);

      const maxLines = Math.max(4, Number(model.get("debug_max_lines") || 12));
      debugPanel.textContent = lines.slice(0, maxLines).join("\n");
    }

    // --- Attribute-driven colouring ------------------------------------------------

    // Rebuild the attribute dropdown from the scalars discovered so far.
    function rebuildAttributeOptions() {
      const names = Array.from(state.attributes.keys()).sort();
      attrSelect.innerHTML = "";
      const noneOpt = document.createElement("option");
      noneOpt.value = "";
      noneOpt.textContent = "Flat colour";
      attrSelect.appendChild(noneOpt);
      for (const name of names) {
        const opt = document.createElement("option");
        opt.value = name;
        opt.textContent = displayAttrName(name);
        attrSelect.appendChild(opt);
      }
      // Keep the selection if it still exists, else fall back to original colours.
      if (!state.attributes.has(state.colorAttribute)) state.colorAttribute = "";
      attrSelect.value = state.colorAttribute;
      colorPanel.style.display = names.length ? "block" : "none";
    }

    // Snapshot a node's original styling and attach any decoded attribute value arrays.
    // `attrSets` are the per-vertex arrays decoded from this object's glbs; we match a set
    // to this geometry by vertex count (each set is consumed once).
    function registerNode(node, attrSets) {
      const geometry = node.geometry;
      if (!geometry || !geometry.attributes) return;
      const materials = Array.isArray(node.material) ? node.material : [node.material];
      const count = geometry.attributes.position ? geometry.attributes.position.count : 0;

      const attrValues = new Map();
      if (attrSets && attrSets.length) {
        let set = attrSets.find((s) => !s.used && s.pointCount === count);
        if (!set) set = attrSets.find((s) => !s.used);
        if (set) {
          set.used = true;
          for (const at of set.attrs) attrValues.set(at.name, at);
        }
      }

      state.loadedNodes.push({
        node,
        geometry,
        materials,
        attrValues,
        originalColor: geometry.attributes.color ? geometry.attributes.color.clone() : null,
        matState: materials.map((m) => ({
          vertexColors: m ? m.vertexColors : false,
          color: m && m.color ? m.color.clone() : null,
        })),
      });
    }

    // Apply the current attribute + colour map to every loaded node (or restore originals).
    function applyColouring() {
      const name = state.colorAttribute;
      const meta = name ? state.attributes.get(name) : null;
      // No-data points (Evo's inf / 1.8e308 sentinels) blend into the background so they
      // don't show up as stray light-coloured dots at the ends of traces.
      const bg = scene.background && scene.background.isColor ? scene.background : null;
      const NO_DATA = bg ? [bg.r, bg.g, bg.b] : [0.07, 0.07, 0.07];

      // Prefer the attribute's Evo colormap; fall back to a default gradient / palette.
      const cmap = meta && meta.colormap ? meta.colormap : null;

      for (const entry of state.loadedNodes) {
        const geometry = entry.geometry;
        const data = name && entry.attrValues ? entry.attrValues.get(name) : null;
        if (name && meta && data) {
          const count = geometry.attributes.position
            ? geometry.attributes.position.count
            : (data.values ? data.values.length : (data.indices ? data.indices.length : 0));
          const colors = new Float32Array(count * 3);
          if (data.kind === "category") {
            const idx = data.indices;
            const cats = data.categories || [];
            const n = cats.length || 1;
            // Build a label -> colour lookup from the Evo category colormap when present.
            let labelColor = null;
            if (cmap && cmap.kind === "category" && cmap.map && cmap.colors) {
              labelColor = new Map();
              for (let k = 0; k < cmap.map.length; k++) {
                labelColor.set(String(cmap.map[k]), cmap.colors[k] || [1, 1, 1]);
              }
            }
            for (let i = 0; i < count; i++) {
              const c = i < idx.length ? idx[i] : -1;
              let rgb;
              if (c < 0) {
                rgb = NO_DATA;
              } else if (labelColor) {
                rgb = labelColor.get(String(cats[c])) || NO_DATA;
              } else {
                rgb = sampleStops(n > 1 ? c / (n - 1) : 0, DEFAULT_STOPS);
              }
              colors[i * 3] = rgb[0];
              colors[i * 3 + 1] = rgb[1];
              colors[i * 3 + 2] = rgb[2];
            }
          } else {
            const src = data.values;
            // Use the Evo colormap's range/stops when available, else the decoded range.
            const useEvo = cmap && cmap.kind === "continuous" && cmap.stops && cmap.stops.length;
            const stops = useEvo ? cmap.stops : DEFAULT_STOPS;
            const lo = useEvo && isFinite(cmap.min) ? cmap.min : meta.min;
            const hi = useEvo && isFinite(cmap.max) ? cmap.max : meta.max;
            const span = hi - lo || 1;
            for (let i = 0; i < count; i++) {
              const v = i < src.length ? src[i] : NaN;
              const rgb = v !== v ? NO_DATA : sampleStops((v - lo) / span, stops);
              colors[i * 3] = rgb[0];
              colors[i * 3 + 1] = rgb[1];
              colors[i * 3 + 2] = rgb[2];
            }
          }
          geometry.setAttribute("color", new THREE.BufferAttribute(colors, 3));
          geometry.attributes.color.needsUpdate = true;
          for (const m of entry.materials) {
            if (!m) continue;
            m.vertexColors = true;
            if (m.color) m.color.setRGB(1, 1, 1);
            m.needsUpdate = true;
          }
        } else {
          // Restore the node's original styling.
          if (entry.originalColor) {
            geometry.setAttribute("color", entry.originalColor.clone());
            geometry.attributes.color.needsUpdate = true;
          } else if (geometry.attributes.color) {
            geometry.deleteAttribute("color");
          }
          entry.materials.forEach((m, i) => {
            if (!m) return;
            const st = entry.matState[i] || {};
            m.vertexColors = !!st.vertexColors;
            if (m.color && st.color) m.color.copy(st.color);
            m.needsUpdate = true;
          });
        }
      }
      updateLegend();
    }

    function updateLegend() {
      const name = state.colorAttribute;
      const meta = name ? state.attributes.get(name) : null;
      if (!name || !meta) {
        legend.style.display = "none";
        return;
      }
      const cmap = meta.colormap || null;
      if (meta.kind === "category") {
        legend.style.display = "block";
        legendBar.style.display = "none";
        legendScale.style.display = "none";
        legendCategories.style.display = "grid";
        legendCategories.replaceChildren();
        const labelColors = new Map();
        if (cmap && cmap.kind === "category" && cmap.map && cmap.colors) {
          for (let i = 0; i < cmap.map.length; i++) {
            labelColors.set(String(cmap.map[i]), cmap.colors[i] || [1, 1, 1]);
          }
        }
        for (const [index, label] of (meta.categories || []).entries()) {
          const row = document.createElement("div");
          row.className = "evo-viz-legend-category";
          const swatch = document.createElement("span");
          swatch.className = "evo-viz-legend-swatch";
          const color = labelColors.get(String(label)) || sampleStops(
            meta.categories.length > 1 ? index / (meta.categories.length - 1) : 0,
            DEFAULT_STOPS
          );
          swatch.style.background = `rgb(${color.map((value) => Math.round(value * 255)).join(", ")})`;
          const text = document.createElement("span");
          text.textContent = label;
          row.append(swatch, text);
          legendCategories.appendChild(row);
        }
        return;
      }
      legendCategories.style.display = "none";
      legendBar.style.display = "block";
      legendScale.style.display = "flex";
      const useEvo = cmap && cmap.kind === "continuous" && cmap.stops && cmap.stops.length;
      const lo = useEvo && isFinite(cmap.min) ? cmap.min : meta.min;
      const hi = useEvo && isFinite(cmap.max) ? cmap.max : meta.max;
      if (!isFinite(lo) || !isFinite(hi)) {
        legend.style.display = "none";
        return;
      }
      legend.style.display = "block";
      legendBar.style.background = useEvo ? stopsCss(cmap.stops) : stopsCss(DEFAULT_STOPS);
      legendMin.textContent = formatTick(lo);
      legendMax.textContent = formatTick(hi);
    }

    attrSelect.addEventListener("change", () => {
      state.colorAttribute = attrSelect.value;
      if (model.get("color_attribute") !== attrSelect.value) {
        model.set("color_attribute", attrSelect.value);
        if (model.save_changes) model.save_changes();
      }
      applyColouring();
    });

    // Register the bytes for one manifest into the global virtual-file registry.
    function loadData() {
      try {
        loadDataInner();
      } catch (err) {
        const msg = String((err && err.message) || err);
        setError(msg);
        setStatus(`Evo Viz: loadData failed \u2014 ${msg}`, true);
        refreshDebugPanel();
      }
    }

    function loadDataInner() {
      // Clear any previously registered URLs / tilesets.
      for (const url of state.registeredUrls) {
        const blob = g.__evoVizRegistry.get(url);
        if (blob) URL.revokeObjectURL(blob);
        g.__evoVizRegistry.delete(url);
        try {
          const path = new URL(url).pathname;
          if (g.__evoVizPathRegistry.get(path) === blob) {
            g.__evoVizPathRegistry.delete(path);
          }
        } catch {
          // Ignore non-URL keys.
        }
      }
      state.registeredUrls = [];
      state.fallbackBoxes = [];
      for (const t of state.tiles) {
        world.remove(t.group);
        t.dispose();
      }
      state.tiles = [];
      if (state.axesGroup) {
        // CSS2DObject labels keep their own DOM node; removing the group does not
        // remove those <div>s, so strip them explicitly to avoid orphaned/stacked labels.
        state.axesGroup.traverse((o) => {
          if (o.element && o.element.parentNode) {
            o.element.parentNode.removeChild(o.element);
          }
        });
        world.remove(state.axesGroup);
        state.axesGroup = null;
      }
      state.framed = false;
      // Tiles are being reloaded: drop discovered attributes and node snapshots.
      state.loadedNodes = [];
      state.attributes = new Map();
      state.diag = {
        attrs: new Set(),
        featureIds: new Set(),
        metaProps: new Set(),
        extNames: new Set(),
        userDataKeys: new Set(),
        batchKeys: new Set(),
        sceneUserDataKeys: new Set(),
        nodeCount: 0,
      };
      rebuildAttributeOptions();
      world.position.set(0, 0, 0);
      state.stats = {
        objectCount: 0,
        fileCount: 0,
        gzFileCount: 0,
        normalizedUriCount: 0,
        tilesCount: 0,
        modelLoads: 0,
        pointNodes: 0,
        frameSource: "none",
        urlMapHits: 0,
        urlMapMisses: 0,
        fetchHits: 0,
        fetchMisses: 0,
        lastMissUrl: "",
        lastError: "",
      };
      g.__evoVizActiveStats = state.stats;

      const manifestStr = model.get("_manifest") || "{}";
      const manifest = JSON.parse(manifestStr);
      const blob = model.get("_blob"); // DataView
      const blobType = blob === null || blob === undefined ? "none" : (blob.constructor && blob.constructor.name) || typeof blob;
      if (!manifest.objects || !blob) {
        setStatus(
          `Evo Viz: no data yet (manifest_chars=${manifestStr.length}, objects=${(manifest.objects || []).length}, blob=${blobType})`,
          true
        );
        refreshDebugPanel();
        return;
      }
      const bytes = toUint8Array(blob);
      if (!bytes) {
        setError("unsupported _blob payload type");
        setStatus(`Evo Viz: unsupported _blob payload type (${blobType})`, true);
        refreshDebugPanel();
        return;
      }
      state.stats.objectCount = manifest.objects.length;
      setStatus(
        `Evo Viz: hydrated objects=${manifest.objects.length}, blob_bytes=${bytes.length}, blob=${blobType}`
      );

      for (const obj of manifest.objects) {
        // Friendly-name lookup for this object's glb property tables (hash -> {name, kind}).
        const nameByHash = {};
        for (const a of obj.attributes || []) {
          if (a && a.hash)
            nameByHash[a.hash] = {
              name: a.name,
              kind: a.kind || "continuous",
              colormap: a.colormap || null,
            };
        }
        const objGlbBytes = [];

        for (const f of obj.files) {
          state.stats.fileCount += 1;
          const slice = bytes.subarray(f.offset, f.offset + f.length);
          const url = `${manifest.origin}/${f.path}`;
          const aliasPath = f.path.replace(/\.gz$/i, "");
          const aliasUrl = `${manifest.origin}/${aliasPath}`;

          let payload = slice;
          let typePath = f.path;
          if (f.path.toLowerCase().endsWith(".gz")) {
            state.stats.gzFileCount += 1;
            // Payloads are normalized/decompressed in Python when bundle is built.
            // Keep extension semantics aligned for correct loader selection.
            typePath = f.path.replace(/\.gz$/i, "");
          }

          if (/\.glb$/i.test(typePath)) objGlbBytes.push(slice);

          if (f.path.endsWith("/tileset.json")) {
            try {
              const text = new TextDecoder().decode(payload);
              const ts = JSON.parse(text);
              const before = JSON.stringify(ts).match(/\.gz(\?|\"|$)/g)?.length || 0;
              if (ts && ts.root) {
                normalizeTileUris(ts.root);
              }
              const after = JSON.stringify(ts).match(/\.gz(\?|\"|$)/g)?.length || 0;
              state.stats.normalizedUriCount += Math.max(0, before - after);
              const bv = ts && ts.root && ts.root.boundingVolume;
              const b = bv && bv.box ? boxFromBoundingVolumeBox(bv.box, THREE) : null;
              if (b && !b.isEmpty()) state.fallbackBoxes.push(b);
              payload = new TextEncoder().encode(JSON.stringify(ts));
            } catch {
              // Ignore parse failures for fallback framing.
            }
          }

          const blobUrl = URL.createObjectURL(
            new Blob([payload], { type: contentTypeFor(typePath) })
          );
          g.__evoVizRegistry.set(url, blobUrl);
          try {
            g.__evoVizPathRegistry.set(new URL(url).pathname, blobUrl);
          } catch {
            // Ignore non-URL keys.
          }
          state.registeredUrls.push(url);
          if (aliasUrl !== url) {
            g.__evoVizRegistry.set(aliasUrl, blobUrl);
            try {
              g.__evoVizPathRegistry.set(new URL(aliasUrl).pathname, blobUrl);
            } catch {
              // Ignore non-URL keys.
            }
            state.registeredUrls.push(aliasUrl);
          }
        }

        // Decode real attribute values from this object's glbs (three.js drops these).
        // Each set holds per-vertex arrays we later match to a loaded geometry by count.
        const attrSets = [];
        if (Object.keys(nameByHash).length) {
          for (const gbytes of objGlbBytes) {
            let decoded = null;
            try {
              decoded = decodeGlbAttributes(gbytes, nameByHash);
            } catch (err) {
              console.warn("[evo_viz] attribute decode failed", err);
            }
            if (!decoded || !decoded.attrs.length) continue;
            attrSets.push({ pointCount: decoded.pointCount, attrs: decoded.attrs, used: false });
            // Merge into the global attribute metadata used by the dropdown + legend.
            for (const at of decoded.attrs) {
              let meta = state.attributes.get(at.name);
              if (!meta) {
                meta = { kind: at.kind, min: Infinity, max: -Infinity, categories: null, colormap: null };
                state.attributes.set(at.name, meta);
              }
              if (at.colormap && !meta.colormap) meta.colormap = at.colormap;
              if (at.kind === "continuous") {
                if (at.min < meta.min) meta.min = at.min;
                if (at.max > meta.max) meta.max = at.max;
              } else if (at.kind === "category") {
                meta.categories = at.categories;
              }
            }
          }
          rebuildAttributeOptions();
        }

        const tiles = new TilesRenderer(obj.root);
        if (typeof tiles.setRenderer === "function") {
          tiles.setRenderer(renderer);
        }
        state.stats.tilesCount += 1;
        tiles.addEventListener("load-error", (e) => {
          const message = e && (e.message || e.error || e.url || "tile load error");
          setError(message);
          setStatus(`Evo Viz: tile load error \u2014 ${String(message)}`, true);
          refreshDebugPanel();
        });
        tiles.addEventListener("load-model", (e) => {
          state.stats.modelLoads += 1;
          setStatus(
            `Evo Viz: rendering (model_loads=${state.stats.modelLoads}, point_nodes=${state.stats.pointNodes})`
          );
          const modelScene = e && e.scene;
          if (!modelScene || !modelScene.traverse) {
            refreshDebugPanel();
            return;
          }
          modelScene.traverse((node) => {
            if (!node) return;
            // Track any renderable geometry so we can recolour it by attribute.
            if ((node.isPoints || node.isMesh) && node.geometry) {
              registerNode(node, attrSets);
            }
            if (!node.isPoints || !node.material) return;
            state.stats.pointNodes += 1;
            const mats = Array.isArray(node.material) ? node.material : [node.material];
            for (const mat of mats) {
              if (!mat) continue;
              if (typeof mat.size === "number") mat.size = Math.max(mat.size, 3.0);
              if ("sizeAttenuation" in mat) mat.sizeAttenuation = false;
              if ("transparent" in mat) mat.transparent = false;
              if ("opacity" in mat) mat.opacity = 1.0;
              mat.needsUpdate = true;
            }
          });
          // Surface any newly discovered attributes and (re)apply the active colouring.
          rebuildAttributeOptions();
          applyColouring();

          // --- Diagnostics: what do these tiles actually contain? -----------------
          try {
            const report = collectModelDiagnostics(modelScene);
            console.log("[evo_viz] tile model diagnostics", report);
            const d = state.diag;
            for (const skey of report.sceneUserDataKeys) d.sceneUserDataKeys.add(skey);
            for (const n of report.nodes) {
              d.nodeCount += 1;
              for (const akey of Object.keys(n.attributes)) {
                if (isFeatureIdAttr(akey)) d.featureIds.add(akey);
                else d.attrs.add(akey);
              }
              for (const ukey of n.userDataKeys) d.userDataKeys.add(ukey);
              if (n.gltfExtensionNames) {
                for (const ekey of n.gltfExtensionNames) d.extNames.add(ekey);
              }
              if (n.batchTableKeys) {
                for (const bkey of n.batchTableKeys) d.batchKeys.add(bkey);
              }
              if (n.structuralMetadata && n.structuralMetadata.classes) {
                for (const cname of Object.keys(n.structuralMetadata.classes)) {
                  for (const pname of n.structuralMetadata.classes[cname]) {
                    d.metaProps.add(`${cname}.${pname}`);
                  }
                }
              }
            }
          } catch (err) {
            console.warn("[evo_viz] diagnostics failed", err);
          }

          refreshDebugPanel();
        });

        // Route all tileset/content URLs through the virtual registry.
        const urlModifier = (url) => {
          const noQuery = url.split("?")[0];
          let mapped = g.__evoVizRegistry.get(url) || g.__evoVizRegistry.get(noQuery);

          // Some tilesets use absolute hub URLs; remap by pathname into the virtual origin.
          if (!mapped) {
            try {
              const parsed = new URL(noQuery);
              const byPath = `${manifest.origin}${parsed.pathname}`;
              mapped = g.__evoVizRegistry.get(byPath);
            } catch {
              // Keep default behavior for non-URL strings.
            }
          }

          if (mapped) {
            state.stats.urlMapHits += 1;
            return mapped;
          }

          state.stats.urlMapMisses += 1;
          state.stats.lastMissUrl = noQuery;
          return url;
        };
        if (tiles.manager && typeof tiles.manager.setURLModifier === "function") {
          tiles.manager.setURLModifier(urlModifier);
        }
        if (typeof tiles.setCamera === "function") tiles.setCamera(camera);
        if (typeof tiles.setResolutionFromRenderer === "function") {
          tiles.setResolutionFromRenderer(camera, renderer);
        }
        if (tiles.group) world.add(tiles.group);
        state.tiles.push(tiles);
      }

      refreshDebugPanel();
    }

    // Compute the combined bounding box once tiles have loaded, then frame + add axes.
    function tryFrame() {
      if (state.framed || state.tiles.length === 0) return;
      const combined = new THREE.Box3();
      const tmp = new THREE.Box3();
      let any = false;
      for (const t of state.tiles) {
        if (t.getBoundingBox && t.getBoundingBox(tmp)) {
          // getBoundingBox is in the tiles group's local frame.
          tmp.applyMatrix4(t.group.matrixWorld);
          combined.union(tmp);
          any = true;
        }
      }

      if ((!any || combined.isEmpty()) && state.fallbackBoxes.length > 0) {
        for (const b of state.fallbackBoxes) combined.union(b);
        any = !combined.isEmpty();
        if (any) state.stats.frameSource = "fallback-boundingVolume";
      }
      if (!any || combined.isEmpty()) return;

      if (state.stats.frameSource === "none") {
        state.stats.frameSource = "tiles-bounding-box";
      }

      const center = combined.getCenter(new THREE.Vector3());
      const size = combined.getSize(new THREE.Vector3()).length();

      // Recenter the world so coordinates near the model are small.
      world.position.copy(center).multiplyScalar(-1);

      if (model.get("show_axes")) {
        state.axesGroup = buildAxes(
          combined,
          model.get("axis_labels") || ["X", "Y", "Z"],
          model.get("tick_count") || 5,
          THREE,
          CSS2DObject
        );
        world.add(state.axesGroup);
      }

      // Frame the camera on the (recentered) model.
      controls.target.set(0, 0, 0);
      // View from the -X/-Y/+Z octant so Easting (X) reads to the right, Northing (Y)
      // to the left, and Elevation (Z) up — a slightly-elevated 3/4 view.
      const dist = size * 1.6 || 10;
      const viewDir = new THREE.Vector3(-1, -1, 0.8).normalize();
      camera.position.copy(viewDir.multiplyScalar(dist));
      camera.near = Math.max(size / 1000, 0.01);
      camera.far = size * 100 || 1e7;
      // Orthographic frustum height in world units; resize() derives width from aspect.
      state.orthoHalfHeight = size * 0.6 || 1;
      resize();
      controls.update();
      state.framed = true;
      refreshDebugPanel();
    }

    function resize() {
      const w = container.clientWidth || 1;
      const h = container.clientHeight || 1;
      renderer.setSize(w, h);
      labelRenderer.setSize(w, h);
      const halfH = state.orthoHalfHeight || 1;
      const halfW = halfH * (w / h);
      camera.left = -halfW;
      camera.right = halfW;
      camera.top = halfH;
      camera.bottom = -halfH;
      camera.updateProjectionMatrix();
      for (const t of state.tiles) {
        if (typeof t.setResolutionFromRenderer === "function") {
          t.setResolutionFromRenderer(camera, renderer);
        }
      }
    }

    const ro = new ResizeObserver(resize);
    ro.observe(container);

    // Live status line so the tile-loading stage is observable even when no model loads.
    let statusFrame = 0;
    function updateLiveStatus() {
      const s = state.stats;
      if (s.objectCount === 0) return; // pre-hydration message already shown
      const isError = !!s.lastError || (s.urlMapMisses > 0 && s.modelLoads === 0);
      // Only surface the status line when something is wrong; the normal "objs=… tiles=…"
      // running line is suppressed so it no longer clutters the view.
      if (!isError) {
        // Clear any leftover progress message once rendering is healthy.
        if (s.modelLoads > 0) setStatus("");
        return;
      }
      const parts = [
        `Evo Viz: objs=${s.objectCount} tiles=${s.tilesCount} loads=${s.modelLoads} pts=${s.pointNodes}`,
        `url ok/miss=${s.urlMapHits}/${s.urlMapMisses}`,
        `fetch ok/miss=${s.fetchHits}/${s.fetchMisses}`,
        `framed=${state.framed ? s.frameSource : "no"}`,
      ];
      if (s.lastMissUrl) parts.push(`miss=${s.lastMissUrl}`);
      if (s.lastError) parts.push(`err=${s.lastError}`);
      setStatus(parts.join(" | "), isError);
    }

    function animate() {
      if (!state.running) return;
      requestAnimationFrame(animate);
      try {
        controls.update();
        camera.updateMatrixWorld();
        for (const t of state.tiles) {
          if (typeof t.setCamera === "function") t.setCamera(camera);
          if (typeof t.setResolutionFromRenderer === "function") {
            t.setResolutionFromRenderer(camera, renderer);
          }
          if (typeof t.update === "function") t.update();
        }
        tryFrame();
        renderer.render(scene, camera);
        labelRenderer.render(scene, camera);
      } catch (err) {
        setError(String((err && err.message) || err));
      }
      if ((statusFrame++ & 31) === 0) updateLiveStatus();
    }

    // React to updated data / configuration from Python.
    const onData = () => loadData();
    const onBg = () => {
      scene.background = new THREE.Color(model.get("background_color") || "#1e1e1e");
      // No-data points are painted with the background colour; refresh them to match.
      applyColouring();
    };
    const onDebug = () => refreshDebugPanel();
    const onColorAttr = () => {
      const v = model.get("color_attribute") || "";
      state.colorAttribute = v;
      if (state.attributes.has(v) || v === "") attrSelect.value = v;
      applyColouring();
    };
    model.on("change:_blob", onData);
    model.on("change:_manifest", onData);
    model.on("change:background_color", onBg);
    model.on("change:debug", onDebug);
    model.on("change:debug_max_lines", onDebug);
    model.on("change:color_attribute", onColorAttr);

    // Initial render + a delayed retry to avoid missing early model state hydration.
    loadData();
    setTimeout(loadData, 0);
    setTimeout(loadData, 100);
    resize();
    animate();

    // Cleanup when the widget is torn down.
    return () => {
      state.running = false;
      ro.disconnect();
      model.off("change:_blob", onData);
      model.off("change:_manifest", onData);
      model.off("change:background_color", onBg);
      model.off("change:debug", onDebug);
      model.off("change:debug_max_lines", onDebug);
      model.off("change:color_attribute", onColorAttr);
      if (g.__evoVizActiveStats === state.stats) {
        g.__evoVizActiveStats = null;
      }
      for (const url of state.registeredUrls) {
        const blob = g.__evoVizRegistry.get(url);
        if (blob) URL.revokeObjectURL(blob);
        g.__evoVizRegistry.delete(url);
      }
      for (const t of state.tiles) t.dispose();
      renderer.dispose();
    };
  },
};
