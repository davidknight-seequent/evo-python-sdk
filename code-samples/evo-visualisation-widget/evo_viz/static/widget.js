// evo_viz front-end module (anywidget ESM).
//
// Renders Evo geoscience objects delivered as OGC 3D Tiles, plus a 3D axis frame with
// numbered tick labels. Bytes for every tile come from Python via a single binary blob, so
// the browser never needs an Evo token and there are no CORS issues.
//
// Rendering stack (loaded from a CDN so the widget stays dependency-free on the Python side):
//   * three           — scene graph + WebGL renderer
//   * OrbitControls    — mouse/touch camera navigation
//   * CSS2DRenderer    — DOM-based axis + tick labels that always face the camera
//   * 3d-tiles-renderer — streams the tileset
//
// NOTE ON VERSIONS: the exact hook used to feed in-memory bytes to 3d-tiles-renderer depends
// on the library version. This module uses `tiles.manager.setURLModifier(...)` plus a scoped
// `fetch` patch, which covers the 0.3.x line. If you upgrade, check the README for the
// equivalent option.

let __depsPromise = null;

async function importFirst(urls) {
  let lastError = null;
  for (const url of urls) {
    try {
      return await import(url);
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError || new Error("module import failed");
}

function loadDependencies() {
  if (__depsPromise) return __depsPromise;
  __depsPromise = (async () => {
    const three = await importFirst([
      "https://cdn.jsdelivr.net/npm/three@0.161.0/+esm",
      "https://esm.sh/three@0.161.0",
    ]);
    const controls = await importFirst([
      "https://cdn.jsdelivr.net/npm/three@0.161.0/examples/jsm/controls/OrbitControls.js/+esm",
      "https://esm.sh/three@0.161.0/examples/jsm/controls/OrbitControls.js",
    ]);
    const labels = await importFirst([
      "https://cdn.jsdelivr.net/npm/three@0.161.0/examples/jsm/renderers/CSS2DRenderer.js/+esm",
      "https://esm.sh/three@0.161.0/examples/jsm/renderers/CSS2DRenderer.js",
    ]);
    const tilesMod = await importFirst([
      "https://cdn.jsdelivr.net/npm/3d-tiles-renderer@0.3.36/+esm",
      "https://esm.sh/3d-tiles-renderer@0.3.36",
    ]);

    const TilesRenderer =
      tilesMod.TilesRenderer ||
      (tilesMod.default && tilesMod.default.TilesRenderer) ||
      tilesMod.default;
    if (!TilesRenderer) {
      throw new Error("TilesRenderer export not found");
    }

    return {
      THREE: three,
      OrbitControls: controls.OrbitControls,
      CSS2DRenderer: labels.CSS2DRenderer,
      CSS2DObject: labels.CSS2DObject,
      TilesRenderer,
    };
  })();
  return __depsPromise;
}

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
    };

    let deps;
    try {
      deps = await loadDependencies();
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

      const maxLines = Math.max(4, Number(model.get("debug_max_lines") || 12));
      debugPanel.textContent = lines.slice(0, maxLines).join("\n");
    }

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
            if (!node || !node.isPoints || !node.material) return;
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
      const parts = [
        `Evo Viz: objs=${s.objectCount} tiles=${s.tilesCount} loads=${s.modelLoads} pts=${s.pointNodes}`,
        `url ok/miss=${s.urlMapHits}/${s.urlMapMisses}`,
        `fetch ok/miss=${s.fetchHits}/${s.fetchMisses}`,
        `framed=${state.framed ? s.frameSource : "no"}`,
      ];
      const isError = !!s.lastError || (s.urlMapMisses > 0 && s.modelLoads === 0);
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
    };
    const onDebug = () => refreshDebugPanel();
    model.on("change:_blob", onData);
    model.on("change:_manifest", onData);
    model.on("change:background_color", onBg);
    model.on("change:debug", onDebug);
    model.on("change:debug_max_lines", onDebug);

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
