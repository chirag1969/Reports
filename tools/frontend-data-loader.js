/*
 * Front-end data loading helper for the static dashboard.
 *
 * The generated ``data/index.json`` file contains:
 *   - a ``filters`` object with all possible values for each filter dropdown.
 *   - a ``partitions`` array where every entry describes a JSON (or csv.gz)
 *     slice that only contains the records matching those filter values.
 *
 * The code below loads the metadata, downloads the minimum number of
 * partitions required for the current filter selection, and exposes helper
 * methods to (1) query the combined dataset and (2) recompute pivots without
 * touching the DOM until the new values are ready.
 */

const DATA_ROOT = './data';

/**
 * A very small event emitter so that pivot widgets can subscribe to filter
 * changes.  We keep it inside this file so there is no dependency on external
 * libraries.
 */
function createEmitter() {
  const listeners = new Set();
  return {
    subscribe(callback) {
      listeners.add(callback);
      return () => listeners.delete(callback);
    },
    emit(payload) {
      for (const callback of listeners) callback(payload);
    }
  };
}

/**
 * Load ``index.json`` and build a constant-time lookup for partitions.
 */
async function fetchMetadata() {
  const response = await fetch(`${DATA_ROOT}/index.json`, { cache: 'no-store' });
  if (!response.ok) throw new Error('Unable to load dataset metadata');
  const metadata = await response.json();
  const partitionsByFilter = new Map();
  for (const part of metadata.partitions) {
    const filterKey = JSON.stringify(part.filters);
    partitionsByFilter.set(filterKey, part);
  }
  return { metadata, partitionsByFilter };
}

/**
 * Fetch and parse a partition.  JSON is decoded via ``response.json()``,
 * while compressed CSV is parsed client-side with ``Papaparse`` or a
 * lightweight streaming CSV reader (plug the parser you already use).
 */
async function loadPartitionSlice(partition, format) {
  const url = `${DATA_ROOT}/${partition.path}`;
  if (format === 'json') {
    const response = await fetch(url, { cache: 'force-cache' });
    if (!response.ok) throw new Error(`Failed to download ${url}`);
    const { columns, data } = await response.json();
    return { columns, data };
  }
  // Example CSV loader – replace with the parser you prefer.
  const response = await fetch(url, { cache: 'force-cache' });
  if (!response.ok) throw new Error(`Failed to download ${url}`);
  const text = await response.text();
  const [header, ...rows] = text.trim().split('\n');
  const columns = header.split(',');
  const data = rows.map((line) => line.split(','));
  return { columns, data };
}

/**
 * Convert a row-oriented array into a column index map for cheap filtering.
 */
function createColumnIndex(columns) {
  const index = new Map();
  columns.forEach((name, idx) => index.set(name, idx));
  return index;
}

/**
 * Filter the combined dataset by the active filter state without mutating the
 * original data arrays.
 */
function filterRows(columnIndex, rows, filters) {
  const entries = Object.entries(filters).filter(([, value]) => value != null && value !== '');
  if (!entries.length) return rows;
  return rows.filter((row) =>
    entries.every(([column, value]) => {
      const idx = columnIndex.get(column);
      if (idx == null) return true;
      const cell = row[idx];
      if (Array.isArray(value)) {
        return value.length === 0 || value.includes(cell);
      }
      return value === '' || cell === value;
    })
  );
}

/**
 * Group rows by one or more dimensions, aggregating metrics for the pivots.
 */
function buildPivot(rows, columnIndex, groupBy, metrics) {
  const table = new Map();
  for (const row of rows) {
    const groupKey = groupBy.map((column) => row[columnIndex.get(column)] ?? '∅').join('¦');
    let bucket = table.get(groupKey);
    if (!bucket) {
      bucket = { key: groupKey, values: {}, rows: [] };
      for (const metric of metrics) bucket.values[metric.name] = 0;
      table.set(groupKey, bucket);
    }
    bucket.rows.push(row);
    for (const metric of metrics) {
      const value = Number(row[columnIndex.get(metric.source)] || 0);
      bucket.values[metric.name] += metric.reducer === 'avg'
        ? value / rows.length
        : value;
    }
  }
  return Array.from(table.values());
}

/**
 * The central store keeps metadata, caches partition downloads, exposes a
 * filter API, and broadcasts changes so the pivot widgets stay in sync.
 */
export async function createDataStore(initialFilters = {}) {
  const emitter = createEmitter();
  const { metadata, partitionsByFilter } = await fetchMetadata();
  const cache = new Map();
  let activeFilters = { ...initialFilters };

  async function ensurePartitions(filters) {
    const key = JSON.stringify(filters);
    if (!cache.has(key)) {
      const partition = partitionsByFilter.get(key);
      if (!partition) {
        // Fallback: load every partition and merge client-side.
        const downloads = await Promise.all(
          metadata.partitions.map((p) => loadPartitionSlice(p, metadata.format))
        );
        const combined = mergeSlices(downloads);
        cache.set(key, combined);
      } else {
        const slice = await loadPartitionSlice(partition, metadata.format);
        cache.set(key, slice);
      }
    }
    return cache.get(key);
  }

  function mergeSlices(slices) {
    if (!slices.length) return { columns: [], data: [] };
    const columns = slices[0].columns;
    const data = slices.flatMap((slice) => slice.data);
    return { columns, data };
  }

  async function getRows(filters = activeFilters) {
    const slice = await ensurePartitions(filters);
    const columnIndex = createColumnIndex(slice.columns);
    return { columnIndex, rows: filterRows(columnIndex, slice.data, filters) };
  }

  async function updateFilters(patch) {
    activeFilters = { ...activeFilters, ...patch };
    emitter.emit({ filters: activeFilters });
    return getRows(activeFilters);
  }

  return {
    metadata,
    get filters() {
      return activeFilters;
    },
    async bootstrap() {
      return getRows(activeFilters);
    },
    async query(groupBy, metrics) {
      const { columnIndex, rows } = await getRows(activeFilters);
      return buildPivot(rows, columnIndex, groupBy, metrics);
    },
    async setFilters(nextFilters) {
      return updateFilters(nextFilters);
    },
    onFiltersChanged: emitter.subscribe,
  };
}

// Example usage -------------------------------------------------------------

// const dataStore = await createDataStore({ date: '2024-01-01' });
// const { rows, columnIndex } = await dataStore.bootstrap();
// const overviewPivot = await dataStore.query(
//   ['store', 'targetingType'],
//   [
//     { name: 'Spend', source: 'spend', reducer: 'sum' },
//     { name: 'Revenue', source: 'revenue', reducer: 'sum' },
//   ],
// );
// renderPivotTable('storePivot', overviewPivot);
//
// dataStore.onFiltersChanged(async ({ filters }) => {
//   const pivot = await dataStore.query(['campaign'], [
//     { name: 'Clicks', source: 'clicks', reducer: 'sum' },
//   ]);
//   renderPivotTable('campaignPivot', pivot);
// });
