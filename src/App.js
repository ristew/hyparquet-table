import React, { useState, useEffect, useRef, forwardRef } from 'react';
import {
  asyncBufferFromUrl,
  parquetReadObjects,
  parquetMetadataAsync,
  parquetSchema,
  toJson,
} from 'hyparquet';
import './App.css';

const ROW_HEIGHT = 35;  // px
const PRELOAD    = 10;  // rows to over-fetch

async function loadParquetFile() {
  const url = 'https://s3.hyperparam.app/wiki_en.parquet';
  const file = await asyncBufferFromUrl({ url });
  const metadata = await parquetMetadataAsync(file);
  const schema = parquetSchema(metadata);
  return {
    file,
    numRows: Number(metadata.num_rows),
    columnNames: schema.children.map(e => e.element.name),
  };
}

async function readParquetRange({ file, columnNames }, start, end) {
  console.log('loading rows', start, '–', end);
  const objs = await parquetReadObjects({
    file,
    columns: columnNames,
    rowStart: start,
    rowEnd: end,
  });
  return toJson(objs);
}

function ParquetTable(
  { rows, columnNames, onScroll, containerHeight, totalRows, startIndex },
  ref
) {
  return (
    <div
      ref={ref}
      className="parquet-body"
      onScroll={onScroll}
      style={{
        height: containerHeight,
        overflowY: 'auto',
        position: 'relative',  // for absolute children
      }}
    >
      {/* spacer to give full scroll height */}
      <div style={{ height: totalRows * ROW_HEIGHT }} />

      {/* absolutely-positioned window of real rows */}
      <div
        style={{
          position: 'absolute',
          top: startIndex * ROW_HEIGHT,
          left: 0,
          right: 0,
        }}
      >
        {rows.map((row, i) => (
          <div
            key={startIndex + i}
            className="parquet-row"
            style={{
              display: 'flex',
              borderBottom: '1px solid #eee',
              height: `${ROW_HEIGHT}px`,
              lineHeight: `${ROW_HEIGHT}px`,
            }}
          >
            {columnNames.map(col => (
              <div
                key={col}
                className="parquet-cell"
                style={{
                  flex: 1,
                  padding: '0 8px',
                  whiteSpace: 'nowrap',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                }}
              >
                {startIndex + i}: {String(row[col]).slice(0, 30)}
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}

function App() {
  const [data, setData] = useState(null);
  const [error, setError] = useState();
  const [start, setStart] = useState(0);
  const windowSize = Math.ceil(window.innerHeight / ROW_HEIGHT);
  const end = start + windowSize;
  const [bufferedRows, setBufferedRows] = useState([]);
  const [lastStart,     setLastStart]     = useState(-999);
  const scrollRef = useRef();

  // 1) load file + schema
  useEffect(() => {
    loadParquetFile()
      .then(setData)
      .catch(setError);
  }, []);

  // 2) fetch any time start/end moves
  useEffect(() => {
    if (!data) return;
    const bufStart = Math.max(0, start - PRELOAD);
    const bufEnd = Math.min(data.numRows, end + PRELOAD);
    if (Math.abs(start - lastStart) > windowSize / 2) {
      readParquetRange(data, bufStart, bufEnd).then(rows => {
        console.log('rows loaded?', bufStart, start);
        if (bufStart === Math.max(0, start - PRELOAD)) {
          setBufferedRows(rows);
          setLastStart(start);
        }
      });
    }
  }, [data, start, end]);

  if (error) return <div>Error: {error.message||error}</div>;
  if (!data)  return <div>Loading…</div>;

  // 3) onScroll: compute new start
  const handleScroll = e => {
    const scrollTop = e.target.scrollTop;
    const targetStart = Math.floor(scrollTop / ROW_HEIGHT);
    if (targetStart !== start) {
      // clamp in [0, numRows-windowSize]
      const clamped = Math.min(
        Math.max(0, targetStart),
        data.numRows - windowSize
      );
      setStart(clamped);
    }
  };

  return (
    <div className="App">
      <h1>
        rows {start}–{end} / {data.numRows}
      </h1>
      <ParquetTable
        ref={scrollRef}
        rows={bufferedRows}
        columnNames={data.columnNames}
        totalRows={data.numRows}
        startIndex={Math.max(0, start - PRELOAD)}
        containerHeight={windowSize * ROW_HEIGHT}
        onScroll={handleScroll}
      />
    </div>
  );
}

export default App;
