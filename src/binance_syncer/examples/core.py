import os
import glob
import datetime as dt
from pathlib import Path
from typing import List, Optional, Sequence, Union, Tuple, Dict

import duckdb
import boto3

from utilities import Config

from ..constant import (
    MarketType,
    DataType,
    KlineInterval,
    TIME_COLUMNS,
    TIMESTAMP_COLUMNS,
    DATE_COLUMNS,
)
from ..utils import BinancePathBuilder


TimeLike = Union[str, dt.datetime]

LOCAL_PREFIX = Config.BINANCE_SYNCER.LOCAL.PATH

class Loader:
    """
    DuckDB Parquet loader (optimized).

    Key optimizations vs the naive version:
    - Cache schema + time column detection (per Loader instance).
    - Detect underlying *DuckDB type* of the time column and build WHERE predicates that
      preserve Parquet predicate pushdown when possible.

    Notes about your pipeline:
    - If you truly write all time columns as proper Parquet TIMESTAMP (via pandas -> parquet),
      then time_unit (ms/s) is irrelevant and numeric epochs shouldn't happen.
    - Still, we keep a robust fallback for numeric time columns just in case some datasets
      were written differently (or come from external sources).
    """

    LOCAL_PREFIX = LOCAL_PREFIX
    S3_PREFIX = Config.BINANCE_SYNCER.S3.PREFIX
    S3_BUCKET = Config.BINANCE_SYNCER.S3.BUCKET

    _TIME_CANDIDATES = TIME_COLUMNS + TIMESTAMP_COLUMNS + DATE_COLUMNS

    # DuckDB type "hints" used to classify the time column
    _NUMERIC_HINTS = (
        "BIGINT",
        "HUGEINT",
        "INTEGER",
        "UBIGINT",
        "UINTEGER",
        "SMALLINT",
        "TINYINT",
        "DECIMAL",
        "DOUBLE",
        "REAL",
        "FLOAT",
    )

    def __init__(
        self,
        market_type: MarketType,
        data_type: DataType,
        interval: Optional[KlineInterval] = None,
        *,
        base_path: Union[str, Path] = LOCAL_PREFIX,
        db: str = ":memory:",
        threads: Optional[int] = os.cpu_count(),
        memory_limit: Optional[str] = None,
        union_by_name: bool = True,
        # kept for robustness; unused if time col is TIMESTAMP
        default_time_unit: str = "ms",  # "ms" | "s"
        enable_cache: bool = True,
        s3: bool = False,  # if True, use s3fs for listing and reading (requires s3fs installed and configured)
    ):
        self.market_type = market_type
        self.data_type = data_type
        self.interval = interval

        self.path_builder = BinancePathBuilder(market_type, data_type, interval)

        self.base_path = Path(base_path)
        self.union_by_name = union_by_name
        self.default_time_unit = default_time_unit
        self.enable_cache = enable_cache
        self.s3 = s3
        self.con = duckdb.connect(database=db)
        if threads is not None:
            self.con.execute(f"PRAGMA threads={int(threads)};")
        if memory_limit:
            self.con.execute(f"SET memory_limit='{memory_limit}';")

        # Configure S3 support if needed
        if s3:
            self.s3_client = boto3.client('s3')
            # Install and load httpfs extension for S3 support in DuckDB
            try:
                self.con.execute("INSTALL httpfs;")
                self.con.execute("LOAD httpfs;")
            except Exception:
                pass  # Extension might already be installed
            
            # Configure AWS credentials for DuckDB
            # Get credentials from boto3 session
            session = boto3.Session()
            credentials = session.get_credentials()
            if credentials:
                # Frozen credentials to avoid issues with temporary credentials
                creds = credentials.get_frozen_credentials()
                self.con.execute(f"SET s3_access_key_id='{creds.access_key}';")
                self.con.execute(f"SET s3_secret_access_key='{creds.secret_key}';")
                if creds.token:
                    self.con.execute(f"SET s3_session_token='{creds.token}';")
                
                # Set the region if available
                region = session.region_name
                if region:
                    self.con.execute(f"SET s3_region='{region}';")
        else:
            self.s3_client = None

        # per-instance caches
        self._schema_cache: Dict[Tuple[str, str, Optional[str]], Tuple[List[str], bool]] = {}
        self._time_cache: Dict[Tuple[str, str, Optional[str]], Tuple[Optional[str], Optional[str]]] = {}
        # time_cache: (detected_time_col, detected_time_kind)

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------

    def load(
        self,
        symbols: Optional[Sequence[str]] = None,
        start: Optional[TimeLike] = None,
        end: Optional[TimeLike] = None,
        time_col: Optional[str] = None,
        select_cols: Optional[Sequence[str]] = None,
        # only relevant if time col is numeric (epoch); should be unused in your setup
        time_unit: Optional[str] = None,  # "ms" | "s"
    ):
        if symbols is None:
            symbols = self.available_symbols()
        if isinstance(symbols, str):
            symbols = [symbols]

        globs_ = self._build_globs(symbols)
        if not globs_:
            raise FileNotFoundError(
                f"No parquet found for market={self.market_type.value}, data={self.data_type.value}, "
                f"interval={(self.interval.value if self.interval else None)}, symbols={list(symbols)}"
            )

        # schema (cached)
        columns, has_symbol = self._get_schema_and_symbol(globs_)

        # time column (cached unless user overrides)
        if time_col is None:
            time_col, cached_kind = self._get_cached_time_detection()
            if time_col is None:
                time_col = self._detect_time_column(columns)
                cached_kind = None
            # cache detected time_col (kind computed lazily below)
            if self.enable_cache:
                self._set_cached_time_detection(time_col, cached_kind)

        if (start is not None or end is not None) and not time_col:
            raise ValueError(
                f"Time column not found. Candidates={self._TIME_CANDIDATES}. "
                "Provide `time_col=` explicitly if needed."
            )

        # time kind only needed if we filter on time
        time_kind = None
        if time_col and (start is not None or end is not None):
            # use cached kind if present, else detect and cache
            _, cached_kind = self._get_cached_time_detection()
            time_kind = cached_kind
            if time_kind is None:
                time_kind = self._detect_time_kind(globs_, time_col)
                if self.enable_cache:
                    self._set_cached_time_detection(time_col, time_kind)

        sql = self._build_query(
            globs_=globs_,
            columns=columns,
            has_symbol=has_symbol,
            time_col=time_col,
            time_kind=time_kind,
            time_unit=(time_unit or self.default_time_unit),
            start=start,
            end=end,
            select_cols=select_cols,
        )

        out = self.con.execute(sql).fetchdf()
        if "filename" in out.columns:
            del out["filename"]
        return out

    def available_symbols(self) -> List[str]:
        if self.s3:
            # List symbols from S3
            prefix = self.path_builder.build_save_path(self.S3_PREFIX, "").rstrip("/") + "/"
            paginator = self.s3_client.get_paginator("list_objects_v2")
            syms = set()
            for page in paginator.paginate(Bucket=self.S3_BUCKET, Prefix=prefix, Delimiter="/"):
                for common_prefix in page.get("CommonPrefixes", []):
                    # Extract symbol from prefix like: prefix/BTCUSDT/
                    symbol = common_prefix["Prefix"].rstrip("/").split("/")[-1]
                    if symbol:
                        syms.add(symbol)
            return sorted(list(syms))
        else:
            # List symbols from local filesystem
            root = self.base_path / "data" / self.market_type.value / self.data_type.value
            if not root.is_dir():
                return []

            syms: List[str] = []
            for sym in os.listdir(root):
                p = root / sym / (self.interval.value if self.interval else "")
                if p.is_dir() and glob.glob(str(p / "*.parquet")):
                    syms.append(sym)
            return sorted(syms)

    # ---------------------------------------------------------------------
    # Internals
    # ---------------------------------------------------------------------

    def _cache_key(self) -> Tuple[str, str, Optional[str]]:
        return (
            str(self.market_type.value),
            str(self.data_type.value),
            str(self.interval.value) if self.interval else None,
        )

    def _get_schema_and_symbol(self, globs_: Sequence[str]) -> Tuple[List[str], bool]:
        key = self._cache_key()
        if self.enable_cache and key in self._schema_cache:
            return self._schema_cache[key]

        columns = self._describe_columns(globs_)
        has_symbol = any(c.lower() == "symbol" for c in columns)

        if self.enable_cache:
            self._schema_cache[key] = (columns, has_symbol)
        return columns, has_symbol

    def _get_cached_time_detection(self) -> Tuple[Optional[str], Optional[str]]:
        key = self._cache_key()
        if self.enable_cache and key in self._time_cache:
            return self._time_cache[key]
        return None, None

    def _set_cached_time_detection(self, time_col: Optional[str], time_kind: Optional[str]) -> None:
        key = self._cache_key()
        self._time_cache[key] = (time_col, time_kind)

    def _build_globs(self, symbols: Sequence[str]) -> List[str]:
        out: List[str] = []
        if self.s3:
            # Build S3 URLs for DuckDB
            for s in symbols:
                prefix = self.path_builder.build_save_path(self.S3_PREFIX, s, "")
                # List files on S3 for this symbol
                paginator = self.s3_client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.S3_BUCKET, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        if key.endswith(".parquet"):
                            # DuckDB can read from S3 with s3:// URLs
                            s3_url = f"s3://{self.S3_BUCKET}/{key}"
                            out.append(s3_url)
            return out
        else:
            # Build local file globs
            for s in symbols:
                p = self.path_builder.build_save_path(str(self.base_path), s, "*.parquet")
                out.append(str(p))
            return out if any(glob.glob(g) for g in out) else []

    def _describe_columns(self, globs_: Sequence[str]) -> List[str]:
        globs_sql = self._list_literal_sql(globs_)
        df = self.con.execute(
            f"""
            DESCRIBE
            SELECT *
            FROM read_parquet({globs_sql}, union_by_name={str(self.union_by_name).lower()})
            LIMIT 0
            """
        ).fetchdf()
        return df["column_name"].tolist()

    def _detect_time_column(self, columns: Sequence[str]) -> Optional[str]:
        low = [c.lower() for c in columns]
        for cand in self._TIME_CANDIDATES:
            cl = cand.lower()
            if cl in low:
                return columns[low.index(cl)]
        return None

    def _detect_time_kind(self, globs_: Sequence[str], time_col: str) -> str:
        """
        Return kind ∈ {"timestamp","date","numeric","string"} based on DuckDB's inferred type.
        In your pipeline this should almost always be "timestamp".
        """
        globs_sql = self._list_literal_sql(globs_)
        desc = self.con.execute(
            f"""
            DESCRIBE
            SELECT {self._quote_ident(time_col)}
            FROM read_parquet({globs_sql}, union_by_name={str(self.union_by_name).lower()})
            LIMIT 0
            """
        ).fetchdf()

        tname = str(desc.loc[0, "column_type"]).upper()

        # DuckDB uses e.g. TIMESTAMP, TIMESTAMP_NS, TIMESTAMP_MS, TIMESTAMP_TZ
        if "TIMESTAMP" in tname:
            return "timestamp"
        if "DATE" == tname or tname.endswith("DATE"):
            return "date"
        if any(h in tname for h in self._NUMERIC_HINTS):
            return "numeric"
        return "string"

    def _build_query(
        self,
        globs_: Sequence[str],
        columns: Sequence[str],
        has_symbol: bool,
        time_col: Optional[str],
        time_kind: Optional[str],
        time_unit: str,
        start: Optional[TimeLike],
        end: Optional[TimeLike],
        select_cols: Optional[Sequence[str]],
    ) -> str:
        globs_sql = self._list_literal_sql(globs_)

        # Normalize path separators for regex (Windows-safe)
        fname = "replace(filename, '\\\\', '/')"

        m = self._re_escape(self.market_type.value)
        d = self._re_escape(self.data_type.value)
        symbol_from_path = f"regexp_extract({fname}, '/{m}/{d}/([^/]+)/', 1)"

        # --- SELECT list ---
        if select_cols is None:
            select_list = "t.*" if has_symbol else f"{symbol_from_path} AS symbol, t.*"
        else:
            wanted = list(select_cols)

            if "symbol" not in {c.lower() for c in wanted}:
                if not has_symbol:
                    wanted = ["symbol"] + wanted

            projected: List[str] = []
            for c in wanted:
                if c.lower() == "symbol":
                    projected.append(self._quote_ident("symbol") if has_symbol else f"{symbol_from_path} AS symbol")
                else:
                    projected.append(self._quote_ident(c))
            select_list = ", ".join(projected)

        # --- WHERE clause on time column (pushdown-friendly when possible) ---
        where_parts: List[str] = []
        if time_col and (start is not None or end is not None):
            tc = self._quote_ident(time_col)
            kind = time_kind or "timestamp"  # if unknown, assume timestamp (your pipeline)

            if kind == "timestamp":
                # Best case: WHERE directly on timestamp column -> pushdown
                if start is not None:
                    where_parts.append(f"{tc} >= {self._to_timestamp_literal(start)}")
                if end is not None:
                    where_parts.append(f"{tc} <= {self._to_timestamp_literal(end)}")

            elif kind == "date":
                # If stored as DATE, compare to DATE literals (also pushdown-ish)
                if start is not None:
                    where_parts.append(f"{tc} >= {self._to_date_literal(start)}")
                if end is not None:
                    where_parts.append(f"{tc} <= {self._to_date_literal(end)}")

            elif kind == "numeric":
                # Epoch numeric fallback (shouldn't happen if you always wrote timestamp)
                if time_unit not in ("ms", "s"):
                    raise ValueError("time_unit must be 'ms' or 's' for numeric time columns.")
                if start is not None:
                    where_parts.append(f"{tc} >= {self._to_epoch_literal(start, time_unit)}")
                if end is not None:
                    where_parts.append(f"{tc} <= {self._to_epoch_literal(end, time_unit)}")

            else:
                # String fallback: cast in predicate (less pushdown)
                if start is not None:
                    where_parts.append(f"CAST({tc} AS TIMESTAMP) >= {self._to_timestamp_literal(start)}")
                if end is not None:
                    where_parts.append(f"CAST({tc} AS TIMESTAMP) <= {self._to_timestamp_literal(end)}")

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        sql = f"""
        SELECT {select_list}
        FROM read_parquet(
            {globs_sql},
            union_by_name={str(self.union_by_name).lower()},
            filename=true
        ) AS t
        {where_sql}
        ;
        """
        return sql

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------

    @staticmethod
    def _list_literal_sql(paths: Sequence[str]) -> str:
        esc = lambda p: p.replace("'", "''")
        return "[" + ", ".join(f"'{esc(p)}'" for p in paths) + "]"

    @staticmethod
    def _quote_ident(name: str) -> str:
        safe = name.replace('"', '""')
        return f'"{safe}"'

    @staticmethod
    def _to_timestamp_literal(x: TimeLike) -> str:
        """
        DuckDB TIMESTAMP literal (no unit conversion here).
        """
        if isinstance(x, dt.datetime):
            if x.tzinfo is not None:
                x = x.astimezone(dt.timezone.utc).replace(tzinfo=None)
            s = x.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
            return f"TIMESTAMP '{s}'"
        s = str(x).replace("'", "''")
        return f"TIMESTAMP '{s}'"

    @staticmethod
    def _to_date_literal(x: TimeLike) -> str:
        """
        DuckDB DATE literal. If datetime provided, we take the date part (UTC-normalized).
        """
        if isinstance(x, dt.datetime):
            if x.tzinfo is not None:
                x = x.astimezone(dt.timezone.utc).date()
            else:
                x = x.date()
            s = x.isoformat()
            return f"DATE '{s}'"
        # string: take first 10 chars if ISO, otherwise let DuckDB parse
        s = str(x).replace("'", "''")
        # If user passes a full timestamp string, DATE '2025-01-01 00:00:00' is not valid,
        # so try to strip if it looks ISO-ish.
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            s = s[:10]
        return f"DATE '{s}'"

    @staticmethod
    def _to_epoch_literal(x: TimeLike, unit: str) -> str:
        """
        Convert datetime/iso-string to numeric epoch in seconds or milliseconds.
        Used only if the parquet time column is numeric (fallback).
        """
        if isinstance(x, str):
            # interpret as ISO, naive assumed UTC
            dtobj = dt.datetime.fromisoformat(x.replace("Z", ""))
            if dtobj.tzinfo is None:
                dtobj = dtobj.replace(tzinfo=dt.timezone.utc)
            else:
                dtobj = dtobj.astimezone(dt.timezone.utc)
        else:
            dtobj = x
            if dtobj.tzinfo is None:
                dtobj = dtobj.replace(tzinfo=dt.timezone.utc)
            else:
                dtobj = dtobj.astimezone(dt.timezone.utc)

        sec = dtobj.timestamp()
        return str(int(sec * 1000) if unit == "ms" else int(sec))

    @staticmethod
    def _re_escape(x: str) -> str:
        return x.replace(".", "\\.").replace("-", "\\-")