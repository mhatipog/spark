"""Engine adapters: register a file set as view `t` under a mode, run SQL, return rows or an error.

Every engine has an "off" mode with all of its skipping and pushdown switched off; the other
modes switch mechanisms on. A mode that changes the bag relative to "off" is a skipping or
pushdown finding; "off" against the generator's Python answer is a semantics finding.
"""
import os

from shapes import parse_type, spark_ddl, sql_type, to_arrow


class Engine:
    name = "?"
    dialect = "?"
    formats = ("parquet",)
    modes = {}
    cores = 1

    def run(self, mode, fileset, shape, sqls):
        raise NotImplementedError


def read_cols(shape):
    return shape["read_as"] or shape["columns"]


def partition_cols(shape):
    """(name, type) of the hive partition columns, in directory order (pair four)."""
    types = dict(shape["columns"])
    return [(n, types[n]) for n in shape.get("partition", [])]


# ------------------------------------------------------------------ Spark
SPARK_OFF = {"spark.sql.parquet.filterPushdown": "false", "spark.sql.orc.filterPushdown": "false",
             "spark.sql.parquet.aggregatePushdown": "false", "spark.sql.orc.aggregatePushdown": "false"}
V2 = {"spark.sql.sources.useV1SourceList": ""}


class Spark(Engine):
    name, dialect, formats, cores = "spark", "spark", ("parquet", "orc"), 2
    modes = {
        "off": SPARK_OFF,
        "v1": {},
        "v2": V2,
        "v1_no_vectorized": {"spark.sql.parquet.enableVectorizedReader": "false",
                             "spark.sql.orc.enableVectorizedReader": "false"},
        "v1_record_filter": {"spark.sql.parquet.enableVectorizedReader": "false",
                             "spark.sql.parquet.recordLevelFilter.enabled": "true"},
        "v1_batch2": {"spark.sql.parquet.columnarReaderBatchSize": "2", "spark.sql.orc.columnarReaderBatchSize": "2"},
        "v2_agg": {**V2, "spark.sql.parquet.aggregatePushdown": "true", "spark.sql.orc.aggregatePushdown": "true"},
        "v1_no_nested_pushdown": {"spark.sql.optimizer.nestedPredicatePushdownSourceList": ""},
    }

    def __init__(self, version_tag):
        from pyspark.sql import SparkSession
        self.name = "spark-" + version_tag
        self.spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
                      .config("spark.sql.shuffle.partitions", "2").config("spark.driver.memory", "4g")
                      .config("spark.sql.session.timeZone", "UTC")
                      .config("spark.sql.ui.retainedExecutions", "10").getOrCreate())
        self.spark.sparkContext.setLogLevel("ERROR")

    def run(self, mode, fileset, shape, sqls):
        sp = self.spark
        conf = self.modes[mode]
        for k, v in conf.items():
            sp.conf.set(k, v)
        try:
            try:  # partition discovery can fail at load time (pair four: non-canonical directory names)
                sp.read.format(fileset["format"]).schema(spark_ddl(read_cols(shape))).load(fileset["dir"]) \
                    .createOrReplaceTempView("t")
            except Exception as exc:  # noqa: BLE001 - recorded, not hidden
                return [{"error": "load: " + str(exc).split("\n")[0][:280]}] * len(sqls)
            return [self._one(s) for s in sqls]
        finally:
            sp.catalog.dropTempView("t")
            for k in conf:
                sp.conf.unset(k)

    def _one(self, sql):
        try:
            return {"rows": [list(r) for r in self.spark.sql(sql).collect()]}
        except Exception as exc:  # noqa: BLE001 - recorded, not hidden
            c = getattr(exc, "java_exception", None)
            while c is not None and c.getCause() is not None:
                c = c.getCause()
            return {"error": ((c.toString() if c is not None else str(exc)).split("\n")[0])[:300]}

    def write(self, shape, wname, fmt, opts, d):
        """Spark's own writers (parquet-mr, orc-java): one output file per shape file."""
        from pyspark.sql import Row  # noqa: F401
        ddl = spark_ddl(shape["columns"])
        types = [parse_type(ty) for _, ty in shape["columns"]]
        os.makedirs(d, exist_ok=True)  # a shape with no file is an empty directory (C2 E3)
        for idx in shape["files"]:
            rows = [tuple(spark_value(v, t) for v, t in zip(shape["rows"][i], types)) for i in idx]
            df = self.spark.createDataFrame(rows, ddl).coalesce(1)
            w = df.write.format(fmt).options(**opts).mode("append")
            if shape.get("partition"):  # Spark's own directory naming (escaping, NULL and '' as default)
                w = w.partitionBy(*shape["partition"])
            w.save(d)


def spark_value(v, t):
    if v is None:
        return None
    if t[0] == "struct":
        return tuple(spark_value(v.get(n), c) for n, c in t[1])
    if t[0] == "list":
        return [spark_value(x, t[1]) for x in v]
    if t[0] == "map":
        return {k: spark_value(x, t[1]) for k, x in v}
    return v


# ------------------------------------------------------------------ DuckDB
class DuckDB(Engine):
    name, dialect, cores = "duckdb", "duckdb", 2
    modes = {"off": {"disabled_optimizers": "filter_pushdown,statistics_propagation"},
             "on": {}, "on_threads1": {"threads": "1"}}

    def __init__(self):
        import duckdb
        self.duckdb = duckdb
        self.name = "duckdb-" + duckdb.__version__

    def run(self, mode, fileset, shape, sqls):
        con = self.duckdb.connect()
        con.execute(f"SET threads={self.cores}")
        for k, v in self.modes[mode].items():
            con.execute(f"SET {k}='{v}'" if not v.isdigit() else f"SET {k}={v}")
        part = partition_cols(shape)
        glob = os.path.join(fileset["dir"], "**/*.parquet" if part else "*.parquet").replace("\\", "/")
        hive = ""
        if part:
            types = ", ".join(f"'{n}': '{sql_type(parse_type(ty), 'duckdb')}'" for n, ty in part)
            hive = f", hive_partitioning = true, hive_types = {{{types}}}"
        have = dict(shape["columns"])
        sel = []
        for n, ty in read_cols(shape):
            if n not in have:
                sel.append(f"CAST(NULL AS {sql_type(parse_type(ty), 'duckdb')}) AS {n}")
            elif have[n] != ty:
                sel.append(f"CAST({n} AS {sql_type(parse_type(ty), 'duckdb')}) AS {n}")
            else:
                sel.append(n)
        try:
            con.execute(f"CREATE VIEW t AS SELECT {', '.join(sel)} FROM read_parquet('{glob}'{hive})")
        except Exception as exc:  # noqa: BLE001 - e.g. no file in the directory (C2 E3)
            return [{"error": "view: " + str(exc).split("\n")[0][:280]}] * len(sqls)
        out = []
        for s in sqls:
            try:
                out.append({"rows": [list(r) for r in con.execute(s).fetchall()]})
            except self.duckdb.Error as exc:
                out.append({"error": str(exc).split("\n")[0][:300]})
        con.close()
        return out


# ------------------------------------------------------------------ DataFusion
DF_OFF = {"datafusion.execution.parquet.pruning": "false",
          "datafusion.execution.parquet.pushdown_filters": "false",
          "datafusion.execution.parquet.bloom_filter_on_read": "false",
          "datafusion.execution.parquet.enable_page_index": "false",
          "datafusion.execution.collect_statistics": "false"}


class DataFusion(Engine):
    name, dialect, cores = "datafusion", "datafusion", 2
    modes = {
        "off": DF_OFF,
        "default": {},
        "pushdown": {"datafusion.execution.parquet.pushdown_filters": "true",
                     "datafusion.execution.parquet.reorder_filters": "true"},
        "stats": {"datafusion.execution.collect_statistics": "true"},
        "no_page_index": {"datafusion.execution.parquet.enable_page_index": "false"},
    }

    def __init__(self):
        import datafusion
        self.df = datafusion
        self.name = "datafusion-" + datafusion.__version__

    def run(self, mode, fileset, shape, sqls):
        import pyarrow as pa
        conf = {"datafusion.execution.target_partitions": str(self.cores), **self.modes[mode]}
        ctx = self.df.SessionContext(self.df.SessionConfig(conf))
        kw = {}
        part = partition_cols(shape)
        if part:
            kw["table_partition_cols"] = [(n, to_arrow(parse_type(ty))) for n, ty in part]
        if shape["read_as"]:
            kw["schema"] = pa.schema([(n, to_arrow(parse_type(ty))) for n, ty in shape["read_as"]])
        out = []
        try:
            ctx.register_parquet("t", fileset["dir"], file_extension=".parquet", **kw)
        except Exception as exc:  # noqa: BLE001
            return [{"error": "register: " + str(exc).split("\n")[0][:280]}] * len(sqls)
        for s in sqls:
            try:
                tbl = ctx.sql(s).to_arrow_table()
                out.append({"rows": [list(r) for r in zip(*[c.to_pylist() for c in tbl.columns])]})
            except Exception as exc:  # noqa: BLE001
                out.append({"error": str(exc).split("\n")[0][:300]})
        return out


# ------------------------------------------------------------------ Polars (phase two)
class Polars(Engine):
    """scan_parquet over the file set, queried with Polars SQL. "off" turns predicate pushdown and
    row-group statistics off; the other modes are the default engine and the streaming engine."""
    name, dialect, cores = "polars", "polars", 2
    modes = {"off": {"stats": False, "pushdown": False, "engine": "in-memory"},
             "default": {"stats": True, "pushdown": True, "engine": "in-memory"},
             "streaming": {"stats": True, "pushdown": True, "engine": "streaming"}}

    def __init__(self):
        import polars as pl
        self.pl = pl
        self.name = "polars-" + pl.__version__

    def _dtype(self, ty):
        import pyarrow as pa
        return self.pl.from_arrow(pa.array([], type=to_arrow(parse_type(ty)))).dtype

    def run(self, mode, fileset, shape, sqls):
        pl, m = self.pl, self.modes[mode]
        part = partition_cols(shape)
        glob = os.path.join(fileset["dir"], "**/*.parquet" if part else "*.parquet").replace("\\", "/")
        try:
            kw = {"use_statistics": m["stats"]}
            if part:
                kw.update(hive_partitioning=True, hive_schema={n: self._dtype(ty) for n, ty in part})
            lf = pl.scan_parquet(glob, **kw)
            have = dict(shape["columns"])
            cols = []
            for n, ty in read_cols(shape):
                if n not in have:
                    cols.append(pl.lit(None, dtype=self._dtype(ty)).alias(n))
                elif have[n] != ty:
                    cols.append(pl.col(n).cast(self._dtype(ty)))
                else:
                    cols.append(pl.col(n))
            lf = lf.select(cols)
        except Exception as exc:  # noqa: BLE001
            return [{"error": "scan: " + str(exc).split("\n")[0][:280]}] * len(sqls)
        ctx = pl.SQLContext({"t": lf})
        opt = pl.QueryOptFlags(predicate_pushdown=m["pushdown"])
        out = []
        for s in sqls:
            try:
                df = ctx.execute(s).collect(engine=m["engine"], optimizations=opt)
                out.append({"rows": [list(r) for r in df.rows()]})
            except Exception as exc:  # noqa: BLE001
                out.append({"error": str(exc).split("\n")[0][:300]})
        return out


# ------------------------------------------------------------------ pyarrow.dataset (phase two)
class PyArrowDataset(Engine):
    """pyarrow.dataset over the file set. "off" reads every row and filters in memory; "scan" hands
    the filter to the scanner, which prunes row groups by their statistics."""
    name, dialect, formats, cores = "pyarrow", "pyarrow", ("parquet", "orc"), 2
    modes = {"off": {"push": False}, "scan": {"push": True}}

    def __init__(self):
        import pyarrow
        self.name = "pyarrow-" + pyarrow.__version__

    def run(self, mode, fileset, shape, sqls):
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.dataset as ds
        from templates import pa_query
        part = partition_cols(shape)
        cols = read_cols(shape)
        try:
            schema = pa.schema([(n, to_arrow(parse_type(ty))) for n, ty in cols])
            kw = {"partitioning": ds.partitioning(pa.schema([(n, to_arrow(parse_type(ty))) for n, ty in part]),
                                                 flavor="hive")} if part else {}
            dset = ds.dataset(fileset["dir"], format=fileset["format"], schema=schema, **kw)
            full = None if self.modes[mode]["push"] else dset.to_table()
        except Exception as exc:  # noqa: BLE001
            return [{"error": "dataset: " + str(exc).split("\n")[0][:280]}] * len(sqls)
        out = []
        for q in sqls:
            try:
                kind, flt, path = pa_query(q)
                if full is None:
                    tbl = dset.to_table(filter=flt) if flt is not None else dset.to_table()
                else:
                    tbl = full.filter(flt) if flt is not None else full
                if kind == "rows":
                    out.append({"rows": [[x] for x in tbl.column("k").to_pylist()]})
                    continue
                arr = tbl.column(path[0])
                for p in path[1:]:
                    arr = pc.struct_field(arr, p)
                ty = parse_type(dict(leaves_of(cols))[path])
                row = [tbl.num_rows, pc.count(arr).as_py(), pc.min(arr).as_py(), pc.max(arr).as_py()]
                if ty[0] in ("int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "float32", "float64",
                             "decimal"):
                    row.append(pc.sum(arr).as_py())
                out.append({"rows": [row]})
            except Exception as exc:  # noqa: BLE001
                out.append({"error": str(exc).split("\n")[0][:300]})
        return out


def leaves_of(cols):
    from templates import leaves
    return leaves(cols).items()


# ------------------------------------------------------------------ Trino (Docker, Hive connector on the local file system)
# Every session: the Hive connector's default MILLISECONDS precision rejects TIMESTAMP(6) tables
# (pair three harness gap; added for pair four).
TRINO_BASE = {"timestamp_precision": "MICROSECONDS"}
TRINO_OFF = {"parquet_ignore_statistics": "true", "parquet_use_column_index": "false",
             "parquet_use_bloom_filter": "false", "orc_bloom_filters_enabled": "false"}


class Trino(Engine):
    name, dialect, formats, cores = "trino", "trino", ("parquet", "orc"), 4
    modes = {"off": TRINO_OFF, "default": {}, "orc_bloom": {"orc_bloom_filters_enabled": "true"}}

    def __init__(self, base, mount="/qpdata", version="484"):
        import trino
        self.trino, self.base, self.mount = trino, os.path.abspath(base), mount
        self.name = "trino-" + version
        self.tables = {}
        self._sql("CREATE SCHEMA IF NOT EXISTS hive.qp", {})

    def _conn(self, props):
        return self.trino.dbapi.connect(host="localhost", port=8080, user="qp", catalog="hive", schema="qp",
                                        session_properties={f"hive.{k}": v for k, v in {**TRINO_BASE, **props}.items()})

    def _sql(self, sql, props):
        cur = self._conn(props).cursor()
        cur.execute(sql)
        return cur.fetchall()

    def _table(self, fileset, shape):
        if fileset["dir"] in self.tables:
            return self.tables[fileset["dir"]]
        # A fresh name per attempt (after pair four): a CREATE that succeeded before a failing
        # partition sync left its name taken, and every later table of the shard collided with it.
        self.attempts = getattr(self, "attempts", 0) + 1
        name = f"t{self.attempts}"
        rel = os.path.relpath(os.path.abspath(fileset["dir"]), self.base).replace("\\", "/")
        cols = ", ".join(f'"{n}" {sql_type(parse_type(ty), "trino")}' for n, ty in read_cols(shape))
        fmt = fileset["format"].upper()
        part = partition_cols(shape)
        by = (", partitioned_by = ARRAY[" + ", ".join(f"'{n}'" for n, _ in part) + "]") if part else ""
        self._sql(f"CREATE TABLE {name} ({cols}) WITH (external_location = 'local://{self.mount}/{rel}', "
                  f"format = '{fmt}'{by})", {})
        if part:  # register the partition directories in the file metastore
            self._sql(f"CALL system.sync_partition_metadata('qp', '{name}', 'FULL')", {})
        self.tables[fileset["dir"]] = name
        return name

    def run(self, mode, fileset, shape, sqls):
        try:
            name = self._table(fileset, shape)
        except Exception as exc:  # noqa: BLE001
            return [{"error": "create: " + str(exc).split("\n")[0][:280]}] * len(sqls)
        cur = self._conn(self.modes[mode]).cursor()
        out = []
        for s in sqls:
            try:
                cur.execute(s.replace(" FROM t", f" FROM {name}"))
                out.append({"rows": [list(r) for r in cur.fetchall()]})
            except Exception as exc:  # noqa: BLE001
                out.append({"error": str(exc).split("\n")[0][:300]})
        return out
