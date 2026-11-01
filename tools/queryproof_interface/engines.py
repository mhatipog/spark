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
                      .config("spark.sql.ui.retainedExecutions", "10").getOrCreate())
        self.spark.sparkContext.setLogLevel("ERROR")

    def run(self, mode, fileset, shape, sqls):
        sp = self.spark
        conf = self.modes[mode]
        for k, v in conf.items():
            sp.conf.set(k, v)
        try:
            sp.read.format(fileset["format"]).schema(spark_ddl(read_cols(shape))).load(fileset["dir"]) \
                .createOrReplaceTempView("t")
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
        for idx in shape["files"]:
            rows = [tuple(spark_value(v, t) for v, t in zip(shape["rows"][i], types)) for i in idx]
            df = self.spark.createDataFrame(rows, ddl).coalesce(1)
            df.write.format(fmt).options(**opts).mode("append").save(d)


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
        glob = os.path.join(fileset["dir"], "*.parquet").replace("\\", "/")
        have = dict(shape["columns"])
        sel = []
        for n, ty in read_cols(shape):
            if n not in have:
                sel.append(f"CAST(NULL AS {sql_type(parse_type(ty), 'duckdb')}) AS {n}")
            elif have[n] != ty:
                sel.append(f"CAST({n} AS {sql_type(parse_type(ty), 'duckdb')}) AS {n}")
            else:
                sel.append(n)
        con.execute(f"CREATE VIEW t AS SELECT {', '.join(sel)} FROM read_parquet('{glob}')")
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


# ------------------------------------------------------------------ Trino (Docker, Hive connector on the local file system)
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
                                        session_properties={f"hive.{k}": v for k, v in props.items()})

    def _sql(self, sql, props):
        cur = self._conn(props).cursor()
        cur.execute(sql)
        return cur.fetchall()

    def _table(self, fileset, shape):
        if fileset["dir"] in self.tables:
            return self.tables[fileset["dir"]]
        name = f"t{len(self.tables)}"
        rel = os.path.relpath(os.path.abspath(fileset["dir"]), self.base).replace("\\", "/")
        cols = ", ".join(f'"{n}" {sql_type(parse_type(ty), "trino")}' for n, ty in read_cols(shape))
        fmt = fileset["format"].upper()
        self._sql(f"CREATE TABLE {name} ({cols}) WITH (external_location = 'local://{self.mount}/{rel}', "
                  f"format = '{fmt}')", {})
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
