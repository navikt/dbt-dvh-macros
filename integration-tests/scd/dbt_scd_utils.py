from datetime import datetime, timedelta
from typing import NamedTuple
import oracledb
import os

SCHEMA = "dbtuser"
SOURCE = "testdata"
TARGET = "dim_testdata"
BACKUP = f"{TARGET}__dbt_backup"
# oracle__make_temp_relation builds 'o$pt_' ~ identifier ~ strftime("%H%M%S%f"),
# so every run leaks a differently named table and only the prefix is stable
TEMP_PREFIX = f"o$pt_{TARGET}"

SCD_TYPES = [0, 1, 2]
FILTER_MODES = ["scd_key", "changed_at", "changed_at_per_scd_key"]

EPOCH = datetime(2020, 1, 1)

# the macro defaults from SCD__validate_config, and the overrides dim_testdata.sql applies when
# USE_CUSTOM_NAMES is true. Tests assert against whichever set is active.
DEFAULT_NAMES = {
    "primary_key": f"pk_{TARGET}",
    "changed_at": "oppdatert_tid_kilde",
    "created_at": "opprettet_tid_kilde",
    "valid_from": "gyldig_fom_tid",
    "valid_to": "gyldig_til_tid",
    "valid_flag": "gyldig_flagg",
    "updated_at": "oppdatert_dato",
    "loaded_at": "lastet_dato",
}

CUSTOM_NAMES = {
    "primary_key": "pk_test",
    "changed_at": "endret",
    "created_at": "opprettet",
    "valid_from": "gyldig_fra_og_med",
    "valid_to": "gyldig_til",
    "valid_flag": "gyldig_naa",
    "updated_at": "oppdatert",
    "loaded_at": "lastet",
}


class Row(NamedTuple):
    """One source row. tid1 feeds changed_at and tid2 feeds created_at."""
    pk: str
    kode1: str
    kode2: str
    navn1: str
    navn2: str
    tid1: datetime
    tid2: datetime


class DbtEnvVarContext:
    """Set the environment variables that drive the model and its config for one dbt invocation."""

    def __init__(self, **kwargs) -> None:
        self._kwargs = {k: str(v) for k, v in kwargs.items()}
        self._restore = {}

    def __enter__(self):
        for k, v in self._kwargs.items():
            self._restore[k] = os.environ.get(k)
            os.environ[k] = v
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for k, old in self._restore.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old


class Db:
    """Thin helper over the session connection, scoped to one test."""

    def __init__(self, con):
        self.con = con

    def execute(self, sql, **binds):
        with self.con.cursor() as cur:
            cur.execute(sql, **binds)
            self.con.commit()

    def query(self, sql, **binds):
        with self.con.cursor() as cur:
            cur.execute(sql, **binds)
            cols = [c[0].lower() for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def object_types(self, name):
        """Every all_objects type for name, materialized views first.
        A materialized view also owns a TABLE entry, so the order decides which drop is correct."""
        rows = self.query(
            "select object_type from all_objects "
            "where owner = :o and object_name = :n "
            "and object_type in ('TABLE', 'VIEW', 'MATERIALIZED VIEW') "
            "order by case object_type when 'MATERIALIZED VIEW' then 0 else 1 end",
            o=SCHEMA.upper(), n=name.upper(),
        )
        return [r["object_type"] for r in rows]

    def exists(self, name):
        return bool(self.object_types(name))

    def relation_type(self, name):
        types = self.object_types(name)
        return types[0] if types else None

    def drop(self, name):
        """Drop name whatever it currently is. The truncate matters for the global temporary
        tables the materialization leaves behind: they are 'on commit preserve rows', and a bare
        drop raises ORA-14452."""
        kind = self.relation_type(name)
        if kind is None:
            return
        with self.con.cursor() as cur:
            if kind == "TABLE":
                try:
                    cur.execute(f"truncate table {SCHEMA}.{name}")
                except oracledb.DatabaseError:
                    pass
                cur.execute(f'drop table {SCHEMA}."{name.upper()}" cascade constraints purge')
            elif kind == "VIEW":
                cur.execute(f"drop view {SCHEMA}.{name}")
            else:
                cur.execute(f"drop materialized view {SCHEMA}.{name}")
            self.con.commit()

    def temp_leftovers(self):
        """Temporary source relations the materialization failed to clean up."""
        rows = self.query(
            "select object_name from all_objects where owner = :o and object_name like :p",
            o=SCHEMA.upper(), p=f"{TEMP_PREFIX.upper()}%",
        )
        return sorted(r["object_name"] for r in rows)

    def load(self, rows):
        """Append rows to the source table."""
        with self.con.cursor() as cur:
            cur.executemany(
                f"insert into {SCHEMA}.{SOURCE} (pk, kode1, kode2, navn1, navn2, tid1, tid2) "
                "values (:pk, :kode1, :kode2, :navn1, :navn2, :tid1, :tid2)",
                [r._asdict() for r in rows],
            )
            self.con.commit()

    def target_rows(self, name=TARGET, order_by=None):
        sql = f"select * from {SCHEMA}.{name}"
        if order_by:
            sql += f" order by {order_by}"
        return self.query(sql)

    def count(self, name=TARGET):
        return self.query(f"select count(*) as n from {SCHEMA}.{name}")[0]["n"]

    def reset(self):
        """Return the schema to the state a first-ever dbt run would see."""
        self.execute(f"truncate table {SCHEMA}.{SOURCE}")
        for name in [TARGET, BACKUP, *self.temp_leftovers()]:
            self.drop(name)



def make_rows(n, *, batch=0, first_key=0, changed_at=None):
    """Build n rows whose scd_key columns are stable across batches but whose data columns
    move with the batch number, so re-loading a later batch is a genuine change."""
    changed_at = changed_at if changed_at is not None else EPOCH + timedelta(days=batch)
    return [
        Row(
            pk=f"pk-{first_key + i}",
            kode1=f"kode1-{first_key + i}",
            kode2=f"k2-{first_key + i}",
            navn1=f"navn1 batch{batch} nr{first_key + i}",
            navn2=f"navn2 b{batch} n{first_key + i}",
            tid1=changed_at,
            tid2=EPOCH - timedelta(days=365),
        )
        for i in range(n)
    ]



def names(use_custom_names=False):
    return CUSTOM_NAMES if use_custom_names else DEFAULT_NAMES


def scd_env(scd_type=2, scd_key="kode1", scd_hash="", filter_mode="changed_at",
            schema_changes="", use_custom_names=False, use_existing_pk=False,
            exclude_columns=""):
    """Every variable the model and properties.yml read, always set. env_var without a default
    raises EnvVarMissingError, and an unset SCD_KEY would render as an empty list."""
    return DbtEnvVarContext(
        SCD_TYPE=scd_type,
        SCD_KEY=scd_key,
        SCD_HASH=scd_hash,
        FILTER_MODE=filter_mode,
        SCHEMA_CHANGES=schema_changes,
        USE_CUSTOM_NAMES="true" if use_custom_names else "false",
        USE_EXISTING_PK="true" if use_existing_pk else "false",
        EXCLUDE_COLUMNS=exclude_columns,
    )
