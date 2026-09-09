import os
import pytest
from testcontainers.core import testcontainers_config
testcontainers_config.ryuk_disabled = True
from testcontainers.community.oracle import OracleDbContainer
import oracledb
from random import randbytes
from hashlib import sha256
from typing import NamedTuple
from pathlib import Path
from dbt.cli.main import dbtRunner, dbtRunnerResult


ORA_SCHEMA = "dbtuser"
TEMP_PREFIX = "o$pt_"
BACKUP_SUFFIX = "__dbt_backup"


class DbtEnvVarContext:
    """Set environment variables for one dbt invocation."""
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


class OracleConnectionConfig(NamedTuple):
    "Small helper class to store connection info for oracle connection"
    host:str
    port:int
    service_name:str
    user:str
    password:str
    app_user:str
    app_pass:str


@pytest.fixture(autouse=True, scope="session")
def oracle_connection():
    """Fixture to start and provide an Oracle test container with a connection object."""
    config = OracleConnectionConfig(
        host="127.0.0.1",
        port=1521,
        service_name="FREEPDB1",
        user="system",
        password=sha256(randbytes(64)).hexdigest(),
        app_user=ORA_SCHEMA,
        app_pass=sha256(randbytes(64)).hexdigest()

    )
    oracle = OracleDbContainer(
        oracle_password=config.password,
        dbname=config.service_name,
    )
    # Set the time zone to local, default is UTC which can cause issues when converting to local time
    oracle.with_env("TZ", "Europe/Oslo")
    oracle.with_env("ORA_SDTZ", "Europe/Oslo")
    oracle.with_env("APP_USER", config.app_user)
    oracle.with_env("APP_USER_PASSWORD", config.app_pass)

    ## do not bind to 0.0.0.0 to avoid exposing the database to all interfaces
    #oracle.with_bind_ports(1521, (config.host, config.port)) #type: ignore
    # do not bind to 0.0.0.0 to avoid exposing the database to all interfaces.
    # with_bind_ports() only accepts a host port, so the mapping is set directly;
    # the docker API accepts an (interface, port) tuple as the host side.
    oracle.ports = {f"{oracle.port}/tcp": (config.host, config.port)} # type: ignore[dict-item]
    # fixed name for reuse and error messages should a container be running from before
    oracle.with_name("testcontainers-oracle-db")

    try:
        oracle.start()
        assert oracle.get_container_host_ip() != "0.0.0.0", "bad host"
        env_vars = {
            "DBT_USER": config.app_user,
            "DBT_ENV_SECRET_PASS": config.app_pass,
            "DBT_HOST": config.host,
            "DBT_PORT": str(config.port),
            "DBT_SERVICE": config.service_name,
            "DBT_DATABASE": config.service_name,
            "DBT_SCHEMA": config.app_user,
            "ORA_PYTHON_DRIVER_TYPE": "thin",
        }
        for k, v in env_vars.items():
            os.environ[k] = str(v)
        # Create a persistent connection for setup/teardown
        with oracledb.connect(
            user=config.app_user, # type: ignore
            password=config.app_pass,
            host=config.host,
            port=config.port,
            service_name=config.service_name
        ) as con:
            yield con
        for k in env_vars:
            del os.environ[k]
    finally:
        oracle.stop()


class OracleDBHelper:
    """Thin helper over the session connection, scoped to one test. No caching."""
    def __init__(self, con):
        self.con = con

    def execute(self, sql, **binds):
        with self.con.cursor() as cur:
            cur.execute(sql, **binds)
            self.con.commit()

    def query(self, sql, **binds):
        """Returns all rows in record format [{col:data}]"""
        with self.con.cursor() as cur:
            cur.execute(sql, **binds)
            cols = [c[0].lower() for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def materializations(self, name):
        """Every all_objects type for name, materialized views first.
        A materialized view also owns a TABLE entry, so the order decides which drop is correct."""
        rows = self.query(
            "select object_type from all_objects "
            "where owner = :o and object_name = :n "
            "and object_type in ('TABLE', 'VIEW', 'MATERIALIZED VIEW') "
            "order by case object_type when 'MATERIALIZED VIEW' then 0 else 1 end",
            o=ORA_SCHEMA.upper(), n=name.upper(),
        )
        return [r["object_type"] for r in rows]

    def exists(self, name):
        return bool(self.materializations(name))

    def relation_type(self, name):
        types = self.materializations(name)
        return types[0] if types else None

    def drop(self, name):
        """If exists, drop name whatever it currently is. The truncate matters for the global temporary
        tables the materialization leaves behind: they are 'on commit preserve rows', and a bare
        drop raises ORA-14452."""
        kind = self.relation_type(name)
        if kind is None:
            return
        with self.con.cursor() as cur:
            if kind == "TABLE":
                try:
                    self.truncate_table(name)
                except oracledb.DatabaseError:
                    pass
                cur.execute(f'drop table {ORA_SCHEMA}."{name.upper()}" cascade constraints purge')
            elif kind == "VIEW":
                cur.execute(f"drop view {ORA_SCHEMA}.{name}")
            else:
                cur.execute(f"drop materialized view {ORA_SCHEMA}.{name}")
            self.con.commit()

    def temp_leftovers(self):
        """Temporary oracle source relations dbt failed to clean up.
        oracle__make_temp_relation builds 'o$pt_' ~ identifier ~ strftime("%H%M%S%f"),
        """
        rows = self.query(
            "select object_name from all_objects where owner = :o and object_name like :p",
            o=ORA_SCHEMA.upper(), p=f"{TEMP_PREFIX.upper()}%",
        )
        return sorted(r["object_name"] for r in rows)

    def backup_leftovers(self):
        """Backup target relations oracle dbt failed to clean up.
        the default is identifier ~ __dbt_backup"""
        rows = self.query(
            "select object_name from all_objects where owner = :o and object_name like :p",
            o=ORA_SCHEMA.upper(), p=f"%{BACKUP_SUFFIX.upper()}",
        )
        return sorted(r["object_name"] for r in rows)

    def count(self, name):
        return self.query(f"select count(*) as n from {ORA_SCHEMA}.{name}")[0]["n"]
    
    def cleanup_leftovers(self):
        """Drop all temp and backup materializations"""
        for name in self.temp_leftovers() + self.backup_leftovers():
            self.drop(name)

    def truncate_table(self, name):
        """Truncate a table to remove all rows, will throw if not a table or not exist"""
        self.execute(f"truncate table {ORA_SCHEMA}.{name}")

    def columns(self, name):
        """Get columns of relation {colname: {}}"""
        return {
            r["column_name"]: r for r in self.query(
                "select column_name, data_type, data_length, data_precision, data_scale "
                "from all_tab_columns where owner = :o and table_name = :n",
                o=ORA_SCHEMA.upper(), n=name.upper()
            )
        }
    
    def comments(self, name):
        """Get comments on relation columns {colname: comment}"""
        return {
            r["column_name"]: r["comments"] for r in self.query(
                "select column_name, comments "
                "from all_col_comments where owner = :o and table_name = :n",
                o=ORA_SCHEMA.upper(), n=name.upper()
            )
        }

@pytest.fixture(scope="function")
def dbt_run():
    """Invoke dbt against the bundled project. Partial parsing is disabled because the model and
    its properties are driven by environment variables that change between tests."""
    dbt_folder = str(Path(__file__).parent / "dbt")

    def run(*args, expect_failure=False):
        cli_args = list(args) + [
            "--project-dir", dbt_folder,
            "--profiles-dir", dbt_folder,
            "--no-partial-parse",
        ]
        result: dbtRunnerResult = dbtRunner().invoke(cli_args)
        if expect_failure:
            assert not result.success, f"expected dbt to fail, it succeeded: {result.result}"
            return result
        assert result.success, result.result or result.exception
        return result

    return run