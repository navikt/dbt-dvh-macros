# Property-based integrasjonstest 
import pytest
import os
from hypothesis import given, settings, strategies as st
from dbt.cli.main import dbtRunner, dbtRunnerResult
from pathlib import Path
from datetime import datetime

RC_INIT = 5
RC_DELTA = 3

class DbtEnvVarContext:
    def __init__(self, **kwargs) -> None:
        self._kwargs = kwargs
    def __enter__(self):
        os.environ.update(**self._kwargs)
    def __exit__(self, exc_type, exc_val, exc_tb):
        for k in self._kwargs:
            del os.environ[k]

def run_dbt(*args):
    runner: dbtRunner = dbtRunner()
    dbt_folder = str(Path(__file__).parent / "dbt")
    cli_args = list(args) + [
        "--project-dir", dbt_folder,
        "--profiles-dir", dbt_folder,
    ]
    original_dir = Path.cwd()
    try:
        run: dbtRunnerResult = runner.invoke(cli_args)
        os.chdir(dbt_folder)
    finally:
        os.chdir(original_dir)
    assert run.success, run.result or run.exception

@pytest.mark.usefixtures("oracle_connection")
def test_dbt_debug():
    run_dbt("debug")

@pytest.mark.usefixtures("oracle_connection")
@pytest.mark.order(after="test_dbt_debug")
@settings(deadline=2000)
@given(
    kode=st.lists(st.text(min_size=8, max_size=8), min_size=RC_INIT, max_size=RC_INIT),
    navn=st.lists(st.text(min_size=32, max_size=32), min_size=RC_INIT, max_size=RC_INIT),
    oppdatert=st.lists(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime.today()), min_size=RC_INIT, max_size=RC_INIT),
    opprettet=st.lists(st.datetimes(min_value=datetime(1900, 1, 1), max_value=datetime(2020, 1, 1)), min_size=RC_INIT, max_size=RC_INIT)
    )
def test_dbt_incremental(oracle_connection, kode, navn, oppdatert, opprettet):
    table = oracle_connection.username + ".scd_raadata"
    with oracle_connection.cursor() as cur:
        cur.execute(f"drop table if exists {table}")
        cur.execute(f"create table {table} "
            "(kode varchar2(8 char), navn varchar2(32 char), "
            "oppdatert timestamp(6), opprettet timestamp(6))"
            )
        cur.executemany(
            f"insert into {table} (navn, kode, oppdatert, opprettet) values(:navn, :kode, :oppdatert, :opprettet)",
            parameters=[dict(kode=k, navn=n, oppdatert=od, opprettet=ot) for k,n,od,ot in zip(kode,navn,oppdatert,opprettet)]
        )
        oracle_connection.commit()
    
    with DbtEnvVarContext(
        SCD_TYPE="0",
        FILTER_MODE="changed_at",
    ):
        run_dbt("run", "--select", "dim_scd_default")
    
    with oracle_connection.cursor() as cur:
        cur.execute(f"drop table if exists {table}")

@pytest.mark.usefixtures("oracle_connection")
@pytest.mark.order(after="test_dbt_incremental")
def test_dbt_run_scd0_default():
    with DbtEnvVarContext(
        SCD_TYPE=0,
        FILTER_MODE="changed_at",
    ):
        run_dbt("run", "--select", "dim_scd_default")


#def validate_results(connection, query, expected_results):
#    """Validate the results of a query against expected results."""
#    with connection.cursor() as cursor:
#        cursor.execute(query)
#        results = cursor.fetchall()
#        assert results == expected_results, f"Validation failed: {results} != {expected_results}"



#service_name = oracle.dbname
#with oracledb.connect(
#    #user="system",
#    #password=oracle.oracle_password,
#    service_name=oracle.dbname, # type: ignore
#    user=oracle.username, # type: ignore
#    password=oracle.password, # type: ignore
#    host=oracle.get_container_host_ip(),
#    port=oracle.get_exposed_port(oracle.port),
#    
#) as con: