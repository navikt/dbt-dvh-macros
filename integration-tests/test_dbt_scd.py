# Property-based integrasjonstest 
import pytest
import os
import hypothesis
from dbt.cli.main import dbtRunner, dbtRunnerResult
from pathlib import Path



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

@pytest.mark.usefixtures("oracle_container")
def test_dbt_debug():
    run_dbt("debug")

@pytest.mark.usefixtures("oracle_container")
@pytest.mark.order(after="test_dbt_install_package")
def test_dbt_install_package():
    run_dbt("deps")

@pytest.mark.usefixtures("oracle_container")
@pytest.mark.order(after="test_dbt_install_package")
def test_dbt_seed():
    run_dbt("seed")

@pytest.mark.usefixtures("oracle_container")
@pytest.mark.order(after="test_dbt_seed")
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