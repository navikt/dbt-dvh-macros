# Property-based integrasjonstest 
import pytest
import os
from hypothesis import given, settings, strategies as st, reproduce_failure, Phase
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


#@reproduce_failure('6.151.9', b'AXicazEAghZUohVEGEwxwAJoIejE/sSREQgZYJA6AheoL+DIBADL0Dml')
@pytest.mark.usefixtures("oracle_connection")
@settings(deadline=2000, print_blob=True, phases=[Phase.generate])
@given(
    kode=st.lists(st.text(min_size=4, max_size=12), min_size=RC_INIT, max_size=RC_INIT),
    navn=st.lists(st.text(min_size=20, max_size=40), min_size=RC_INIT, max_size=RC_INIT),
    oppdatert=st.lists(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime.today()), min_size=RC_INIT, max_size=RC_INIT),
    opprettet=st.lists(st.datetimes(min_value=datetime(1900, 1, 1), max_value=datetime(2020, 1, 1)), min_size=RC_INIT, max_size=RC_INIT),
    changed_at=st.sampled_from(["scd_key", "changed_at", "changed_at_per_scd_key"]),
)
def test_dbt_default_scd(oracle_connection, kode, navn, oppdatert, opprettet, changed_at):
    table = oracle_connection.username + ".scd_raadata"

    with oracle_connection.cursor() as cur:
        cur.executemany(
            f"insert into {table} (navn, kode, oppdatert, opprettet) values(:navn, :kode, :oppdatert, :opprettet)",
            parameters=[dict(kode=k, navn=n, oppdatert=od, opprettet=ot) for k,n,od,ot in zip(kode,navn,oppdatert,opprettet)]
        )
        oracle_connection.commit()
    
    with DbtEnvVarContext(
        FILTER_MODE=changed_at,
    ):
        run_dbt("run", "--select", "dim_scd0", "dim_scd1", "dim_scd2")
