# Property-based integrasjonstest 
import pytest
import os
from hypothesis import given, settings, strategies as st, reproduce_failure, Phase
from dbt.cli.main import dbtRunner, dbtRunnerResult
from pathlib import Path
from datetime import datetime
import csv


BATCH_SIZE = 5

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

def write_csv(fp, cursor):
    headers = [col[0] for col in cursor.description]
    with open(fp, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        while True:
            rows = cursor.fetchmany()
            if not rows:
                break
            writer.writerows(rows)

@pytest.mark.usefixtures("oracle_connection")
@settings(deadline=4000, print_blob=True, phases=[Phase.generate]) # stop at first failure
@given(
    kode=st.lists(st.text(min_size=4, max_size=12), min_size=BATCH_SIZE, max_size=BATCH_SIZE),
    navn=st.lists(st.text(min_size=20, max_size=40), min_size=BATCH_SIZE, max_size=BATCH_SIZE),
    oppdatert=st.lists(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime.today()), min_size=BATCH_SIZE, max_size=BATCH_SIZE),
    opprettet=st.lists(st.datetimes(min_value=datetime(1900, 1, 1), max_value=datetime(2020, 1, 1)), min_size=BATCH_SIZE, max_size=BATCH_SIZE),
    changed_at=st.sampled_from(["scd_key", "changed_at", "changed_at_per_scd_key"]),
)
def test_dbt_default_scd(oracle_connection, kode, navn, oppdatert, opprettet, changed_at):
    with oracle_connection.cursor() as cur:
        #cur.execute("truncate table dbtuser.scd_raadata")
        cur.executemany(
            "insert into dbtuser.scd_raadata (navn, kode, oppdatert, opprettet) values(:navn, :kode, :oppdatert, :opprettet)",
            parameters=[dict(kode=k, navn=n, oppdatert=od, opprettet=ot) for k,n,od,ot in zip(kode,navn,oppdatert,opprettet)]
        )
        oracle_connection.commit()
    
    with DbtEnvVarContext(
        FILTER_MODE=changed_at,
    ):
        try:
            run_dbt("run", "--select", "dim_scd0", "dim_scd1", "dim_scd2")
        except Exception:
            # dump tables to csv
            tables = ["scd_raadata", "dim_scd0", "dim_scd1", "dim_scd2"]
            folder = Path(__file__).parent.parent / "failures"
            folder.mkdir(exist_ok=True)
            for table in tables:
                path = (folder / (table + ".csv"))
                try:
                    with oracle_connection.cursor() as cur:
                        cur.arraysize = 1000
                        cur.execute(f"select * from dbtuser.{table}")
                        write_csv(path, cur)
                except Exception:
                    # may occur if table not created?
                    pass
            raise