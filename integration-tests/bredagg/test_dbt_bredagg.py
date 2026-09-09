import pytest
from pathlib import Path
from dbt.cli.main import dbtRunner, dbtRunnerResult
from conftest import OracleDBHelper, ORA_SCHEMA


@pytest.fixture(scope="function")
def db(oracle_connection):
    """Function scoped clean slate. Tests run sequentially and share one source table, so the
    reset truncates and drops rather than building per-test objects."""
    helper = OracleDBHelper(oracle_connection)
    # Pre-test Reset / Defensive Cleanup
    helper.cleanup_leftovers()
    yield helper


def test_ephemeral_star_macro(db, dbt_run):
    #SOURCE = "bredagg_ephemeral_star_src" # actually ephemeral select model
    target = "bredagg_ephemeral_star_targ"
    dbt_run("run", "--select", f"+{target}")
    assert db.exists(target)
    cols = db.columns(target)
    for col in ["navn", "besk", "ikkemed", "test_ikkemed"]:
        assert col.upper() not in cols, f"fant {col} som ikke skal være der"
    for col in ["test_navn", "test_besk"]:
        assert col.upper() in cols, f"fant ikke {col} som SKAL være der"

def test_sync_multi_source_comments(db, dbt_run):
    # NB: missing test for source in different schema
    target = "bredagg_sync_multisource_comments_targ"
    source = "bredagg_testdata"
    source2 = "bredagg_testdata2"
    db.execute(
        f"create table {ORA_SCHEMA}.{source} ( "
        "navn varchar2(10 char), besk varchar2(20 char) "
        ")"
    )
    db.execute(f"comment on column {ORA_SCHEMA}.{source}.navn is 'Navn'")
    db.execute(f"comment on column {ORA_SCHEMA}.{source}.besk is 'Besk'")
    db.execute(
        f"create table {ORA_SCHEMA}.{source2} ( "
        "navn varchar2(10 char), besk varchar2(20 char), langbesk varchar2(40 char) "
        ")"
    )
    db.execute(f"comment on column {ORA_SCHEMA}.{source2}.navn is 'Alternativ Navn'")
    db.execute(f"comment on column {ORA_SCHEMA}.{source2}.besk is 'Alternativ Besk'")
    db.execute(f"comment on column {ORA_SCHEMA}.{source2}.langbesk is 'Langbesk'")

    dbt_run("run", "--select", target)
    assert db.exists(target)
    # post hook should have run comments
    comments = db.comments("bredagg_sync_multisource_comments_targ")
    #assert False
    assert comments["NAVN"] == "Alternativ Navn"
    assert comments["BESK"] == "Alternativ Besk"
    assert comments["LANGBESK"] == "Tekst fra yaml fil, vanlig DBT"
    pass