"""Integration tests for the scd materialization.

The dbt project holds a single generic model, dim_testdata, whose shape and column naming are
driven by environment variables. Each test picks a combination, loads rows into dbtuser.testdata
and runs dbt, so one model covers the whole configuration space.

Tests share the source table and run sequentially, so the db fixture truncates and drops rather
than building its own objects.
"""
import pytest
from pathlib import Path
from datetime import datetime, timedelta
from hypothesis import given, settings, assume, HealthCheck, strategies as st
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt_scd_utils import (
    BACKUP,
    SCHEMA,
    SOURCE,
    TARGET,
    EPOCH,
    SCD_TYPES,
    FILTER_MODES,
    Row,
    make_rows,
    names,
    scd_env,
    Db
)


@pytest.fixture
def db(oracle_connection):
    """Function scoped clean slate. Tests run sequentially and share one source table, so the
    reset truncates and drops rather than building per-test objects."""
    helper = Db(oracle_connection)
    helper.reset()
    yield helper


@pytest.fixture
def dbt_run():
    """Invoke dbt against the bundled project. Partial parsing is disabled because the model and
    its properties are driven by environment variables that change between tests."""
    dbt_folder = str(Path(__file__).parent.parent / "dbt")

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


def assert_clean(db):
    """Nothing transient may outlive a run: no temporary source relation, no backup."""
    assert db.temp_leftovers() == [], f"temporary source relations left behind: {db.temp_leftovers()}"
    assert not db.exists(BACKUP), f"{BACKUP} left behind"


def columns_of(db, relation=TARGET):
    return {
        r["column_name"].lower()
        for r in db.query(
            "select column_name from all_tab_columns where owner = :o and table_name = :t",
            o=SCHEMA.upper(), t=relation.upper(),
        )
    }


def assert_is_scd_table(db, use_custom_names=False, relation=TARGET):
    """The target must be a table and carry every column the materialization maintains."""
    assert db.relation_type(relation) == "TABLE", (
        f"{relation} is {db.relation_type(relation)}, expected TABLE"
    )
    cols = columns_of(db, relation)
    n = names(use_custom_names)
    for key in ("primary_key", "valid_from", "valid_to", "valid_flag", "updated_at", "loaded_at"):
        assert n[key] in cols, f"{n[key]} missing from {relation}, has {sorted(cols)}"


# --------------------------------------------------------------------------------------------
# Configuration matrix
# --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("scd_type", SCD_TYPES)
@pytest.mark.parametrize("filter_mode", FILTER_MODES)
@pytest.mark.parametrize("use_custom_names", [False, True])
@pytest.mark.parametrize("use_existing_pk", [False, True])
def test_first_run_creates_scd_table(db, dbt_run, scd_type, filter_mode, use_custom_names, use_existing_pk):
    """Every combination must build a valid scd table from an empty schema and clean up after
    itself. This is the broadest guard: the row validation in step 8 runs on each of these, so a
    combination that produces overlapping or multiply-valid rows fails here."""
    rows = make_rows(5)
    db.load(rows)

    with scd_env(scd_type=scd_type, filter_mode=filter_mode,
                 use_custom_names=use_custom_names, use_existing_pk=use_existing_pk):
        dbt_run("run", "--select", TARGET)

    assert_is_scd_table(db, use_custom_names)
    assert db.count() == len(rows)
    assert_clean(db)


@pytest.mark.parametrize("scd_type", SCD_TYPES)
@pytest.mark.parametrize("filter_mode", FILTER_MODES)
def test_second_run_applies_scd_semantics(db, dbt_run, scd_type, filter_mode):
    """Reload the same keys with changed data. The row count afterwards is what separates the
    three types: 0 ignores the change, 1 overwrites in place, 2 adds a version.

    filter_mode scd_key is the exception: it drops any source row whose key is already present,
    whatever its changed_at, so an existing key is never revisited by any type."""
    keys = 4
    db.load(make_rows(keys, batch=0))
    env = dict(scd_type=scd_type, filter_mode=filter_mode)

    with scd_env(**env):
        dbt_run("run", "--select", TARGET)
    assert db.count() == keys

    # replace rather than append, so the second run sees only the changed batch and the expected
    # row count does not depend on how filter_mode re-reads old source rows
    db.execute(f"truncate table {SCHEMA}.testdata")
    db.load(make_rows(keys, batch=1))
    with scd_env(**env):
        dbt_run("run", "--select", TARGET)

    n = names()
    versions_expected = scd_type == 2 and filter_mode != "scd_key"
    expected = 2 * keys if versions_expected else keys
    assert db.count() == expected, f"scd_type {scd_type} produced {db.count()} rows, expected {expected}"

    # exactly one currently valid row per key, whatever the type
    valid = db.query(
        f"select count(*) as n from {SCHEMA}.{TARGET} where {n['valid_flag']} = 1"
    )[0]["n"]
    assert valid == keys, f"expected {keys} valid rows, found {valid}"
    assert_clean(db)


@pytest.mark.parametrize("scd_type", SCD_TYPES)
def test_filter_mode_scd_key_only_admits_new_keys(db, dbt_run, scd_type):
    """The flip side of the case above: a key not yet in the target is still inserted."""
    db.load(make_rows(3, batch=0))
    with scd_env(scd_type=scd_type, filter_mode="scd_key"):
        dbt_run("run", "--select", TARGET)

    # one repeat of an existing key plus two genuinely new ones
    db.execute(f"truncate table {SCHEMA}.testdata")
    db.load(make_rows(1, batch=1, first_key=0) + make_rows(2, batch=1, first_key=50))
    with scd_env(scd_type=scd_type, filter_mode="scd_key"):
        dbt_run("run", "--select", TARGET)

    assert db.count() == 5, "new keys were not inserted, or the existing key was revisited"
    assert_clean(db)


@pytest.mark.parametrize("scd_type", SCD_TYPES)
def test_rows_tied_on_changed_at_are_deduplicated(db, dbt_run, scd_type):
    """With no scd_hash the source is deduplicated on (scd_key, changed_at) by
    rn_dedup_tied_changed_at, so tied rows cannot produce two rows valid at once."""
    rows = make_rows(2, batch=0, first_key=0)
    rows = [r._replace(kode1="samme_kode") for r in rows]
    db.load(rows)

    with scd_env(scd_type=scd_type):
        dbt_run("run", "--select", TARGET)

    assert db.count() == 1, "tied rows were not deduplicated"
    assert_clean(db)


@pytest.mark.parametrize("scd_type", SCD_TYPES)
def test_several_versions_of_one_key_in_a_single_run(db, dbt_run, scd_type):
    """The untied counterpart of the case above. Two source rows share a key but have distinct
    changed_at, so nothing deduplicates them and one run has to version them against each other
    rather than against an existing target row. Only type 2 keeps the history, but every type must
    end up with the latest version valid and with exactly one valid row."""
    db.load([
        Row(pk="p0", kode1="K", kode2="x", navn1="foer", navn2="b",
            tid1=EPOCH, tid2=EPOCH - timedelta(days=365)),
        Row(pk="p1", kode1="K", kode2="x", navn1="etter", navn2="d",
            tid1=EPOCH + timedelta(days=1), tid2=EPOCH - timedelta(days=365)),
    ])

    with scd_env(scd_type=scd_type, scd_key="kode1"):
        dbt_run("run", "--select", TARGET)

    n = names()
    expected_total = 2 if scd_type == 2 else 1
    assert db.count() == expected_total, (
        f"scd_type {scd_type} produced {db.count()} rows, expected {expected_total}"
    )

    valid = db.query(
        f"select navn1 from {SCHEMA}.{TARGET} where {n['valid_flag']} = 1"
    )
    assert len(valid) == 1, f"expected one valid row, found {len(valid)}"
    assert valid[0]["navn1"] == "etter", "the valid row is not the latest version"
    assert_clean(db)


# --------------------------------------------------------------------------------------------
# Full refresh
# --------------------------------------------------------------------------------------------

def test_full_refresh_rebuilds_target_and_removes_backup(db, dbt_run):
    """A successful full refresh rebuilds from the source only, and the backup it took on the way
    is transient: it must not survive the run."""
    db.load(make_rows(3, batch=0))
    with scd_env():
        dbt_run("run", "--select", TARGET)
    assert db.count() == 3

    # replace the source contents entirely, so a rebuild is visible in the row count
    db.execute(f"truncate table {SCHEMA}.testdata")
    db.load(make_rows(2, batch=1, first_key=100))

    with scd_env():
        dbt_run("run", "--select", TARGET, "--full-refresh")

    assert db.count() == 2, "full refresh did not rebuild the target from the source alone"
    assert_is_scd_table(db)
    assert_clean(db)


def test_full_refresh_keeps_backup_when_the_run_fails(db, dbt_run):
    """The recoverability property. Once the backup is taken the target is gone, so any later
    failure must leave the backup in place, otherwise the previous data is unrecoverable.

    A null changed_at feeds a null into valid_from, which the materialization declares not null,
    so the merge raises after the backup has already been taken."""
    db.load(make_rows(3, batch=0))
    with scd_env():
        dbt_run("run", "--select", TARGET)
    assert db.count() == 3

    db.execute(f"truncate table {SCHEMA}.testdata")
    # two versions of one key, the later one with a null changed_at. The first version takes
    # valid_from from created_at, but the second takes it straight from changed_at, so the not
    # null column rejects it and the merge raises with the backup already in place.
    poisoned = make_rows(2, batch=1, first_key=100)
    poisoned = [r._replace(kode1="samme_kode") for r in poisoned]
    poisoned[1] = poisoned[1]._replace(tid1=None)
    db.load(poisoned)

    with scd_env():
        dbt_run("run", "--select", TARGET, "--full-refresh", expect_failure=True)

    assert db.exists(BACKUP), "full refresh backup was destroyed by a failed run"
    assert db.count(BACKUP) == 3, "backup does not hold the previous data"


# --------------------------------------------------------------------------------------------
# Migrating an existing relation
# --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("kind", ["view", "materialized view"])
def test_existing_view_is_migrated_to_table(db, dbt_run, kind):
    """A user switching an existing view or materialized view over to the scd materialization.
    Oracle can rename neither, so the macro copies the data into a table."""
    db.load(make_rows(3))
    with scd_env():
        dbt_run("run", "--select", TARGET)
    n = names()

    # rebuild the target as a view/mview over the same shape, so it passes column validation
    db.execute(f"create table {SCHEMA}.scd_snapshot as select * from {SCHEMA}.{TARGET}")
    db.drop(TARGET)
    db.execute(f"create {kind} {SCHEMA}.{TARGET} as select * from {SCHEMA}.scd_snapshot")
    assert db.relation_type(TARGET) == kind.upper()

    with scd_env():
        dbt_run("run", "--select", TARGET)

    assert_is_scd_table(db)
    assert db.count() == 3, "data was lost while migrating to a table"
    assert_clean(db)
    db.drop("scd_snapshot")


# --------------------------------------------------------------------------------------------
# Failure paths
# --------------------------------------------------------------------------------------------

def test_missing_scd_key_is_a_config_error(db, dbt_run):
    """An unset SCD_KEY renders as an empty list. Without the guard it reaches SQL generation and
    produces 'partition by  order by', a syntax error rather than a clear config error."""
    db.load(make_rows(2))
    with scd_env(scd_key=""):
        dbt_run("run", "--select", TARGET, expect_failure=True)

    assert not db.exists(TARGET), "target created despite invalid config"
    assert_clean(db)


def test_unknown_filter_mode_is_a_config_error(db, dbt_run):
    db.load(make_rows(2))
    with scd_env(filter_mode="not_a_mode"):
        dbt_run("run", "--select", TARGET, expect_failure=True)

    assert not db.exists(TARGET)
    assert_clean(db)


def test_scd_key_absent_from_select_is_a_model_error(db, dbt_run):
    """Caught in step 3, after the temporary source relation exists, so this exercises the
    cleanup on that raise path."""
    db.load(make_rows(2))
    with scd_env(scd_key="ikke_en_kolonne"):
        dbt_run("run", "--select", TARGET, expect_failure=True)

    assert not db.exists(TARGET)
    assert_clean(db)


def test_existing_relation_without_scd_columns_is_rejected(db, dbt_run):
    """A plain table sitting on the target name must be refused rather than merged into, and the
    run must leave it untouched."""
    db.execute(
        f"create table {SCHEMA}.{TARGET} as "
        f"select 'x' as kode1, 'y' as kode2 from dual"
    )
    before = db.count()
    db.load(make_rows(2))

    with scd_env():
        dbt_run("run", "--select", TARGET, expect_failure=True)

    assert db.exists(TARGET), "pre-existing table was destroyed"
    assert db.count() == before, "pre-existing table was modified"
    assert_clean(db)


def test_renaming_a_configured_column_rejects_the_existing_target(db, dbt_run):
    """Switching USE_CUSTOM_NAMES against an existing target changes the expected column names,
    which is the other way step 4 validation trips."""
    db.load(make_rows(3))
    with scd_env(use_custom_names=False):
        dbt_run("run", "--select", TARGET)
    before = db.count()

    with scd_env(use_custom_names=True):
        dbt_run("run", "--select", TARGET, expect_failure=True)

    assert db.count() == before, "target was modified by a rejected run"
    assert_clean(db)


def test_unenabled_schema_change_is_rejected(db, dbt_run):
    """A source column that appears after the target was built is an append. With
    schema_changes_enabled empty it must raise, and leave the target untouched."""
    db.load(make_rows(3))
    with scd_env(exclude_columns="navn2"):
        dbt_run("run", "--select", TARGET)
    before = db.count()
    assert "navn2" not in columns_of(db)

    with scd_env(exclude_columns="", schema_changes=""):
        dbt_run("run", "--select", TARGET, expect_failure=True)

    assert "navn2" not in columns_of(db), "column added despite append not being enabled"
    assert db.count() == before, "target was modified by a rejected run"
    assert_clean(db)


def test_enabled_append_adds_the_new_column(db, dbt_run):
    db.load(make_rows(3))
    with scd_env(exclude_columns="navn2"):
        dbt_run("run", "--select", TARGET)
    assert "navn2" not in columns_of(db)

    db.execute(f"truncate table {SCHEMA}.testdata")
    db.load(make_rows(3, batch=1))
    with scd_env(exclude_columns="", schema_changes="append"):
        dbt_run("run", "--select", TARGET)

    assert "navn2" in columns_of(db), "append was enabled but the column was not added"
    assert_clean(db)


def test_enabled_remove_drops_the_missing_column(db, dbt_run):
    db.load(make_rows(3))
    with scd_env():
        dbt_run("run", "--select", TARGET)
    assert "navn2" in columns_of(db)

    db.execute(f"truncate table {SCHEMA}.testdata")
    db.load(make_rows(3, batch=1))
    with scd_env(exclude_columns="navn2", schema_changes="remove"):
        dbt_run("run", "--select", TARGET)

    assert "navn2" not in columns_of(db), "remove was enabled but the column was kept"
    assert_clean(db)


# ============================================================================================
# Property based tests
#
# These probe whether the sql built around scd_key and scd_hash survives arbitrary data rather
# than the tidy values the tests above use. They are slow, because every example is a full dbt
# run, so they carry the "property" marker and are excluded from the default run.
#
#   pytest -m property        only these
#   pytest                    everything else
# ============================================================================================

# the source column widths from APP_USER_INIT_SQL. varchar2 is declared with char semantics, so
# these are character counts and an astral character still counts as one.
COLUMN_WIDTHS = {"kode1": 12, "kode2": 6, "navn1": 40, "navn2": 20}

# The database is AL32UTF8, so anything is storable. This alphabet leans on the characters most
# likely to break sql generation or comparison: the '¿' that SCD__sha256_hash uses to separate
# columns, quote and escape characters, the like wildcards, whitespace that varchar2 comparison
# treats as significant, and a NUL.
WEIRD_CHARS = "¿'\"\\%_&|/*-\x00 \t\næøåÆØÅ😀"

# The separator is deliberately left out when generating scd_hash values. Two different tuples
# can join into the same string, which SCD__sha256_hash then cannot tell apart, and the run
# fails. That is a real defect, pinned deterministically by
# test_scd_hash_separator_collision_rejects_valid_data below, so leaving the character in here
# would only make the property test fail at random. Put it back once the hash is fixed.
HASH_CHARS = WEIRD_CHARS.replace("¿", "")

KEY_SHAPES = [["kode1"], ["kode1", "kode2"]]


def _col_text(name):
    """Values for one source column, mixing unrestricted unicode with a dense sample of the
    characters that are actually dangerous."""
    width = COLUMN_WIDTHS[name]
    return st.one_of(
        st.text(max_size=width),
        st.text(alphabet=st.sampled_from(WEIRD_CHARS), max_size=min(width, 8)),
    )


def _oracle_value(s):
    """Oracle stores the empty string as NULL, so '' and None are the same value once written.
    Comparisons made in python have to agree with that or they describe a difference the
    database cannot see."""
    return None if s == "" else s


def _distinct_source_keys(db, key_columns):
    """Key groups in the source, counted by the database so that its own notion of equality
    applies. select distinct keeps NULL as a group of its own."""
    cols = ", ".join(key_columns)
    return db.query(
        f"select count(*) as n from (select distinct {cols} from {SCHEMA}.{SOURCE})"
    )[0]["n"]


def _keys_with_several_valid_rows(db, key_columns):
    cols = ", ".join(key_columns)
    flag = names()["valid_flag"]
    return db.query(
        f"select count(*) as n from ("
        f"  select {cols} from {SCHEMA}.{TARGET} where {flag} = 1"
        f"  group by {cols} having count(*) > 1)"
    )[0]["n"]


@pytest.mark.property
@settings(
    max_examples=30,
    deadline=None,
    # the db fixture is function scoped and so runs once for the whole test rather than once per
    # example, which is why each example resets explicitly below
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    rows=st.lists(
        st.tuples(_col_text("kode1"), _col_text("kode2"), _col_text("navn1"), _col_text("navn2")),
        min_size=1,
        max_size=6,
    ),
    changed_at=st.lists(
        st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2030, 1, 1)),
        min_size=1,
        max_size=6,
    ),
    key_columns=st.sampled_from(KEY_SHAPES),
    scd_type=st.sampled_from(SCD_TYPES),
)
def test_property_scd_key_survives_arbitrary_data(db, dbt_run, rows, changed_at, key_columns, scd_type):
    """Arbitrary key values must not break the generated sql, and must still leave exactly one
    valid row per key group.

    The materialization validates its own output in step 8, so a run that succeeds has already
    ruled out overlapping and reversed intervals. What is checked here is that it succeeded at
    all, and that the key grouping matches what the database itself considers distinct."""
    db.reset()
    db.load([
        Row(
            pk=f"pk-{i}",
            kode1=kode1, kode2=kode2, navn1=navn1, navn2=navn2,
            tid1=changed_at[i % len(changed_at)],
            tid2=EPOCH - timedelta(days=365),
        )
        for i, (kode1, kode2, navn1, navn2) in enumerate(rows)
    ])

    with scd_env(scd_type=scd_type, scd_key=",".join(key_columns)):
        dbt_run("run", "--select", TARGET)

    expected = _distinct_source_keys(db, key_columns)
    flag = names()["valid_flag"]
    valid = db.query(f"select count(*) as n from {SCHEMA}.{TARGET} where {flag} = 1")[0]["n"]

    assert valid == expected, f"expected one valid row for each of {expected} key groups, found {valid}"
    assert _keys_with_several_valid_rows(db, key_columns) == 0, "a key group has several valid rows"
    assert_clean(db)


@pytest.mark.property
@settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    before=st.tuples(
        st.text(alphabet=st.sampled_from(HASH_CHARS), max_size=6),
        st.text(alphabet=st.sampled_from(HASH_CHARS), max_size=6),
    ),
    after=st.tuples(
        st.text(alphabet=st.sampled_from(HASH_CHARS), max_size=6),
        st.text(alphabet=st.sampled_from(HASH_CHARS), max_size=6),
    ),
)
def test_property_scd_hash_agrees_with_the_merge(db, dbt_run, before, after):
    """Two versions of one key whose scd_hash columns genuinely differ must produce two rows.

    This is where the two halves of the macro can disagree. The type 2 merge compares each hash
    column on its own with decode, while the repetitions check in SCD__validate_scd_target_rows
    compares one sha256 over the columns joined by a separator. A pair of values that differ per
    column but agree once joined would be versioned by the merge and then rejected by the
    validation, failing the run on data that is perfectly legitimate."""
    assume(tuple(map(_oracle_value, before)) != tuple(map(_oracle_value, after)))
    db.reset()

    key = "K1"

    db.load([Row(pk="pk-0", kode1=key, kode2="x", navn1=before[0], navn2=before[1],
                 tid1=EPOCH, tid2=EPOCH - timedelta(days=365))])
    with scd_env(scd_type=2, scd_key="kode1", scd_hash="navn1,navn2", filter_mode="changed_at"):
        dbt_run("run", "--select", TARGET)
    assert db.count() == 1

    db.execute(f"truncate table {SCHEMA}.{SOURCE}")
    db.load([Row(pk="pk-0", kode1=key, kode2="x", navn1=after[0], navn2=after[1],
                 tid1=EPOCH + timedelta(days=1), tid2=EPOCH - timedelta(days=365))])
    with scd_env(scd_type=2, scd_key="kode1", scd_hash="navn1,navn2", filter_mode="changed_at"):
        dbt_run("run", "--select", TARGET)

    assert db.count() == 2, (
        f"changing the hash columns from {before!r} to {after!r} did not produce a new version"
    )
    assert_clean(db)


@pytest.mark.xfail(
    strict=True,
    reason="SCD__sha256_hash joins the scd_hash columns with '¿' and hashes the result, so two "
           "tuples that differ per column but join to the same string are indistinguishable. The "
           "type 2 merge compares the columns separately and versions the row, then the "
           "repetitions check rejects it. Remove this marker once the hash separates the columns "
           "unambiguously.",
)
@pytest.mark.parametrize("before,after", [
    (("¿", ""), ("", "¿")),          # '¿' || '¿' || null   against  null || '¿' || '¿'
    (("a¿b", "c"), ("a", "b¿c")),    # 'a¿b¿c'              against  'a¿b¿c'
])
def test_scd_hash_separator_collision_rejects_valid_data(db, dbt_run, before, after):
    """Two versions whose scd_hash columns genuinely differ, but whose values joined by the
    separator are identical. The run fails with {'repetitions': 1} even though the data is
    legitimate and the merge was right to version it."""

    db.load([Row(pk="p", kode1="K1", kode2="x", navn1=before[0], navn2=before[1],
                 tid1=EPOCH, tid2=EPOCH - timedelta(days=365))])
    with scd_env(scd_type=2, scd_key="kode1", scd_hash="navn1,navn2", filter_mode="changed_at"):
        dbt_run("run", "--select", TARGET)

    db.execute(f"truncate table {SCHEMA}.{SOURCE}")
    db.load([Row(pk="p", kode1="K1", kode2="x", navn1=after[0], navn2=after[1],
                 tid1=EPOCH + timedelta(days=1), tid2=EPOCH - timedelta(days=365))])
    with scd_env(scd_type=2, scd_key="kode1", scd_hash="navn1,navn2", filter_mode="changed_at"):
        dbt_run("run", "--select", TARGET)

    assert db.count() == 2, (
        f"changing the hash columns from {before!r} to {after!r} did not produce a new version"
    )
