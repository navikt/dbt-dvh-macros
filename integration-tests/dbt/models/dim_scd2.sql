select
    kode
    , navn
    , oppdatert as oppdatert_tid_kilde
    , opprettet as opprettet_tid_kilde
from
    {{ source(env_var('DBT_SCHEMA'), "scd_raadata") }}