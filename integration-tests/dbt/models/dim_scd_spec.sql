select
    alt_pk as pk_test
    , kode
    , kode2
    , navn
    , navn2
    , oppdatert as endret
    , opprettet as opprettet
from
    {{ source(env_var('DBT_SCHEMA'), "scd_raadata") }}