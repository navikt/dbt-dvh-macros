select
    alt_pk as pk_test
    , kode
    , kode2
    , navn
    , navn2
    , to_date('oppdatert', 'yyyymmdd') as endret
    , to_date('opprettet', 'yyyymmdd') as opprettet
from
    {{ source(env_var('DBT_SCHEMA'), "scd_raadata") }}