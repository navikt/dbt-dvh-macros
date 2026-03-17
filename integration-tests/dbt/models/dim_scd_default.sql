select
    kode
    , navn
    , to_date('oppdatert', 'yyyymmdd') as oppdatert_tid_kilde
    , to_date('opprettet', 'yyyymmdd') as opprettet_tid_kilde
from
    {{ source(env_var('DBT_SCHEMA'), "scd_raadata") }}