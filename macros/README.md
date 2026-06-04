# DVH Makroer
Makroene er inndelt i mapper etter bruksområde.
## `scd`
Implementasjon av DBT materialiseringstype `scd` som støtter Slowly Changing Dimension Type 0, 1, og 2 inkrementelle SQL transformasjoner/modeller for dbt-oracle adapteret.

Dette løses hovedsaklig ved å sortere innkommende rader, slå opp mot eksisterende tabell, og så eksekvere MERGE på Primary Key kolonne.

Navngivning og metadata kolonner følger typisk DVH bruk.


| SCD    | Action          |
|--------|-----------------|
| Type 0 | Retain original |
| Type 1 | Overwrite       |
| Type 2 | Add new row     |

Ytterligere informasjon:
- [Wikipedia](https://en.wikipedia.org/wiki/Slowly_changing_dimension) for en grei introduksjon.
- [Data Warehouse Toolkit](https://www.oreilly.com/library/view/the-data-warehouse/9781118530801/) bok av Ralph Kimball

### Eksempel modell
```yaml
models:
    - name: dim_kodeverk
      description: SCD-1 dimensjon for grønnsaker
      config:
        materialized: scd
        scd_type: 1
        scd_key: kode
        scd_hash: [navn, kildesystem]
        filter_mode: changed_at
```
### Materialisering Flowchart
```mermaid
graph TD
    A[Model SQL Query] --> B[Create Empty Temp Table\nget precise datatypes]
    CFG[Model Properties\nscd_type, scd_key, scd_hash\nchanged_at, filter_mode, ...] --> V1

    B --> V1[Validate Config\n& Source Columns]
    V1 -->|errors| FAIL1[raise_compiler_error]

    V1 --> SC[Handle Schema Changes\nappend / remove / expand / morph]
    SC -->|errors| FAIL2[raise_compiler_error]
    SC -->|target exists| TGT[(Target Table)]
    SC -->|full refresh or first run| CREATE[Create Target Table\nwith generated columns\npk, valid_from/to/flag, loaded_at]
    CREATE --> TGT

    V1 --> INS[Insert into Temp Table\nwith filter]
    A --> INS

    subgraph Filtering & Deduplication
        INS --> FM{filter_mode}
        FM -->|changed_at| F1[source.changed_at >\nglobal max target.changed_at]
        FM -->|changed_at_per_scd_key| F2[no target row exists\nwith same key and\nchanged_at >= source.changed_at]
        FM -->|scd_key| F3[key does not\nexist in target]
        FM -->|ignore - full refresh| F4[1=1]
        F1 & F2 & F3 & F4 --> DEDUP{scd_hash\nspecified?}
        DEDUP -->|no| RN[rn_stable_sort = 1\ndedup on scd_key + changed_at]
        DEDUP -->|yes| PASS[pass through\nduplicates handled\ndownstream by IS_REPETITION]
    end

    RN --> TMP[(Temp Source Table)]
    PASS --> TMP

    subgraph Merge into Target
        TMP --> JOIN[Left join temp source\nto target on scd_key\nand valid_flag = 1]
        JOIN --> REP{scd_hash\nspecified?}
        REP -->|yes| REPCHECK[Compute IS_REPETITION\nvia lag on hash columns\nseeded from active target row]
        REP -->|no| NOCHECK[no repetition check]
        REPCHECK --> FILT[Filter out repetitions\nexcept first new row per key\nnot yet in target]
        NOCHECK --> MERGE
        FILT --> MERGE[MERGE into Target\nupsert valid_to + valid_flag\nfor changed rows\ninsert new rows]
    end

    MERGE --> TGT

    subgraph Post-Merge Validation
        TGT --> CHK1[valid_from > valid_to\nreversed intervals]
        TGT --> CHK2[overlapping intervals\nper scd_key]
        TGT --> CHK3[multiple valid_flag = 1\nper scd_key]
        TGT --> CHK4[gaps in history\nper scd_key]
        TGT --> CHK5[repetitions in target\nsame hash as previous row]
        CHK1 & CHK2 & CHK3 & CHK4 & CHK5 -->|violations found| FAIL3[raise_compiler_error]
        CHK1 & CHK2 & CHK3 & CHK4 & CHK5 -->|ok| DONE[Done]
    end
```
## `utils` (under arbeid)