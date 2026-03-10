# macros
## scd
Implementasjon av DBT materialiseringstype `SCD` som støtter Slowly Changing Dimension Type 0, 1, og 2 inkrementelle SQL transformasjoner/modeller for dbt-oracle adapteret.

| SCD    | Description     |
|--------|-----------------|
| Type 0 | retain original |
| Type 1 | overwrite       |
| Type 2 | add new row     |

Se [Wikipedia](https://en.wikipedia.org/wiki/Slowly_changing_dimension) for en grei introduksjon.