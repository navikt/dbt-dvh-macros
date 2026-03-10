# macros
## scd
Implementasjon av DBT materialiseringstype `SCD` som støtter Slowly Changing Dimension Type 0, 1, og 2 inkrementelle SQL transformasjoner/modeller for dbt-oracle adapteret.


| SCD    | Action          |
|--------|-----------------|
| Type 0 | Retain original |
| Type 1 | Overwrite       |
| Type 2 | Add new row     |

Ytterligere informasjon:
- [Wikipedia](https://en.wikipedia.org/wiki/Slowly_changing_dimension) for en grei introduksjon.
- [Data Warehouse Toolkit](https://www.oreilly.com/library/view/the-data-warehouse/9781118530801/) bok av Ralph Kimball