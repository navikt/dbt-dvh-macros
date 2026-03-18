# integration-tests
Testrammeverket for makro pakken er under arbeid, men idéen er å teste alle jinja sql makroer i pakken mot testcontainere med oracle database installert i forløp av testing mot DVH, både ved utvikling og oppdatering av DBT versjon.
## Rammeverk
### DBT
DBT definerer selv to typer tester: [unit tests](https://docs.getdbt.com/docs/build/unit-tests?version=1.11), og [data tests](https://docs.getdbt.com/docs/build/data-tests?version=1.11). Sistnevnte deles videre inn i *singular* og *generic*. Disse er dessverre begrenset i scope (se lenker), men er uansett ment mer for å teste dataen i rader, og ikke makroene som generer flyten av radene.

Når det er sagt, så kan man "unit teste" en og en dbt makro innenfor dbt sitt test rammeverk ved å f.eks.:
- definere en dbt seed csv fil med rader X
- definere en dbt seed csv fil med forventet rader Y
- definere en dbt modell som selecter makro(X)
- definere en generisk dbt data test som selecter A minus B union B minus A
- sette modellen til å bruke denne testen ved å refere til seed med forventet rader Y

Denne strategien er blandt annet brukt av [dbt-utils](https://github.com/dbt-labs/dbt-utils/blob/main/integration_tests/models/sql/schema.yml) og fungerer bra med enkle makroer som skal fungere mot flere adaptere (database typer).

### Pytest
Pythons Pytest kan blandt annet brukes både til enhets-, integrasjons-, og ende-til-ende tester. Pakker som Hypothesis kan brukes til property-based (underkategori av fuzzy) testing sammen med pytest. Det gjør det mulig å håndtere tilstanden til databasen og tabellene, og eventuelle feil på en bedre måte.
