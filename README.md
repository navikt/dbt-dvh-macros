# dbt-dvh-macros
DBT makroer utviklet i DVH til felles bruk.

## Installasjon
```yaml
# packages.yml
packages:
  - git: "https://github.com/navikt/dbt-dvh-macros.git"
    revision: 1.0.0
```
Ikke bruk main som revision ovenfor fordi DBT kloner alltid hele repoet.
Bruk kun release branchene (x.y.z) som bare har de relevante filene i seg.
