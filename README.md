# dbt-dvh-macros
DBT makroer utviklet i DVH til felles bruk.

## Installasjon
```yaml
# packages.yml
packages:
  - git: "https://github.com/navikt/dbt-dvh-macros.git"
    revision: bc50c6e9274b587dcb0c592fc335e140ac989af2
```
Ikke bruk main som revision ovenfor fordi DBT kloner alltid hele repoet.
Bruk kun release branchene (x.y.z) som bare har de relevante filene i seg,
og spesifiser commit-hash fra branchen.
