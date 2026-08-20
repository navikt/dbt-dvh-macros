# dbt-dvh-macros
DBT makroer utviklet i DVH til felles bruk.

## Installasjon
```yaml
# packages.yml
packages:
  - git: "https://github.com/navikt/dbt-dvh-macros.git"
    revision: 6475118508edc78e91e0dd8ee3adf95491d52d89 # 1.0.5
```
Ikke bruk main som revision ovenfor fordi DBT kloner alltid hele repoet.
Bruk kun release branchene (x.y.z) som bare har de relevante filene i seg,
og spesifiser commit-hash fra branchen.

## Kode generert av GitHub Copilot
Dette repoet bruker GitHub Copilot til å generere kode.
