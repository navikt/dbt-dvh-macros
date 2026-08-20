# dbt-dvh-macros
DBT makroer utviklet i DVH til felles bruk.

## Installasjon
```yaml
# packages.yml
packages:
  - git: "https://github.com/navikt/dbt-dvh-macros.git"
    revision: 928327c803bf693b20028238d8db37e27df61f87 # 1.0.7
```
Ikke bruk main som revision ovenfor fordi DBT kloner alltid hele repoet.
Bruk kun release branchene (x.y.z) som bare har de relevante filene i seg,
og spesifiser commit-hash fra branchen.

## Kode generert av GitHub Copilot
Dette repoet bruker GitHub Copilot til å generere kode.
