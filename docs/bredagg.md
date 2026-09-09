## `BREDAGG`

### ephemeral_star
Muliggjør å ha en one-liner for å hente kolonner fra ephemeral tabeller. Man kan tilpasse blant annet prefix og droppe navngitte kolonner.

Eksempel kode:

    {{ ephemeral_star(model_name='dim_person_felter', relation_alias='t3', prefix='SKYLDNER_', except=["fk_person1","gyldig_fra_dato", "gyldig_til_dato" ]) }}

### sync_multi_source_comments
Valg man kan legge til i config, som henter databasekommentarer på identiske kolonner i Oracle fra tabellene man ønsker. Den siste listede får "siste ord", i de tilfellene hvor man har tabeller med identiske kolonner (lastet_dato for eks). Man kan overskrive felter man ønsker i yml-fil.

Eksempel kode:

    config(
        materialized='table',
        post_hook="{{ sync_multi_source_comments([ ['kode_verk', 'dim_tid'], ['kode_verk', 'dim_geografi']]) }}"
    )