select
    a.navn
    , a.besk
    , b.langbesk
from
    dual
    inner join
        {{source("dbtuser", "bredagg_testdata")}} a
        on 1 = 1
    inner join
        {{source("dbtuser", "bredagg_testdata2")}} b
        on 1 = 1
