select
    {{ BREDAGG__ephemeral_star('bredagg_ephemeral_star_src', 't', prefix='test_', except=['ikkemed']) }}
from 
    {{ ref('bredagg_ephemeral_star_src') }} t
    
    