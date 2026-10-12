with ventes as (

    select * from {{ ref('stg_ventes') }}

)

select
    code_commune,
    max(nom_commune)      as nom_commune,
    max(code_departement) as code_departement
from ventes
group by code_commune