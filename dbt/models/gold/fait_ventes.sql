with ventes as (

    select * from {{ ref('stg_ventes') }}

),

communes as (

    select code_commune from {{ ref('dim_commune') }}

),

dates as (

    select date_jour from {{ ref('dim_date') }}

)

select
    v.id_mutation         as id_vente,
    v.date_mutation       as date_vente,
    v.code_commune,
    v.type_local,
    v.prix,
    v.surface_m2,
    v.nb_pieces,
    v.surface_terrain_m2,
    v.prix_m2,
    v.longitude,
    v.latitude
from ventes v
inner join communes c on v.code_commune = c.code_commune
inner join dates d    on v.date_mutation = d.date_jour