with source as (

    select * from {{ source('raw', 'ventes') }}

)

select
    id_mutation,
    date_mutation,
    cast(valeur_fonciere as numeric(14, 2))     as prix,
    cast(surface_reelle_bati as numeric(10, 2)) as surface_m2,
    cast(nombre_pieces_principales as integer)  as nb_pieces,
    cast(surface_terrain as numeric(12, 2))     as surface_terrain_m2,
    cast(prix_m2 as numeric(10, 2))             as prix_m2,
    type_local,
    code_commune,
    nom_commune,
    code_postal,
    code_departement,
    longitude,
    latitude
from source