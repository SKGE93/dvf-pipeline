with bornes as (

    select
        min(date_mutation) as date_min,
        max(date_mutation) as date_max
    from {{ ref('stg_ventes') }}

),

jours as (

    select generate_series(date_min, date_max, interval '1 day')::date as date_jour
    from bornes

)

select
    date_jour,
    extract(year from date_jour)::int     as annee,
    extract(quarter from date_jour)::int  as trimestre,
    extract(month from date_jour)::int    as mois,
    to_char(date_jour, 'YYYY-MM')         as annee_mois,
    extract(isodow from date_jour)::int   as jour_semaine,
    extract(isodow from date_jour) in (6, 7) as est_weekend
from jours