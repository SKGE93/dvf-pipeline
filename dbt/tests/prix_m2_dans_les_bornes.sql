-- Le test échoue si cette requête renvoie au moins une ligne
select
    id_vente,
    prix_m2
from {{ ref('fait_ventes') }}
where prix_m2 < 500
   or prix_m2 > 20000