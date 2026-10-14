"""
Tests unitaires des fonctions de nommage et des règles de nettoyage.
"""

import pandas as pd

from ingestion import construire_cle, construire_url
from transformation import construire_cle_silver, nettoyer

# ---------- Nommage des fichiers ----------

def test_construire_url():
    url = construire_url("https://exemple.fr/csv", "2024", "93")
    assert url == "https://exemple.fr/csv/2024/departements/93.csv.gz"


def test_construire_cle():
    assert construire_cle("bronze", "2024", "93") == "bronze/2024/93.csv.gz"


def test_construire_cle_silver():
    cle = construire_cle_silver("bronze/2024/93.csv.gz", "bronze", "silver")
    assert cle == "silver/2024/93.parquet"


# ---------- Règles de nettoyage ----------

def ligne(**changements):
    """Fabrique une vente valide, qu'on peut modifier pour chaque test."""
    vente = {
        "id_mutation": "2024-1",
        "date_mutation": "2024-03-15",
        "nature_mutation": "Vente",
        "valeur_fonciere": 250000.0,
        "adresse_numero": "10",
        "adresse_nom_voie": "RUE DE PARIS",
        "code_postal": "93100",
        "code_commune": "93048",
        "nom_commune": "Montreuil",
        "code_departement": "93",
        "type_local": "Appartement",
        "surface_reelle_bati": 50.0,
        "nombre_pieces_principales": 3.0,
        "surface_terrain": None,
        "longitude": 2.44,
        "latitude": 48.86,
    }
    vente.update(changements)
    return vente


def test_garde_une_vente_valide_et_calcule_le_prix_m2():
    df = pd.DataFrame([ligne()])
    resultat = nettoyer(df)
    assert len(resultat) == 1
    assert resultat.loc[0, "prix_m2"] == 5000


def test_retire_ce_qui_n_est_pas_une_vente():
    df = pd.DataFrame([ligne(nature_mutation="Echange")])
    assert len(nettoyer(df)) == 0


def test_retire_les_dependances():
    df = pd.DataFrame([ligne(type_local="Dépendance")])
    assert len(nettoyer(df)) == 0


def test_retire_une_vente_de_deux_logements():
    df = pd.DataFrame([
        ligne(id_mutation="2024-2", surface_reelle_bati=50.0),
        ligne(id_mutation="2024-2", surface_reelle_bati=30.0),
    ])
    assert len(nettoyer(df)) == 0


def test_garde_une_seule_ligne_pour_un_meme_logement():
    df = pd.DataFrame([
        ligne(id_mutation="2024-3", surface_terrain=100.0),
        ligne(id_mutation="2024-3", surface_terrain=200.0),
    ])
    assert len(nettoyer(df)) == 1


def test_retire_un_prix_m2_absurde():
    df = pd.DataFrame([ligne(valeur_fonciere=1000.0)])
    assert len(nettoyer(df)) == 0


def test_retire_une_surface_nulle():
    df = pd.DataFrame([ligne(surface_reelle_bati=0.0)])
    assert len(nettoyer(df)) == 0   