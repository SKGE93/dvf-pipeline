"""
Transformation DVF : bronze -> silver.
Nettoie les fichiers bruts et les dépose en Parquet dans la couche silver.
"""

import logging
import os
import tempfile
from pathlib import Path

import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from datalake import (
    creer_client,
    envoyer_fichier,
    lister_cles,
    objet_existe,
    recuperer_fichier,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Règles métier
TYPES_GARDES = ["Maison", "Appartement"]
PRIX_M2_MIN = 500
PRIX_M2_MAX = 20000

# Colonnes à lire en texte pour ne pas perdre les zéros devant
TYPES_TEXTE = {
    "id_mutation": str,
    "code_postal": str,
    "code_commune": str,
    "code_departement": str,
    "adresse_numero": str,
    "id_parcelle": str,
}

# Colonnes gardées en silver
COLONNES_SILVER = [
    "id_mutation",
    "date_mutation",
    "valeur_fonciere",
    "adresse_numero",
    "adresse_nom_voie",
    "code_postal",
    "code_commune",
    "nom_commune",
    "code_departement",
    "type_local",
    "surface_reelle_bati",
    "nombre_pieces_principales",
    "surface_terrain",
    "longitude",
    "latitude",
    "prix_m2",
]


def charger_config():
    """Lit le fichier .env et renvoie les réglages."""
    load_dotenv()
    return {
        "bucket": os.getenv("S3_BUCKET"),
        "prefixe_bronze": os.getenv("BRONZE_PREFIX"),
        "prefixe_silver": os.getenv("SILVER_PREFIX"),
    }


def construire_cle_silver(cle_bronze, prefixe_bronze, prefixe_silver):
    """bronze/2024/93.csv.gz -> silver/2024/93.parquet"""
    cle = cle_bronze.replace(f"{prefixe_bronze}/", f"{prefixe_silver}/", 1)
    return cle.replace(".csv.gz", ".parquet")


def nettoyer(df):
    """Applique les règles de nettoyage et renvoie un tableau propre."""
    nb_depart = len(df)

    # 1. Garder uniquement les ventes classiques
    df = df[df["nature_mutation"] == "Vente"]

    # 2. Garder uniquement les maisons et appartements
    df = df[df["type_local"].isin(TYPES_GARDES)]

    # 3. Un même logement peut apparaître sur plusieurs lignes (terrain, parcelles)
    #    On garde une seule ligne par logement
    df = df.drop_duplicates(
        subset=["id_mutation", "type_local", "surface_reelle_bati", "nombre_pieces_principales"]
    )

    # 4. Garder les ventes qui concernent un seul logement
    nb_logements = df.groupby("id_mutation")["id_mutation"].transform("size")
    df = df[nb_logements == 1]

    # 5. Retirer les prix et surfaces vides ou nuls
    df = df.dropna(subset=["valeur_fonciere", "surface_reelle_bati"])
    df = df[(df["valeur_fonciere"] > 0) & (df["surface_reelle_bati"] > 0)]

    # 6. Convertir la date et calculer le prix au m²
    df = df.assign(
        date_mutation=pd.to_datetime(df["date_mutation"]),
        prix_m2=(df["valeur_fonciere"] / df["surface_reelle_bati"]).round(2),
    )

    # 7. Retirer les prix au m² absurdes
    df = df[df["prix_m2"].between(PRIX_M2_MIN, PRIX_M2_MAX)]

    df = df[COLONNES_SILVER].reset_index(drop=True)
    logger.info("Lignes : %d au départ -> %d après nettoyage", nb_depart, len(df))
    return df


def main():
    """Point d'entrée : transforme chaque fichier bronze pas encore traité."""
    config = charger_config()
    client = creer_client()

    cles_bronze = [
        cle
        for cle in lister_cles(client, config["bucket"], f"{config['prefixe_bronze']}/")
        if cle.endswith(".csv.gz")
    ]
    logger.info("%d fichiers bronze trouvés", len(cles_bronze))

    nb_traites = 0
    nb_ignores = 0
    nb_erreurs = 0

    with tempfile.TemporaryDirectory() as dossier_temp:
        chemin_csv = Path(dossier_temp) / "bronze.csv.gz"
        chemin_parquet = Path(dossier_temp) / "silver.parquet"

        for cle_bronze in cles_bronze:
            cle_silver = construire_cle_silver(
                cle_bronze, config["prefixe_bronze"], config["prefixe_silver"]
            )

            if objet_existe(client, config["bucket"], cle_silver):
                logger.info("Déjà transformé, ignoré : %s", cle_silver)
                nb_ignores += 1
                continue

            try:
                logger.info("Lecture : %s", cle_bronze)
                recuperer_fichier(client, config["bucket"], cle_bronze, chemin_csv)
                df = pd.read_csv(chemin_csv, dtype=TYPES_TEXTE, low_memory=False)

                df_propre = nettoyer(df)
                logger.info("Prix médian au m² : %.0f €", df_propre["prix_m2"].median())

                df_propre.to_parquet(chemin_parquet, index=False)
                envoyer_fichier(client, chemin_parquet, config["bucket"], cle_silver)
                logger.info("OK : %s", cle_silver)
                nb_traites += 1
            except (ClientError, pd.errors.ParserError, KeyError) as erreur:
                logger.error("Échec : %s -> %s", cle_bronze, erreur)
                nb_erreurs += 1

    logger.info(
        "Terminé : %d transformés, %d ignorés, %d erreurs",
        nb_traites, nb_ignores, nb_erreurs,
    )


if __name__ == "__main__":
    main()