"""
Chargement DVF : silver -> PostgreSQL (schéma raw).
Chaque fichier silver est rechargé en entier, sans doublon.
"""

import logging
import os
import tempfile
from pathlib import Path

import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from datalake import creer_client, lister_cles, recuperer_fichier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

SQL_TABLE = """
CREATE TABLE IF NOT EXISTS raw.ventes (
    id_mutation               TEXT,
    date_mutation             DATE,
    valeur_fonciere           NUMERIC,
    adresse_numero            TEXT,
    adresse_nom_voie          TEXT,
    code_postal               TEXT,
    code_commune              TEXT,
    nom_commune               TEXT,
    code_departement          TEXT,
    type_local                TEXT,
    surface_reelle_bati       NUMERIC,
    nombre_pieces_principales NUMERIC,
    surface_terrain           NUMERIC,
    longitude                 DOUBLE PRECISION,
    latitude                  DOUBLE PRECISION,
    prix_m2                   NUMERIC,
    fichier_source            TEXT,
    date_chargement           TIMESTAMP DEFAULT now()
)
"""


def charger_config():
    """Lit le fichier .env et renvoie les réglages."""
    load_dotenv()
    return {
        "bucket": os.getenv("S3_BUCKET"),
        "prefixe_silver": os.getenv("SILVER_PREFIX"),
        "url_base": (
            f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        ),
    }


def preparer_tables(moteur):
    """Crée le schéma et la table s'ils n'existent pas."""
    with moteur.begin() as connexion:
        connexion.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        connexion.execute(text(SQL_TABLE))


def charger_fichier(moteur, df, cle_silver):
    """Remplace les lignes de ce fichier dans la table, en une transaction."""
    df = df.assign(
        date_mutation=df["date_mutation"].dt.date,
        fichier_source=cle_silver,
    )
    with moteur.begin() as connexion:
        connexion.execute(
            text("DELETE FROM raw.ventes WHERE fichier_source = :source"),
            {"source": cle_silver},
        )
        df.to_sql(
            "ventes",
            connexion,
            schema="raw",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )


def compter_lignes(moteur):
    """Renvoie le nombre total de lignes dans la table."""
    with moteur.connect() as connexion:
        return connexion.execute(text("SELECT COUNT(*) FROM raw.ventes")).scalar()


def main():
    """Point d'entrée : charge chaque fichier silver dans PostgreSQL."""
    config = charger_config()
    client = creer_client()
    moteur = create_engine(config["url_base"])

    preparer_tables(moteur)

    cles_silver = [
        cle
        for cle in lister_cles(client, config["bucket"], f"{config['prefixe_silver']}/")
        if cle.endswith(".parquet")
    ]
    logger.info("%d fichiers silver trouvés", len(cles_silver))

    nb_charges = 0
    nb_erreurs = 0

    with tempfile.TemporaryDirectory() as dossier_temp:
        chemin_parquet = Path(dossier_temp) / "silver.parquet"

        for cle_silver in cles_silver:
            try:
                recuperer_fichier(client, config["bucket"], cle_silver, chemin_parquet)
                df = pd.read_parquet(chemin_parquet)
                charger_fichier(moteur, df, cle_silver)
                logger.info("OK : %s (%d lignes)", cle_silver, len(df))
                nb_charges += 1
            except (ClientError, SQLAlchemyError) as erreur:
                logger.error("Échec : %s -> %s", cle_silver, erreur)
                nb_erreurs += 1

    logger.info(
        "Terminé : %d chargés, %d erreurs. Total en base : %d lignes",
        nb_charges, nb_erreurs, compter_lignes(moteur),
    )

    if nb_erreurs > 0:
        raise SystemExit(1)

if __name__ == "__main__":
    main()