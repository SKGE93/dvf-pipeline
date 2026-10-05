"""
Ingestion DVF : télécharge les fichiers bruts depuis data.gouv
et les dépose dans la couche bronze du data lake.
"""

import logging
import os
import tempfile
from pathlib import Path

import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from datalake import creer_bucket_si_absent, creer_client, envoyer_fichier, objet_existe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def charger_config():
    """Lit le fichier .env et renvoie les réglages."""
    load_dotenv()
    return {
        "base_url": os.getenv("DVF_BASE_URL"),
        "departements": [d.strip() for d in os.getenv("DEPARTEMENTS").split(",")],
        "annees": [a.strip() for a in os.getenv("ANNEES").split(",")],
        "bucket": os.getenv("S3_BUCKET"),
        "prefixe": os.getenv("BRONZE_PREFIX"),
    }


def construire_url(base_url, annee, departement):
    """Fabrique l'adresse du fichier à télécharger."""
    return f"{base_url}/{annee}/departements/{departement}.csv.gz"


def construire_cle(prefixe, annee, departement):
    """Fabrique le nom du fichier dans le bucket."""
    return f"{prefixe}/{annee}/{departement}.csv.gz"


def telecharger(url, destination):
    """Télécharge un fichier morceau par morceau."""
    with requests.get(url, stream=True, timeout=60) as reponse:
        reponse.raise_for_status()
        with open(destination, "wb") as f:
            for morceau in reponse.iter_content(chunk_size=1024 * 1024):
                f.write(morceau)


def main():
    """Point d'entrée : boucle sur chaque année et département."""
    config = charger_config()
    client = creer_client()
    creer_bucket_si_absent(client, config["bucket"])

    nb_envoyes = 0
    nb_ignores = 0
    nb_erreurs = 0

    with tempfile.TemporaryDirectory() as dossier_temp:
        for annee in config["annees"]:
            for departement in config["departements"]:
                url = construire_url(config["base_url"], annee, departement)
                cle = construire_cle(config["prefixe"], annee, departement)

                if objet_existe(client, config["bucket"], cle):
                    logger.info("Déjà dans le data lake, ignoré : %s", cle)
                    nb_ignores += 1
                    continue

                try:
                    chemin_local = Path(dossier_temp) / f"{annee}_{departement}.csv.gz"

                    logger.info("Téléchargement : %s", url)
                    telecharger(url, chemin_local)
                    taille_mo = chemin_local.stat().st_size / 1_000_000

                    logger.info("Envoi vers s3://%s/%s", config["bucket"], cle)
                    envoyer_fichier(client, chemin_local, config["bucket"], cle)

                    chemin_local.unlink()
                    logger.info("OK : %s (%.1f Mo)", cle, taille_mo)
                    nb_envoyes += 1
                except (requests.RequestException, ClientError) as erreur:
                    logger.error("Échec : %s -> %s", cle, erreur)
                    nb_erreurs += 1

    logger.info(
        "Terminé : %d envoyés, %d ignorés, %d erreurs",
        nb_envoyes, nb_ignores, nb_erreurs,
    )


if __name__ == "__main__":
    main()