"""
Ingestion DVF télécharge les fichiers bruts depuis data.gouv
et les range dans la couche bronze.
"""

import logging
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# Configuration des messages affichés dans le terminal
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
        "bronze_dir": Path(os.getenv("BRONZE_DIR")),
    }


def construire_url(base_url, annee, departement):
    """Fabrique l'adresse du fichier à télécharger."""
    return f"{base_url}/{annee}/departements/{departement}.csv.gz"


def construire_chemin(bronze_dir, annee, departement):
    """Fabrique l'emplacement où ranger le fichier."""
    return bronze_dir / annee / f"{departement}.csv.gz"


def telecharger(url, destination):
    """Télécharge un fichier morceau par morceau."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    fichier_temp = destination.with_suffix(".tmp")

    with requests.get(url, stream=True, timeout=60) as reponse:
        reponse.raise_for_status()
        with open(fichier_temp, "wb") as f:
            for morceau in reponse.iter_content(chunk_size=1024 * 1024):
                f.write(morceau)

    fichier_temp.replace(destination)


def main():
    """Point d'entrée : boucle sur chaque année et département."""
    config = charger_config()
    nb_telecharges = 0
    nb_ignores = 0
    nb_erreurs = 0

    for annee in config["annees"]:
        for departement in config["departements"]:
            url = construire_url(config["base_url"], annee, departement)
            destination = construire_chemin(config["bronze_dir"], annee, departement)

            if destination.exists():
                logger.info("Déjà présent, ignoré : %s", destination)
                nb_ignores += 1
                continue

            try:
                logger.info("Téléchargement : %s", url)
                telecharger(url, destination)
                taille_mo = destination.stat().st_size / 1_000_000
                logger.info("OK : %s (%.1f Mo)", destination, taille_mo)
                nb_telecharges += 1
            except requests.RequestException as erreur:
                logger.error("Échec : %s -> %s", url, erreur)
                nb_erreurs += 1

    logger.info(
        "Terminé : %d téléchargés, %d ignorés, %d erreurs",
        nb_telecharges, nb_ignores, nb_erreurs,
    )


if __name__ == "__main__":
    main()