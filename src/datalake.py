"""
Connexion au data lake (stockage compatible S3).
Fonctions réutilisables par tous les scripts du projet.
"""

import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


def creer_client():
    """Ouvre la connexion au data lake."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        region_name="us-east-1",
    )


def creer_bucket_si_absent(client, bucket):
    """Crée le bucket s'il n'existe pas encore."""
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)


def objet_existe(client, bucket, cle):
    """Renvoie True si le fichier est déjà dans le bucket."""
    try:
        client.head_object(Bucket=bucket, Key=cle)
        return True
    except ClientError as erreur:
        if erreur.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def envoyer_fichier(client, chemin_local, bucket, cle):
    """Envoie un fichier local dans le bucket."""
    client.upload_file(str(chemin_local), bucket, cle)

def recuperer_fichier(client, bucket, cle, chemin_local):
    """Télécharge un fichier du bucket vers le PC."""
    client.download_file(bucket, cle, str(chemin_local))


def lister_cles(client, bucket, prefixe):
    """Renvoie la liste des fichiers dont le nom commence par le préfixe."""
    cles = []
    paginateur = client.get_paginator("list_objects_v2")
    for page in paginateur.paginate(Bucket=bucket, Prefix=prefixe):
        for objet in page.get("Contents", []):
            cles.append(objet["Key"])
    return cles


if __name__ == "__main__":
    # Test : connexion, puis liste des fichiers bronze
    bucket = os.getenv("S3_BUCKET")
    client = creer_client()
    creer_bucket_si_absent(client, bucket)
    print("Connexion OK.")
    print("Fichiers bronze :")
    for cle in lister_cles(client, bucket, "bronze/"):
        print("  -", cle)