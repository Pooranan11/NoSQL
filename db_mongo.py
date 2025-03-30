from pymongo import MongoClient
import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

MONGODB_URI = os.getenv("MONGODB_URI")
print("MONGODB_URI utilisé (db_mongo.py) :", MONGODB_URI)


def get_mongo_client():
    """Crée et retourne un client MongoDB."""
    try:
        print("MONGODB_URI utilisé :", MONGODB_URI)
        client = MongoClient(MONGODB_URI)
        client.list_database_names()
        print("Connexion MongoDB réussie.")
        return client
    except Exception as e:
        print("Erreur de connexion MongoDB :", e)
        raise
