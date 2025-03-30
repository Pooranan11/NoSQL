from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

# Connexion à MongoDB
client = MongoClient(os.getenv("MONGODB_URI"))
db = client["entertainment"]  # Remplace "movies" par le nom réel de ta base
collection = db.get_collection("films")

# Nombre total de documents
total_docs = collection.count_documents({})

# Films valides : avec tous les champs requis pour Neo4j
total_films = collection.count_documents({
    "title": {"$exists": True, "$ne": ""},
    "year": {"$exists": True},
    "Revenue (Millions)": {"$exists": True, "$ne": ""},
    "rating": {"$exists": True, "$ne": ""},
    "Director": {"$exists": True, "$ne": ""},
    "Votes": {"$exists": True}
})

# Acteurs distincts
actors_set = set()
for doc in collection.find({"Actors": {"$exists": True, "$ne": ""}}, {"Actors": 1}):
    actors = doc["Actors"].split(",")
    for actor in actors:
        actors_set.add(actor.strip())
total_actors = len(actors_set)

# Réalisateurs distincts
directors_set = set()
for doc in collection.find({"Director": {"$exists": True, "$ne": ""}}, {"Director": 1}):
    directors_set.add(doc["Director"])
total_directors = len(directors_set)

# Relations acteur-film
total_relations = 0
for doc in collection.find({"Actors": {"$exists": True, "$ne": ""}}, {"Actors": 1}):
    total_relations += len(doc["Actors"].split(","))

# Résumé
print("📊 Résumé des données MongoDB")
print(f"📁 Total de documents : {total_docs}")
print(f"🎬 Films exportables : {total_films}")
print(f"🎭 Acteurs distincts : {total_actors}")
print(f"🎬 Réalisateurs distincts : {total_directors}")
print(f"🔗 Relations Acteur-Film : {total_relations}")
