from pymongo import MongoClient
from neo4j import GraphDatabase
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# 🔗 MongoDB Atlas
mongodb_uri = "mongodb+srv://vytheswaranp:SbKrmkam4uEdWs57@cluster0.fi61b.mongodb.net/"
mongo_client = MongoClient(mongodb_uri)
mongo_db = mongo_client["test_db"]
print("✅ Connexion MongoDB réussie.")

# 🔗 Neo4j Aura
neo4j_uri = "neo4j+s://1a402009.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "UdtE0P1LOyVeM6t9EWg2DI58zMTZ75JIsMHh1DzwORg"

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
with driver.session() as session:
    session.run("RETURN 1")
print("✅ Connexion Neo4j réussie.")



# Charger les variables d'environnement
load_dotenv()

# Connexion à MongoDB
client = MongoClient(os.getenv("MONGODB_URI"))
db = client.get_database()
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
