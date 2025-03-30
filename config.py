import os
from dotenv import load_dotenv

load_dotenv()  # Charge les variables depuis .env

# MongoDB Atlas
MONGODB_URI = os.getenv("mongodb+srv://vytheswaranp:SbKrmkam4uEdWs57@cluster0.fi61b.mongodb.net/")  # Exemple : "mongodb+srv://votreUser:VotreMotDePasse@cluster0.xxx.mongodb.net/test_db"

# Neo4j Aura
NEO4J_URI = os.getenv("neo4j+s://1a402009.databases.neo4j.io")      # Exemple : "neo4j+s://1a402009.databases.neo4j.io"
NEO4J_USER = os.getenv("neo4j")    # Exemple : "neo4j"
NEO4J_PASSWORD = os.getenv("UdtE0P1LOyVeM6t9EWg2DI58zMTZ75JIsMHh1DzwORg")  # Mot de passe Neo4j
