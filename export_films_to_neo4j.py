from db_mongo import get_films_collection
from db_neo4j import Neo4jConnector

connector = Neo4jConnector()
collection = get_films_collection()

count = 0
for film in collection.find():
    try:
        # Champs nécessaires (correspondance entre MongoDB et Neo4j)
        cleaned_film = {
            "id": str(film.get("_id")),  # _id vers id
            "title": film.get("title"),
            "year": film.get("year"),
            "votes": int(film.get("Votes", 0)),
            "revenue": float(film.get("Revenue (Millions)") or 0),
            "rating": film.get("rating"),
            "director": film.get("Director"),
            "genre": film.get("genre")
        }

        # Vérifie qu'aucune valeur importante n'est manquante
        if None in cleaned_film.values() or "" in cleaned_film.values():
            print(f"⚠️ Film ignoré (champ manquant ou vide) : {film.get('title')}")
            continue

        connector.create_film_node(cleaned_film)
        count += 1

    except Exception as e:
        print(f"Erreur lors du traitement du film {film.get('title')} : {e}")

print(f"{count} films exportés vers Neo4j.")
connector.close()
