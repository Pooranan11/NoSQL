from pymongo import MongoClient
import os
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

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

def get_films_collection(db_name="entertainment", collection_name="films"):
    """Retourne la collection films depuis la base de données MongoDB."""
    client = get_mongo_client()
    db = client[db_name]
    collection = db[collection_name]
    return collection

def get_year_with_most_films():
    """
    Exemple de requête : trouver l'année où le plus grand nombre de films a été produit.
    Nécessite que chaque document ait un champ 'year'.
    """
    collection = get_films_collection()
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    result = list(collection.aggregate(pipeline))
    return result[0] if result else None

def count_films_after_1999():
    """
    Compte le nombre total de films produits après 1999.
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {"year": {"$gt": 1999}}},
        {"$count": "total_films_apres_1999"}
    ]
    result = list(collection.aggregate(pipeline))
    return result[0]["total_films_apres_1999"] if result else 0

def get_avg_votes_for_2007():
    """
    Calcule la moyenne des votes pour les films sortis en 2007.
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {"year": 2007}},
        {"$group": {"_id": None, "avg_votes": {"$avg": "$Votes"}}}
    ]
    result = list(collection.aggregate(pipeline))
    return result[0]["avg_votes"] if result else 0

def count_films_by_year():
    """
    Retourne un DataFrame du nombre de films par année
    ET un histogramme à partir du pipeline MongoDB.
    """
    collection = get_films_collection()
    
    # Pipeline MongoDB
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    results = list(collection.aggregate(pipeline))
    
    # Conversion en DataFrame
    df = pd.DataFrame(results)
    df = df.rename(columns={"_id": "Year", "count": "Count"})
    
    # Création du graphique
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x="Year", y="Count", data=df, color="skyblue", ax=ax)
    ax.set_xlabel("Année")
    ax.set_ylabel("Nombre de films")
    ax.set_title("Nombre de films par année")
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    return df, fig

def get_distinct_genres():
    """
    Retourne la liste des genres distincts dans la base.
    """
    collection = get_films_collection()
    pipeline = [
        {"$project": {"genre": {"$split": ["$genre", ","]}}},
        {"$unwind": "$genre"},
        {"$group": {"_id": "$genre"}}
    ]
    return [genre["_id"].strip() for genre in collection.aggregate(pipeline)]


def get_film_with_highest_revenue():
    """
    Retourne le film avec le revenu le plus élevé (hors valeurs vides).
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {"Revenue (Millions)": {"$ne": ""}}},
        {"$sort": {"Revenue (Millions)": -1}},
        {"$limit": 1}
    ]
    result = list(collection.aggregate(pipeline))
    return result[0] if result else None

def get_directors_with_more_than_3_films():
    """
    Retourne la liste des réalisateurs ayant réalisé plus de 5 films.
    """
    collection = get_films_collection()
    pipeline = [
        {"$group": {"_id": "$Director", "number_of_films": {"$sum": 1}}},
        {"$match": {"number_of_films": {"$gt": 3}}},
        {"$sort": {"number_of_films": -1}}  # Optionnel : tri par nombre décroissant
    ]
    return list(collection.aggregate(pipeline))

def get_directors_with_more_than_2_films_split():
    """
    Prend en compte les réalisateurs multiples séparés par des virgules.
    """
    collection = get_films_collection()
    pipeline = [
        {"$project": {"Director": {"$split": ["$Director", ","]}}},
        {"$unwind": "$Director"},
        {"$group": {"_id": {"$trim": {"input": "$Director"}}, "number_of_films": {"$sum": 1}}},
        {"$match": {"number_of_films": {"$gt": 2}}},
        {"$sort": {"number_of_films": -1}}
    ]
    return list(collection.aggregate(pipeline))

def get_genre_with_highest_avg_revenue():
    """
    Retourne le genre avec le revenu moyen le plus élevé.
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {"Revenue (Millions)": {"$ne": ""}}},
        {
            "$project": {
                "genre": {"$split": ["$genre", ","]},
                "revenue": {"$toDouble": "$Revenue (Millions)"}
            }
        },
        {"$unwind": "$genre"},
        {"$group": {"_id": {"$trim": {"input": "$genre"}}, "avg_revenue": {"$avg": "$revenue"}}},
        {"$sort": {"avg_revenue": -1}},
        {"$limit": 1}
    ]
    result = list(collection.aggregate(pipeline))
    return result[0] if result else None

def get_top_3_by_decade_rating():
    """
    Top 3 films par décennie selon le rating (note IMDb).
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {"rating": {"$ne": "unrated"}}},
        {
            "$addFields": {
                "decade": {
                    "$subtract": [
                        {"$toInt": "$year"},
                        {"$mod": [{"$toInt": "$year"}, 10]}
                    ]
                }
            }
        },
        {"$sort": {"decade": 1, "rating": -1}},
        {
            "$group": {
                "_id": "$decade",
                "top_movies": {
                    "$push": {
                        "title": "$title",
                        "rating": "$rating"
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "top_movies": {"$slice": ["$top_movies", 3]}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

def get_top_3_by_decade_metascore():
    """
    Top 3 films par décennie selon le Metascore.
    """
    collection = get_films_collection()
    pipeline = [
        {
            "$match": {
                "Metascore": {"$ne": ""}
            }
        },
        {
            "$addFields": {
                "decade": {
                    "$subtract": [
                        {"$toInt": "$year"},
                        {"$mod": [{"$toInt": "$year"}, 10]}
                    ]
                },
                "MetascoreInt": {"$toInt": "$Metascore"}
            }
        },
        {
            "$sort": {
                "decade": 1,
                "MetascoreInt": -1
            }
        },
        {
            "$group": {
                "_id": "$decade",
                "top_movies": {
                    "$push": {
                        "title": "$title",
                        "Metascore": "$MetascoreInt"
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "top_movies": {"$slice": ["$top_movies", 3]}
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline))


def get_top_3_full_films_by_decade():
    """
    Retourne les 3 meilleurs films complets (tous champs) par décennie selon le rating.
    """
    collection = get_films_collection()
    pipeline = [
        {
            "$addFields": {
                "decade": {
                    "$subtract": ["$year", {"$mod": ["$year", 10]}]
                }
            }
        },
        {"$sort": {"decade": 1, "rating": -1}},
        {
            "$group": {
                "_id": "$decade",
                "films": {"$push": "$$ROOT"}
            }
        },
        {
            "$project": {
                "decade": "$_id",
                "topFilms": {"$slice": ["$films", 3]},
                "_id": 0
            }
        }
    ]
    return list(collection.aggregate(pipeline))


if __name__ == "__main__":
    # Test rapide de la fonction
    best_year = get_year_with_most_films()
    print("Année avec le plus de films :", best_year)
    total = count_films_after_1999()
    print("Nombre de films après 1999 :", total)

def get_longest_film_by_genre():
    """
    Retourne le film le plus long pour chaque genre.
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {"Runtime (Minutes)": {"$ne": ""}}},
        {
            "$project": {
                "genre": {"$split": ["$genre", ","]},
                "title": 1,
                "runtime": {"$toInt": "$Runtime (Minutes)"}
            }
        },
        {"$unwind": "$genre"},
        {"$sort": {"runtime": -1}},
        {
            "$group": {
                "_id": {"$trim": {"input": "$genre"}},
                "longest_film": {
                    "$first": {
                        "title": "$title",
                        "Runtime": "$runtime"
                    }
                }
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def create_high_score_high_revenue_view():
    client = get_mongo_client()
    db = client["entertainment"]

    view_name = "vue_films_80_50"
    source_collection = "films"
    pipeline = [
        {
            "$match": {
                "Metascore": {"$gt": 80},
                "Revenue (Millions)": {"$gt": 50}
            }
        }
    ]

    try:
        db.create_collection(view_name, viewOn=source_collection, pipeline=pipeline)
        print("Vue créée avec succès.")
    except Exception as e:
        if "already exists" in str(e):
            print("La vue existe déjà.")
        else:
            print("Erreur :", e)

def get_films_from_view(limit=10):
    """
    Récupère les films depuis la vue 'vue_films_80_50'
    """
    client = get_mongo_client()
    db = client["entertainment"]
    view = db["vue_films_80_50"]
    return list(view.find().limit(limit))

def get_runtime_and_revenue():
    """
    Extrait les couples (durée, revenu) pour analyse statistique de corrélation.
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {
            "Runtime (Minutes)": {"$ne": ""},
            "Revenue (Millions)": {"$ne": ""}
        }},
        {"$project": {
            "runtime": {"$toInt": "$Runtime (Minutes)"},
            "revenue": {"$toDouble": "$Revenue (Millions)"}
        }}
    ]
    return list(collection.aggregate(pipeline))

def get_avg_runtime_by_decade():
    """
    Calcule la durée moyenne des films par décennie.
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {"Runtime (Minutes)": {"$ne": ""}, "year": {"$ne": None}}},
        {
            "$addFields": {
                "yearInt": {"$toInt": "$year"},
                "runtimeInt": {"$toInt": "$Runtime (Minutes)"},
                "decade": {
                    "$subtract": [
                        {"$toInt": "$year"},
                        {"$mod": [{"$toInt": "$year"}, 10]}
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$decade",
                "avg_runtime": {"$avg": "$runtimeInt"}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

##################################################################################
def get_films_for_neo4j():
    collection = get_films_collection()
    pipeline = [
        {
            "$match": {
                "Revenue (Millions)": {"$ne": ""}
            }
        },
        {
            "$project": {
                "id": "$_id",
                "title": 1,
                "year": 1,
                "votes": "$Votes",
                "revenue": {"$toDouble": "$Revenue (Millions)"},
                "rating": None,
                "director": "$Director"
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def get_distinct_actors():
    """
    Retourne une liste d'acteurs distincts depuis MongoDB.
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {"Actors": {"$ne": ""}}},
        {"$project": {"actors": {"$split": ["$Actors", ", "]}}},
        {"$unwind": "$actors"},
        {"$group": {"_id": "$actors"}}
    ]
    return [actor["_id"].strip() for actor in collection.aggregate(pipeline)]

def get_actor_film_relations():
    """
    Récupère les relations entre acteurs et films.
    Retourne une liste de dicts : {actor, film_id}
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {"Actors": {"$ne": ""}}},
        {"$project": {
            "film_id": "$_id",
            "actors": {"$split": ["$Actors", ", "]}
        }},
        {"$unwind": "$actors"},
        {"$project": {
            "film_id": 1,
            "actor": {"$trim": {"input": "$actors"}}
        }}
    ]
    return list(collection.aggregate(pipeline))

def get_director_film_relations():
    """
    Récupère les relations réalisateur-film depuis MongoDB.
    Retourne une liste de dicts : {director, film_id}
    """
    collection = get_films_collection()
    pipeline = [
        {"$match": {"Director": {"$ne": ""}}},
        {"$project": {
            "film_id": "$_id",
            "director": "$Director"
        }}
    ]
    results = list(collection.aggregate(pipeline))

    # Juste pour debug, affiche un exemple
    if results:
        print("Exemple de relation réalisateur-film :", results[0])

    return results