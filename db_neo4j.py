import os
from dotenv import load_dotenv
from neo4j import GraphDatabase


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

print("NEO4J_URI utilisé :", NEO4J_URI)


class Neo4jConnector:
    def __init__(self):
        try:
            self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            # Test de connexion rapide
            with self.driver.session() as session:
                session.run("RETURN 1")
            print("Connexion Neo4j réussie.")
        except Exception as e:
            print("Erreur de connexion Neo4j :", e)
            raise

    def close(self):
        self.driver.close()

    def create_film_node(self, film):
        """
        Crée un noeud Film dans Neo4j.
        film doit être un dictionnaire contenant les clés : id, title, year, votes, revenue, rating, director.
        """
        query = """
        MERGE (f:Film {id: $id})
        SET f.title = $title,
            f.year = $year,
            f.votes = $votes,
            f.revenue = $revenue,
            f.rating = $rating,
            f.director = $director,
            f.genre = $genre  
        """
        with self.driver.session() as session:
            session.run(query, **film)

    def find_top_actor(self):
        """
        Exemple de requête Cypher pour trouver l'acteur qui a joué dans le plus grand nombre de films.
        On suppose que les relations (:Actor)-[:A_JOUE_DANS]->(:Film) existent.
        """
        query = """
        MATCH (a:Actor)-[:A_JOUE_DANS]->(f:Film)
        RETURN a.name AS actor, count(f) AS filmsCount
        ORDER BY filmsCount DESC
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query)
            return result.single()
        
    def create_actor_node(self, actor_name):
        """
        Crée un noeud Actor si non existant.
        """
        query = "MERGE (:Actor {name: $name})"
        with self.driver.session() as session:
            session.run(query, name=actor_name)

    def create_actor_film_relation(self, actor_name, film_id):
        """
        Crée une relation (:Actor)-[:A_JOUE_DANS]->(:Film)
        """
        query = """
        MATCH (a:Actor {name: $actor_name})
        MATCH (f:Film {id: $film_id})
        MERGE (a)-[:A_JOUE_DANS]->(f)
        """
        with self.driver.session() as session:
            session.run(query, actor_name=actor_name, film_id=film_id)
    
    def add_project_member_as_actor(self, member_name, film_id):
        """
        Ajoute un membre de l'équipe en tant qu'acteur fictif lié à un film.
        """
        query = """
        MERGE (a:Actor {name: $name})
        WITH a
        MATCH (f:Film {id: $film_id})
        MERGE (a)-[:A_JOUE_DANS]->(f)
        """
        with self.driver.session() as session:
            session.run(query, name=member_name, film_id=film_id)

    def create_director_node(self, name):
        """
        Crée un nœud Realisateur si non existant.
        """
        query = "MERGE (:Realisateur {name: $name})"
        with self.driver.session() as session:
            session.run(query, name=name)

    def create_director_film_relation(self, director_name, film_id):
        """
        Crée une relation (:Realisateur)-[:A_REALISE]->(:Film)
        """
        query = """
        MATCH (r:Realisateur {name: $name})
        MATCH (f:Film {id: $film_id})
        MERGE (r)-[:A_REALISE]->(f)
        """
        with self.driver.session() as session:
            session.run(query, name=director_name, film_id=film_id)


    def get_top_actor_alt(self):
        """
        Variante : retourne l'acteur qui a joué dans le plus de films.
        """
        query = """
        MATCH (a:Actor)-[:A_JOUE_DANS]->(f:Film)
        RETURN a.name AS Acteur, count(f) AS NombreFilms
        ORDER BY NombreFilms DESC
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query)
            record = result.single()
            if record:
                return {
                    "acteur": record["Acteur"],
                    "nb_films": record["NombreFilms"]
                }
            return None

    def get_coactors_of(self, actor_name):
        query = """
        MATCH (a:Actor {name: $name})-[:A_JOUE_DANS]->(f:Film)<-[:A_JOUE_DANS]-(co:Actor)
        WHERE co.name <> $name
        RETURN DISTINCT co.name AS CoActeur
        """
        with self.driver.session() as session:
            result = session.run(query, name=actor_name)
            return [record["CoActeur"] for record in result]

    def get_actor_highest_total_revenue(self):
        query = """
        MATCH (a:Actor)-[:A_JOUE_DANS]->(f:Film)
        WHERE f.revenue IS NOT NULL
        WITH a, sum(f.revenue) AS totalRevenue
        RETURN a.name AS actor, totalRevenue
        ORDER BY totalRevenue DESC
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query).single()
            return result.data() if result else None

    def get_avg_votes(self):
        query = """
        MATCH (f:Film)
        WHERE f.votes IS NOT NULL
        RETURN avg(f.votes) AS MoyenneVotes
        """
        with self.driver.session() as session:
            result = session.run(query).single()
            return result["MoyenneVotes"] if result else None

    def get_votes_per_film(self, limit=10):
        query = """
        MATCH (f:Film)
        WHERE f.votes IS NOT NULL
        RETURN f.title AS title, f.votes AS votes
        ORDER BY f.votes DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            results = session.run(query, limit=limit)
            return [{"title": r["title"], "votes": r["votes"]} for r in results]

    def get_most_common_genre(self):
        query = """
        MATCH (f:Film)
        WHERE f.genre IS NOT NULL
        UNWIND split(f.genre, ",") AS g
        RETURN trim(g) AS genre, count(*) AS occurrences
        ORDER BY occurrences DESC
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query).single()
            return result if result else None

    def get_films_of_my_coactors(self, member_name):
        query = """
        MATCH (me:Actor {name: $member_name})-[:A_JOUE_DANS]->(:Film)<-[:A_JOUE_DANS]-(co:Actor)
        WITH DISTINCT co
        MATCH (co)-[:A_JOUE_DANS]->(f:Film)
        RETURN DISTINCT f.title AS film, f.year AS year
        ORDER BY f.year DESC
        """
        with self.driver.session() as session:
            result = session.run(query, member_name=member_name)
            return [{"title": record["film"], "year": record["year"]} for record in result]

    def get_director_with_most_actors(self):
        query = """
        MATCH (r:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE_DANS]-(a:Actor)
        RETURN r.name AS realisateur, count(DISTINCT a) AS nb_acteurs
        ORDER BY nb_acteurs DESC
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query).single()
            return {
                "realisateur": result["realisateur"],
                "nb_acteurs": result["nb_acteurs"]
            } if result else None

    def get_most_connected_films(self, limit=10):
        query = """
        MATCH (f1:Film)<-[:A_JOUE_DANS]-(a:Actor)-[:A_JOUE_DANS]->(f2:Film)
        WHERE f1 <> f2
        WITH f1, count(DISTINCT a) AS sharedActors
        RETURN f1.title AS film, sharedActors
        ORDER BY sharedActors DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            results = session.run(query, limit=limit)
            return [record["film"] for record in results]

    def get_actors_with_most_directors(self, limit=5):
        query = """
        MATCH (a:Actor)-[:A_JOUE_DANS]->(f:Film)
        WHERE f.director IS NOT NULL
        WITH a, count(DISTINCT f.director) AS NbRealisateurs
        RETURN a.name AS Acteur, NbRealisateurs
        ORDER BY NbRealisateurs DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            results = session.run(query, limit=limit)
            return [{"acteur": r["Acteur"], "nb_realisateurs": r["NbRealisateurs"]} for r in results]

    def recommend_film_to_actor(self, actor_name):
        query = """
        MATCH (a:Actor {name: $name})-[:A_JOUE_DANS]->(f:Film)
        UNWIND split(f.genre, ",") AS g
        WITH a, collect(DISTINCT trim(g)) AS GenresPref
        MATCH (rec:Film)
        WHERE ANY(genre IN GenresPref WHERE rec.genre CONTAINS genre)
        AND NOT (a)-[:A_JOUE_DANS]->(rec)
        RETURN rec.title AS FilmRecommande, rec.genre AS Genres, rec.rating AS Rating
        ORDER BY rec.rating DESC
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query, name=actor_name).single()
            if result:
                return {
                    "titre": result["FilmRecommande"],
                    "genres": result["Genres"],
                    "note": result["Rating"]
                }
            return None

    def create_directors_influence_relations(self):
        query = """
        MATCH (d1:Realisateur)<-[:A_REALISE]-(f1:Film),
            (d2:Realisateur)<-[:A_REALISE]-(f2:Film)
        WHERE d1 <> d2
        AND ANY(genre IN split(f1.genre, ",")
                WHERE trim(genre) IN split(f2.genre, ","))
        MERGE (d1)-[:INFLUENCE_PAR]->(d2)
        """
        with self.driver.session() as session:
            session.run(query)

    def get_shortest_path_between_actors(self, actor1, actor2):
        query = """
        MATCH (a1:Actor {name: $actor1}), (a2:Actor {name: $actor2})
        MATCH p = allShortestPaths((a1)-[:A_JOUE_DANS|A_REALISE*]-(a2))
        RETURN p
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query, actor1=actor1, actor2=actor2).single()
            return result["p"] if result else None

    def get_films_with_common_genres_and_different_directors(self):
        query = """
        MATCH (f1:Film), (f2:Film)
        WHERE f1 <> f2
        AND f1.director <> f2.director
        AND ANY(genre IN split(f1.genre, ",") 
                WHERE trim(genre) IN split(f2.genre, ","))
        RETURN DISTINCT f1.title AS Film1, f2.title AS Film2
        LIMIT 1000
        """
        with self.driver.session() as session:
            results = session.run(query)
            return [record.data() for record in results]


    def recommend_films_based_on_actor_preferences(self, actor_name):
        query = """
        MATCH (a:Actor {name: $name})-[:A_JOUE_DANS]->(f:Film)
        UNWIND split(f.genre, ",") AS g
        WITH a, collect(DISTINCT trim(g)) AS GenresPref
        MATCH (rec:Film)
        WHERE ANY(genre IN GenresPref WHERE rec.genre CONTAINS genre)
        AND NOT (a)-[:A_JOUE_DANS]->(rec)
        AND rec.rating IS NOT NULL
        RETURN rec.title AS Titre, rec.genre AS Genres, rec.rating AS Note
        ORDER BY rec.rating DESC
        LIMIT 5
        """
        with self.driver.session() as session:
            result = session.run(query, name=actor_name)
            return [record.data() for record in result]



    def create_director_competition_relations(self):
        query = """
        MATCH (d1:Director)<-[:REALISE]-(f1:Film),
            (d2:Director)<-[:REALISE]-(f2:Film)
        WHERE d1 <> d2 AND f1.year = f2.year
        AND ANY(genre IN split(f1.genre, ",") WHERE genre IN split(f2.genre, ","))
        MERGE (d1)-[:CONCURRENCE]->(d2)
        """
        with self.driver.session() as session:
            session.run(query)


    def get_director_actor_collaborations(self):
        query = """
        MATCH (d:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE_DANS]-(a:Actor)
        WHERE f.revenue IS NOT NULL
        WITH d, a, COUNT(f) AS nb, 
            AVG(TOFLOAT(f.revenue)) AS avg_rev
        WHERE nb >= 2
        RETURN d.name AS realisateur, 
            a.name AS acteur, 
            nb AS nb_films, 
            avg_rev AS revenu_moyen
        ORDER BY nb DESC, avg_rev DESC
        LIMIT 20
        """
        with self.driver.session() as session:
            results = session.run(query)
            return [record.data() for record in results]

##################################################
if __name__ == "__main__":
    # Test rapide : création d'un noeud film fictif
    connector = Neo4jConnector()
    sample_film = {
        "id": "film1",
        "title": "Exemple Film",
        "year": 2021,
        "votes": 1000,
        "revenue": 50000000,
        "rating": 8.5,
        "director": "Un Réalisateur"
    }
    connector.create_film_node(sample_film)
    top_actor = connector.find_top_actor()
    print("Acteur le plus présent :", top_actor)
    connector.close()



