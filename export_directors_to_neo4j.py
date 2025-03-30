from db_mongo import get_director_film_relations
from db_neo4j import Neo4jConnector

relations = get_director_film_relations()
neo4j = Neo4jConnector()

nb_nodes = 0
nb_links = 0
done_directors = set()

for rel in relations:
    director = rel.get("director")
    film_id = rel.get("film_id")

    if not director or not film_id:
        print("Relation ignorée (champ manquant) :", rel)
        continue

    # Créer le noeud une seule fois
    if director not in done_directors:
        neo4j.create_director_node(director)
        done_directors.add(director)
        nb_nodes += 1

    # Créer la relation
    neo4j.create_director_film_relation(director, film_id)
    nb_links += 1

neo4j.close()
print(f"{nb_nodes} réalisateurs ajoutés")
print(f"{nb_links} relations A_REALISE créées")
