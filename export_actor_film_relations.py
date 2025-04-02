from db_mongo import get_actor_film_relations
from db_neo4j import Neo4jConnector

relations = get_actor_film_relations()
neo4j = Neo4jConnector()

nb_rels = 0
for rel in relations:
    try:
        neo4j.create_actor_film_relation(rel["actor"], rel["film_id"])
        nb_rels += 1
    except Exception as e:
        print("⚠️ Erreur relation :", rel, "->", e)

neo4j.close()
print(f"{nb_rels} relations acteur-film créées dans Neo4j.")
