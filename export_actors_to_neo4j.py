from db_mongo import get_distinct_actors
from db_neo4j import Neo4jConnector

actors = get_distinct_actors()
neo4j = Neo4jConnector()

for actor in actors:
    neo4j.create_actor_node(actor)

print(f"✅ {len(actors)} acteurs exportés vers Neo4j.")
neo4j.close()
