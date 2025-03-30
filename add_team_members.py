from db_neo4j import Neo4jConnector

team_members = ["Pooranan", "Matias"]
film_id = "100"  # à adapter selon un film existant

neo4j = Neo4jConnector()
for member in team_members:
    neo4j.add_project_member_as_actor(member, film_id)

neo4j.close()
print("Membres du projet ajoutés comme acteurs.")
