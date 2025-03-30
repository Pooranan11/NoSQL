import networkx as nx
from pyvis.network import Network
import streamlit.components.v1 as components
from db_neo4j import Neo4jConnector
import streamlit as st


def draw_top_actor_graph():
    connector = Neo4jConnector()
    query = """
    MATCH (a:Actor)-[:A_JOUE_DANS]->(f:Film)
    WITH a, COUNT(f) AS nb
    ORDER BY nb DESC
    LIMIT 1
    MATCH (a)-[:A_JOUE_DANS]->(f:Film)
    RETURN a.name AS actor, f.title AS film
    """
    G = nx.Graph()

    with connector.driver.session() as session:
        results = session.run(query)
        for record in results:
            G.add_node(record["actor"], label=record["actor"], color="lightgreen")
            G.add_node(record["film"], label=record["film"], color="lightblue")
            G.add_edge(record["actor"], record["film"])

    net = Network(height="600px", width="100%", bgcolor="#222", font_color="white")
    net.from_nx(G)
    net.save_graph("top_actor_graph.html")

    components.html(open("top_actor_graph.html", "r", encoding="utf-8").read(), height=600)
    connector.close()

def afficher_graphe_acteurs_films():
    connector = Neo4jConnector()
    query = """
    MATCH (a:Actor)-[:A_JOUE_DANS]->(f:Film)
    RETURN a.name AS acteur, f.title AS film
    LIMIT 100
    """
    G = nx.Graph()

    with connector.driver.session() as session:
        results = session.run(query)
        for record in results:
            G.add_node(record["acteur"], label=record["acteur"], color='lightblue')
            G.add_node(record["film"], label=record["film"], color='orange')
            G.add_edge(record["acteur"], record["film"])
    
    net = Network(notebook=False)
    net.from_nx(G)
    net.save_graph("graph.html")

    components.html(open("graph.html", "r", encoding="utf-8").read(), height=600)
    connector.close()

def draw_coactors_graph(actor_name="Anne Hathaway"):
    connector = Neo4jConnector()
    query = """
    MATCH (a:Actor {name: $name})-[:A_JOUE_DANS]->(f:Film)<-[:A_JOUE_DANS]-(co:Actor)
    WHERE co.name <> $name
    RETURN DISTINCT a.name AS main_actor, co.name AS co_actor
    """
    G = nx.Graph()

    with connector.driver.session() as session:
        results = session.run(query, name=actor_name)
        for record in results:
            main = record["main_actor"]
            co = record["co_actor"]
            G.add_node(main, label=main, color="gold")
            G.add_node(co, label=co, color="lightblue")
            G.add_edge(main, co)

    net = Network(height="600px", width="100%", bgcolor="#222", font_color="white")
    net.from_nx(G)
    net.save_graph("coactors_graph.html")
    components.html(open("coactors_graph.html", "r", encoding="utf-8").read(), height=600)
    connector.close()

def draw_actor_highest_revenue_graph():
    connector = Neo4jConnector()
    query = """
    MATCH (a:Actor)-[:A_JOUE_DANS]->(f:Film)
    WHERE f.revenue IS NOT NULL
    WITH a, sum(f.revenue) AS totalRevenue
    ORDER BY totalRevenue DESC
    LIMIT 1
    MATCH (a)-[:A_JOUE_DANS]->(f:Film)
    RETURN a.name AS actor, f.title AS film, f.revenue AS revenue
    """
    G = nx.Graph()

    with connector.driver.session() as session:
        results = session.run(query)
        for record in results:
            actor = record["actor"]
            film = record["film"]
            revenue = record["revenue"]
            G.add_node(actor, label=actor, color="gold")
            G.add_node(film, label=f"{film}\n💰{round(revenue, 1)}M", color="lightblue")
            G.add_edge(actor, film)

    net = Network(height="600px", width="100%", bgcolor="#222", font_color="white")
    net.from_nx(G)
    net.save_graph("actor_revenue_graph.html")
    components.html(open("actor_revenue_graph.html", "r", encoding="utf-8").read(), height=600)
    connector.close()

def draw_votes_avg_graph(data, moyenne):
    import matplotlib.pyplot as plt
    import os

    fig, ax = plt.subplots(figsize=(10, 5))

    titres = [r["title"] for r in data]
    votes = [r["votes"] for r in data]

    ax.bar(titres, votes, color="skyblue")
    ax.axhline(moyenne, color="red", linestyle="--", label=f"Moyenne : {round(moyenne, 1)}")
    ax.set_title("Votes par film avec moyenne")
    ax.set_ylabel("Votes")
    ax.set_xlabel("Films")
    ax.legend()
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    file_path = "avg_votes_graph.png"
    fig.savefig(file_path)
    plt.close(fig)

    return file_path

def draw_most_common_genre_graph(genre, occurrences):
    G = nx.Graph()
    G.add_node("Genre", label="🎭 Genre", color="gold")
    G.add_node(genre, label=f"{genre}\n🎬 {occurrences} films", color="lightblue")
    G.add_edge("Genre", genre)

    net = Network(height="600px", width="100%", bgcolor="#111", font_color="white")
    net.from_nx(G)
    net.save_graph("most_common_genre.html")
    return "most_common_genre.html"


def draw_coactors_films_graph(member_name):
    connector = Neo4jConnector()
    query = """
    MATCH (me:Actor {name: $name})-[:A_JOUE_DANS]->(:Film)<-[:A_JOUE_DANS]-(co:Actor)
    WITH DISTINCT co
    MATCH (co)-[:A_JOUE_DANS]->(f:Film)
    RETURN DISTINCT co.name AS coactor, f.title AS film
    """
    G = nx.Graph()

    with connector.driver.session() as session:
        results = session.run(query, name=member_name)
        for record in results:
            co = record["coactor"]
            film = record["film"]
            G.add_node(member_name, label=member_name, color="gold")  # central node
            G.add_node(co, label=co, color="lightgreen")
            G.add_node(film, label=film, color="lightblue")
            G.add_edge(member_name, co)
            G.add_edge(co, film)

    net = Network(height="600px", width="100%", bgcolor="#222", font_color="white")
    net.from_nx(G)
    file_path = "coactors_films_graph.html"
    net.save_graph(file_path)
    connector.close()

    components.html(open(file_path, "r", encoding="utf-8").read(), height=600)


def draw_director_with_actors_graph(director_name):
    connector = Neo4jConnector()
    query = """
    MATCH (r:Realisateur {name: $name})-[:A_REALISE]->(f:Film)<-[:A_JOUE_DANS]-(a:Actor)
    RETURN DISTINCT r.name AS realisateur, f.title AS film, a.name AS actor
    """
    G = nx.Graph()

    with connector.driver.session() as session:
        results = session.run(query, name=director_name)
        for record in results:
            realisateur = record["realisateur"]
            film = record["film"]
            actor = record["actor"]

            G.add_node(realisateur, label=realisateur, color="gold")
            G.add_node(film, label=film, color="orange")
            G.add_node(actor, label=actor, color="lightblue")

            G.add_edge(realisateur, film)
            G.add_edge(film, actor)

    net = Network(height="600px", width="100%", bgcolor="#111", font_color="white")
    net.from_nx(G)
    net.save_graph("director_actors_graph.html")
    connector.close()

    components.html(open("director_actors_graph.html", "r", encoding="utf-8").read(), height=600)

def draw_shared_films_graph_only(film_titles):
    connector = Neo4jConnector()
    query = """
    MATCH (f1:Film)<-[:A_JOUE_DANS]-(a:Actor)-[:A_JOUE_DANS]->(f2:Film)
    WHERE f1.title IN $titles AND f2.title IN $titles AND f1.title < f2.title
    WITH f1.title AS film1, f2.title AS film2, count(DISTINCT a) AS nb
    RETURN film1, film2, nb
    ORDER BY nb DESC
    """

    G = nx.Graph()

    with connector.driver.session() as session:
        results = session.run(query, titles=film_titles)
        for record in results:
            f1 = record["film1"]
            f2 = record["film2"]
            nb = record["nb"]

            G.add_node(f1, label=f1, color="orange", size=25)
            G.add_node(f2, label=f2, color="orange", size=25)
            G.add_edge(f1, f2, title=f"{nb} acteur(s) partagés", value=nb)

    net = Network(height="650px", width="100%", bgcolor="#111", font_color="white")
    net.from_nx(G)

    # ✅ JSON CORRECT (pas de "var", tout en double quotes)
    net.set_options("""
    {
      "edges": {
        "color": "#ffa500",
        "scaling": { "min": 1, "max": 10 },
        "font": { "size": 14, "color": "white" }
      },
      "nodes": {
        "shape": "dot",
        "scaling": { "min": 15, "max": 30 },
        "font": { "size": 18, "color": "white" }
      },
      "physics": {
        "barnesHut": {
          "gravitationalConstant": -30000,
          "springLength": 200
        }
      }
    }
    """)

    net.save_graph("film_only_shared_graph.html")
    connector.close()

    components.html(open("film_only_shared_graph.html", "r", encoding="utf-8").read(), height=650)

def draw_film_recommendation_graph(actor_name):
    import networkx as nx
    from pyvis.network import Network
    import streamlit.components.v1 as components
    from db_neo4j import Neo4jConnector

    connector = Neo4jConnector()

    query = """
    MATCH (a:Actor {name: $name})-[:A_JOUE_DANS]->(f:Film)
    UNWIND split(f.genre, ",") AS g
    WITH a, collect(DISTINCT trim(g)) AS GenresPref
    MATCH (rec:Film)
    WHERE ANY(genre IN GenresPref WHERE rec.genre CONTAINS genre)
      AND NOT (a)-[:A_JOUE_DANS]->(rec)
    RETURN a.name AS acteur, GenresPref, rec.title AS film, rec.genre AS genres, rec.rating AS note
    ORDER BY note DESC
    LIMIT 1
    """

    G = nx.Graph()

    with connector.driver.session() as session:
        result = session.run(query, name=actor_name).single()

        if result:
            acteur = result["acteur"]
            genres = result["GenresPref"]
            film = result["film"]
            film_genres = result["genres"]
            note = result["note"]

            G.add_node(acteur, label=acteur, color="gold")

            for g in genres:
                G.add_node(g, label=g, color="lightblue")
                G.add_edge(acteur, g)

            G.add_node(film, label=f"{film}\n⭐ {note}", color="orange")
            for g in genres:
                if g in film_genres:
                    G.add_edge(g, film)

    connector.close()

    net = Network(height="650px", width="100%", bgcolor="#111", font_color="white")
    net.from_nx(G)
    net.set_options("""
    {
      "nodes": {
        "shape": "dot",
        "scaling": { "min": 10, "max": 30 },
        "font": { "size": 18, "color": "white" }
      },
      "edges": {
        "color": "#ffa500",
        "font": { "size": 12, "color": "white" }
      },
      "physics": {
        "barnesHut": { "gravitationalConstant": -30000, "springLength": 200 }
      }
    }
    """)
    net.save_graph("film_reco_graph.html")
    components.html(open("film_reco_graph.html", "r", encoding="utf-8").read(), height=650)

def draw_shortest_path_between_actors(actor1, actor2):
    import networkx as nx
    from pyvis.network import Network
    import streamlit.components.v1 as components
    import streamlit as st
    from db_neo4j import Neo4jConnector

    connector = Neo4jConnector()
    path = connector.get_shortest_path_between_actors(actor1, actor2)

    if not path:
        st.warning(f"Aucun chemin trouvé entre **{actor1}** et **{actor2}**.")
        connector.close()
        return

    G = nx.Graph()

    for node in path.nodes:
        label = node.get("name") or node.get("title")
        color = (
            "gold" if label in [actor1, actor2]
            else "lightblue" if "name" in node
            else "orange"
        )
        G.add_node(label, label=label, color=color)

    for rel in path.relationships:
        start = rel.start_node.get("name") or rel.start_node.get("title")
        end = rel.end_node.get("name") or rel.end_node.get("title")
        G.add_edge(start, end)

    net = Network(height="600px", width="100%", bgcolor="#111", font_color="white")
    net.from_nx(G)
    net.set_options("""
    {
      "nodes": {
        "shape": "dot",
        "font": { "size": 16, "color": "white" },
        "scaling": { "min": 10, "max": 30 }
      },
      "edges": {
        "color": "#ffa500",
        "font": { "size": 12, "color": "white" }
      },
      "physics": {
        "barnesHut": { "gravitationalConstant": -30000, "springLength": 200 }
      }
    }
    """)
    net.save_graph("shortest_path_graph.html")
    connector.close()

    components.html(open("shortest_path_graph.html", "r", encoding="utf-8").read(), height=600)

def draw_actor_communities_graph():
    import networkx as nx
    from pyvis.network import Network
    import streamlit.components.v1 as components
    import streamlit as st
    from db_neo4j import Neo4jConnector

    connector = Neo4jConnector()

    query = """
    MATCH (a:Actor)-[:A_JOUE]-(co:Actor)
    WHERE a.community IS NOT NULL AND co.community IS NOT NULL
    RETURN a.name AS actor1, co.name AS actor2, a.community AS community
    LIMIT 500
    """


    G = nx.Graph()
    color_map = {}

    with connector.driver.session() as session:
        results = session.run(query)
        for record in results:
            a1 = record["actor1"]
            a2 = record["actor2"]
            comm = record["community"]

            # Ajout avec couleur différente par communauté
            if comm not in color_map:
                color_map[comm] = f"hsl({(comm * 47) % 360}, 100%, 60%)"

            G.add_node(a1, label=a1, color=color_map[comm])
            G.add_node(a2, label=a2, color=color_map[comm])
            G.add_edge(a1, a2)

    net = Network(height="700px", width="100%", bgcolor="#111", font_color="white")
    net.from_nx(G)
    net.set_options("""
    {
      "nodes": {
        "shape": "dot",
        "font": { "size": 14, "color": "white" },
        "scaling": { "min": 10, "max": 20 }
      },
      "edges": {
        "color": "gray"
      },
      "physics": {
        "barnesHut": {
          "gravitationalConstant": -30000,
          "springLength": 150
        }
      }
    }
    """)
    net.save_graph("actor_communities.html")
    connector.close()

    components.html(open("actor_communities.html", "r", encoding="utf-8").read(), height=700)


def draw_actors_with_common_movies(min_common=2):
    connector = Neo4jConnector()
    query = """
    MATCH (a1:Actor)-[:A_JOUE_DANS]->(f:Film)<-[:A_JOUE_DANS]-(a2:Actor)
    WHERE a1 <> a2 AND a1.name < a2.name
    WITH a1, a2, COUNT(f) AS common_movies, COLLECT(f.title) AS films
    WHERE common_movies >= $min_common
    RETURN a1.name AS actor1, a2.name AS actor2, common_movies AS nb, films
    """
    G = nx.Graph()

    with connector.driver.session() as session:
        results = session.run(query, min_common=min_common)
        for record in results:
            a1 = record["actor1"]
            a2 = record["actor2"]
            films = ", ".join(record["films"])
            G.add_node(a1, label=a1, color="lightblue")
            G.add_node(a2, label=a2, color="lightblue")
            G.add_edge(a1, a2, title=f"{record['nb']} films\n🎬 {films}", value=record['nb'])

    net = Network(height="650px", width="100%", bgcolor="#111", font_color="white")
    net.from_nx(G)
    net.save_graph("common_movies_actors_graph.html")
    connector.close()

    components.html(open("common_movies_actors_graph.html", "r", encoding="utf-8").read(), height=650)
