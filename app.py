import streamlit as st
from db_mongo import (
    get_top_3_by_decade_rating,
    get_year_with_most_films,
    get_films_collection,
    count_films_after_1999,
    get_avg_votes_for_2007,
    count_films_by_year,
    get_distinct_genres,
    get_film_with_highest_revenue,
    get_directors_with_more_than_3_films,
    get_directors_with_more_than_2_films_split,
    get_genre_with_highest_avg_revenue,
    get_top_3_by_decade_metascore,
    get_top_3_full_films_by_decade,
    get_longest_film_by_genre,
    create_high_score_high_revenue_view,
    get_films_from_view,
    get_runtime_and_revenue,
    get_avg_runtime_by_decade
)
from db_neo4j import Neo4jConnector
import os
from dotenv import load_dotenv
import networkx as nx
from pyvis.network import Network
import streamlit.components.v1 as components
from graph_helpers import (
    draw_actor_communities_graph,
    draw_actor_highest_revenue_graph,
    draw_actors_with_common_movies,
    draw_coactors_films_graph,
    draw_director_with_actors_graph,
    draw_film_recommendation_graph,
    draw_most_common_genre_graph,
    draw_shared_films_graph_only,
    draw_shortest_path_between_actors,
    draw_top_actor_graph,
    afficher_graphe_acteurs_films,
    draw_coactors_graph,
    draw_votes_avg_graph
)
import pandas as pd

# Chargement du .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Juste pour debug
print("MONGODB_URI utilisé :", os.getenv("MONGODB_URI"))

st.title("Application NoSQL : MongoDB & Neo4j")

##########################################
# Section PyMongo - Requêtes MongoDB
##########################################

st.header("MongoDB - Statistiques Films")
if st.button("1 - Afficher l'année avec le plus de films"):
    result = get_year_with_most_films()
    if result:
        st.write(f"L'année {result['_id']} a produit {result['count']} films.")
    else:
        st.write("Aucun résultat trouvé.")

st.header("MongoDB - Films produits après 1999")
if st.button("2 - Compter les films après 1999"):
    total = count_films_after_1999()
    st.success(f"Nombre total de films produits après 1999 : {total}")

st.header("MongoDB - Moyenne des votes en 2007")
if st.button("3 - Afficher la moyenne des votes (2007)"):
    avg_votes = get_avg_votes_for_2007()
    st.info(f"Moyenne des votes pour les films de 2007 : {round(avg_votes, 2)}")

st.header("MongoDB - Statistiques avancées")
if st.button("4 - Nombre de films par année"):
    data = count_films_by_year()
    st.write("Nombre de films par année :")
    st.dataframe(data)

# Genres distincts
st.header("MongoDB - Genres de films disponibles dans la BDD.")
if st.button("5 - Genres distincts disponibles"):
    genres = get_distinct_genres()
    st.write(f"{len(genres)} genres trouvés :")
    st.write(", ".join(genres))

# Film avec le plus gros revenu
st.header("MongoDB - Le film qui a g´en´er´e le plus de revenu")
if st.button("6 - Afficher le film au plus haut revenu"):
    film = get_film_with_highest_revenue()
    if film:
        st.success(f"**{film['title']}** ({film['year']}) – {film['Revenue (Millions)']} M$")
    else:
        st.warning("Aucun film trouvé avec un revenu valide.")

st.header("MongoDB - Réalisateurs prolifiques")
if st.button("7 - Afficher les réalisateurs avec plus de 3 films"):
    directors = get_directors_with_more_than_3_films()
    if directors:
        st.write("Réalisateurs ayant dirigé plus de 3 films :")
        st.dataframe(directors)
    else:
        st.warning("Aucun réalisateur avec plus de 3 films trouvé.")

if st.button("7bis - Réalisateurs avec plus de 2 films"):
    data = get_directors_with_more_than_2_films_split()
    st.dataframe(data)

st.header("MongoDB - Genre le plus rentable")
if st.button("8 - Genre avec revenu moyen le plus élevé"):
    genre_info = get_genre_with_highest_avg_revenue()
    if genre_info:
        genre = genre_info["_id"]
        avg = round(genre_info["avg_revenue"], 2)
        st.success(f"Le genre **{genre}** a un revenu moyen de **{avg} M$**.")
    else:
        st.warning("Aucun genre avec revenu valide trouvé.")

st.header("MongoDB - Top 3 par décennie : Rating & Metascore")
if st.button("9ter - Top 3 par décennie (Rating)"):
    results = get_top_3_by_decade_rating()
    for entry in results:
        st.subheader(f"Décennie {entry['_id']}s (Rating)")
        for movie in entry["top_movies"]:
            st.markdown(f"- **{movie.get('title', 'Inconnu')}** — {movie.get('rating', 'N/A')}")

if st.button("9bis - Top 3 par décennie (Metascore)"):
    results = get_top_3_by_decade_metascore() #à modif
    for entry in results:
        st.subheader(f"Décennie {entry['_id']}s (Metascore)")
        for movie in entry["top_movies"]:
            st.markdown(f"- **{movie.get('title', 'Inconnu')}** — {movie.get('Metascore', 'N/A')}")

st.header("MongoDB - Film le plus long par genre")
if st.button("10 - Afficher le film le plus long pour chaque genre"):
    results = get_longest_film_by_genre()
    for entry in results:
        genre = entry["_id"]
        film = entry["longest_film"]
        st.markdown(f"- **{genre}** → **{film['title']}** — {film['Runtime']} min")

st.header("MongoDB - Vue films Metascore > 80 & Revenue > 50M")
if st.button("11 - Créer la vue 'vue_films_80_50'", key="create_view_button"):
    create_high_score_high_revenue_view()
    st.success("Vue créée (ou déjà existante).")

if st.button("11bis - Afficher les films de la vue", key="show_view_button"):
    films = get_films_from_view()
    if films:
        for film in films:
            st.markdown(f"- **{film.get('title', 'Inconnu')}** — Metascore : {film.get('Metascore', 'N/A')} | Revenu : {film.get('Revenue (Millions)', 'N/A')} M$")
    else:
        st.warning("Aucun film trouvé dans la vue.")

st.header("MongoDB - Calcule de la corrélation entre la durée/revenu des films")
if st.button("12 - Corrélation durée vs revenu"):
    import numpy as np
    data = get_runtime_and_revenue()
    if data:
        df = pd.DataFrame(data)
        corr = np.corrcoef(df['runtime'], df['revenue'])[0, 1]
        st.write("Corrélation entre durée et revenu :", round(corr, 3))
        st.line_chart(df.set_index('runtime')['revenue'])
    else:
        st.warning("Pas de données exploitables.")

st.header("MongoDB - Évolution de la durée moyenne des films par décennie")
if st.button("13 - Durée moyenne des films par décennie"):
    data = get_avg_runtime_by_decade()
    if data:
        df = pd.DataFrame(data)
        df.rename(columns={"_id": "Décennie", "avg_runtime": "Durée moyenne"}, inplace=True)
        st.line_chart(df.set_index("Décennie"))
    else:
        st.warning("Aucune donnée de durée disponible.") #donc oui il y a une évolution

##########################################
# Section Neo4j - Requêtes Cypher
##########################################

st.header("Neo4j - Gestion des Films et Acteurs")
if st.button("Créer un noeud film d'exemple dans Neo4j"):
    connector = Neo4jConnector()
    sample_film = {
        "id": "film_example",
        "title": "Film Exemple",
        "year": 2022,
        "votes": 1500,
        "revenue": 75000000,
        "rating": 8.2,
        "director": "Directeur Exemple"
    }
    connector.create_film_node(sample_film)
    st.write("Noeud film créé avec succès dans Neo4j.")
    connector.close()

##########################################

st.header("Neo4j - Visualisation et requêtes sur les graphes")
if st.button("Visualiser graphe acteurs-films"):
    afficher_graphe_acteurs_films()

if st.button("Top 1 Acteur (requête alternative)", key="top_actor_alt"):
    connector = Neo4jConnector()
    result = connector.get_top_actor_alt()
    if result:
        st.success(f"**{result['acteur']}** a joué dans **{result['nb_films']}** films.")
    else:
        st.warning("Aucun acteur trouvé.")
    connector.close()

if st.button("Visualiser le graphe de l'acteur le plus prolifique", key="graph_top_actor"):
    draw_top_actor_graph()

if st.button("Voir les co-acteurs d'Anne Hathaway (texte + graphe)", key="combo_coactors"):
    connector = Neo4jConnector()
    coactors = connector.get_coactors_of("Anne Hathaway")
    connector.close()
    if coactors:
        st.success(f"Anne Hathaway a joué avec {len(coactors)} acteur(s) :")
        for co in coactors:
            st.markdown(f"- {co}")
    else:
        st.warning("Aucun co-acteur trouvé.")
    draw_coactors_graph("Anne Hathaway")

if st.button("Revenu cumulé + Graphe"):
    connector = Neo4jConnector()
    top = connector.get_actor_highest_total_revenue()
    if top:
        actor = top["actor"]
        revenue = round(top["totalRevenue"], 2)
        st.success(f"**{actor}** a généré un total de **{revenue} M$**.")
        draw_actor_highest_revenue_graph()
    else:
        st.warning("Aucun acteur trouvé.")

if st.button("Moyenne des votes + Graphe"):
    connector = Neo4jConnector()
    results = connector.get_votes_per_film(limit=10)
    votes = [r["votes"] for r in results if r["votes"] is not None]
    moyenne = sum(votes) / len(votes) if votes else 0
    if results:
        st.success(f"Moyenne des votes (top 10) : **{round(moyenne, 2)}**")
        html_file = draw_votes_avg_graph(results, moyenne)
        st.image(html_file, caption="Votes par film avec moyenne")
    else:
        st.warning("Aucune donnée trouvée.")
    connector.close()

if st.button("Genre le plus fréquent + Graphe"):
    connector = Neo4jConnector()
    genre_info = connector.get_most_common_genre()
    connector.close()
    if genre_info:
        genre = genre_info["genre"]
        count = genre_info["occurrences"]
        st.success(f"Le genre le plus fréquent est **{genre}** avec **{count}** films.")
        html_file = draw_most_common_genre_graph(genre, count)
        components.html(open(html_file, "r", encoding="utf-8").read(), height=600)
    else:
        st.warning("Aucun genre trouvé.")

st.header("Films des co-acteurs d'un membre du projet")
selected_member = st.selectbox("Choisissez un membre", ["Matias", "Pooranan"])
if st.button("Co-acteurs + leurs films (texte + graphe)"):
    connector = Neo4jConnector()
    films = connector.get_films_of_my_coactors(selected_member)
    connector.close()
    if films:
        st.success(f"Films dans lesquels les co-acteurs de **{selected_member}** ont joué :")
        for film in films:
            st.markdown(f"- **{film['title']}** ({film['year']})")
    else:
        st.warning("Aucun film trouvé pour les co-acteurs.")
    draw_coactors_films_graph(selected_member)

if st.button("Voir le réalisateur le plus connecté aux acteurs"):
    connector = Neo4jConnector()
    result = connector.get_director_with_most_actors()
    if result:
        director = result["realisateur"]
        nb = result["nb_acteurs"]
        st.success(f"Le réalisateur **{director}** a dirigé **{nb}** acteurs différents.")
        draw_director_with_actors_graph(director)
    else:
        st.warning("Aucun réalisateur trouvé.")
    connector.close()

st.header("Top 5 des films partageant le plus d’acteurs avec d'autres")
if st.button("Films les plus connectés (graphe global)"):
    connector = Neo4jConnector()
    films = connector.get_most_connected_films(limit=10)
    connector.close()
    if films:
        st.success("Voici les films les plus connectés aux autres via des acteurs partagés.")
        draw_shared_films_graph_only(films)
    else:
        st.warning("Aucun film connecté trouvé.")

if st.button("Acteurs avec le plus de réalisateurs différents"):
    connector = Neo4jConnector()
    acteurs = connector.get_actors_with_most_directors()
    connector.close()
    if acteurs:
        st.success("Acteurs ayant travaillé avec le plus de réalisateurs différents :")
        for a in acteurs:
            st.markdown(f"- **{a['acteur']}** – {a['nb_realisateurs']} réalisateur(s)")
    else:
        st.warning("Aucun acteur trouvé.")

if st.button("Recommander un film + afficher le graphe"):
    acteur = "Scarlett Johansson"  # Vous pouvez remplacer par un st.selectbox()
    connector = Neo4jConnector()
    reco = connector.recommend_film_to_actor(acteur)
    connector.close()
    if reco:
        st.success(f"Film recommandé à **{acteur}** :")
        st.markdown(f"""
        - **Titre** : {reco['titre']}  
        - **Genres** : {reco['genres']}  
        - **Note** : {reco['note']}
        """)
        draw_film_recommendation_graph(acteur)
    else:
        st.warning("Aucune recommandation trouvée.")

if st.button("Créer les relations d'influence entre réalisateurs"):
    connector = Neo4jConnector()
    connector.create_directors_influence_relations()
    connector.close()
    st.success("Relations INFLUENCE_PAR créées entre les réalisateurs.")

if st.button("Voir le chemin le plus court entre Tom Hanks et Scarlett Johansson"):
    draw_shortest_path_between_actors("Tom Hanks", "Scarlett Johansson")
    draw_shortest_path_between_actors("Scarlett Johansson", "Chris Evans")
    draw_shortest_path_between_actors("Leonardo DiCaprio", "Tom Hardy")

if st.button("Visualiser les communautés d'acteurs (Louvain)"):
    draw_actor_communities_graph()

if st.button("Voir les paires d’acteurs avec plusieurs films en commun"):
    draw_actors_with_common_movies(min_common=2)

st.header("Requêtes transversales")
if st.button("Films avec genres communs mais réalisateurs différents"):
    connector = Neo4jConnector()
    films = connector.get_films_with_common_genres_and_different_directors()
    connector.close()
    if films:
        st.dataframe(pd.DataFrame(films))
    else:
        st.warning("Aucun résultat trouvé.")

actor_input = st.text_input("Entrez le nom de l'acteur pour la recommandation :", "Anne Hathaway")
if st.button("Recommander des films à l'acteur"):
    connector = Neo4jConnector()
    reco = connector.recommend_films_based_on_actor_preferences(actor_input)
    connector.close()
    if reco:
        st.dataframe(pd.DataFrame(reco))
    else:
        st.warning("Aucune recommandation trouvée pour cet acteur.")

if st.button("Créer la relation de concurrence entre réalisateurs"):
    connector = Neo4jConnector()
    connector.create_director_competition_relations()
    connector.close()
    st.success("La relation CONCURRENCE a été créée entre réalisateurs.")

if st.button("Collaborations fréquentes réalisateur-acteur"):
    connector = Neo4jConnector()
    collaborations = connector.get_director_actor_collaborations()
    connector.close()
    if collaborations:
        st.success("Collaborations fréquentes identifiées avec leur succès commercial moyen :")
        df = pd.DataFrame(collaborations)
        st.dataframe(df)
    else:
        st.warning("Aucune collaboration trouvée.")
