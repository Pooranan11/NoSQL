import streamlit as st
from db_mongo import (
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
import os
from dotenv import load_dotenv
import pandas as pd

# Chargement du .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Juste pour debug
print("MONGODB_URI utilisé :", os.getenv("MONGODB_URI"))

st.title("Application NoSQL : MongoDB")

##########################################
# Section PyMongo - Requêtes MongoDB
##########################################

st.header("MongoDB - Statistiques Films")
if st.button("Afficher l'année avec le plus de films"):
    result = get_year_with_most_films()
    if result:
        st.write(f"L'année {result['_id']} a produit {result['count']} films.")
    else:
        st.write("Aucun résultat trouvé.")

st.header("Affichage d'une collection MongoDB")
if st.button("Afficher les 5 premiers films"):
    collection = get_films_collection()
    films = list(collection.find().limit(5))
    if films:
        st.write("Voici les 5 premiers films :")
        for film in films:
            st.markdown(f"""
            **Titre** : {film.get('title', 'Inconnu')}  
            **Année** : {film.get('year', 'Non précisée')}  
            **Réalisateur** : {film.get('Director', 'Non précisé')}  
            **Votes** : {film.get('Votes', 0)}  
            **Revenus** : {film.get('Revenue (Millions)', 'N/A')} M$
            ---
            """)
    else:
        st.warning("Aucun film trouvé dans la collection.")

st.header("MongoDB - Films produits après 1999")
if st.button("Compter les films après 1999"):
    total = count_films_after_1999()
    st.success(f"Nombre total de films produits après 1999 : {total}")

st.header("MongoDB - Moyenne des votes en 2007")
if st.button("Afficher la moyenne des votes (2007)"):
    avg_votes = get_avg_votes_for_2007()
    st.info(f"Moyenne des votes pour les films de 2007 : {round(avg_votes, 2)}")

st.header("MongoDB - Statistiques avancées")

# Films par année
if st.button("Nombre de films par année"):
    data = count_films_by_year()
    st.write("Nombre de films par année :")
    st.dataframe(data)

# Genres distincts
if st.button("Genres distincts disponibles"):
    genres = get_distinct_genres()
    st.write(f"{len(genres)} genres trouvés :")
    st.write(", ".join(genres))

# Film avec le plus gros revenu
if st.button("Afficher le film au plus haut revenu"):
    film = get_film_with_highest_revenue()
    if film:
        st.success(f"**{film['title']}** ({film['year']}) – {film['Revenue (Millions)']} M$")
    else:
        st.warning("Aucun film trouvé avec un revenu valide.")

st.header("MongoDB - Réalisateurs prolifiques")
if st.button("Afficher les réalisateurs avec plus de 3 films"):
    directors = get_directors_with_more_than_3_films()
    if directors:
        st.write("Réalisateurs ayant dirigé plus de 3 films :")
        st.dataframe(directors)
    else:
        st.warning("Aucun réalisateur avec plus de 3 films trouvé.")

if st.button("Réalisateurs avec plus de 2 films"):
    data = get_directors_with_more_than_2_films_split()
    st.dataframe(data)

st.header("MongoDB - Genre le plus rentable")
if st.button("Genre avec revenu moyen le plus élevé"):
    genre_info = get_genre_with_highest_avg_revenue()
    if genre_info:
        genre = genre_info["_id"]
        avg = round(genre_info["avg_revenue"], 2)
        st.success(f"Le genre **{genre}** a un revenu moyen de **{avg} M$**.")
    else:
        st.warning("Aucun genre avec revenu valide trouvé.")

st.header("MongoDB - Top 3 par décennie : Rating & Metascore")
if st.button("Top 3 par décennie (Metascore)"):
    results = get_top_3_by_decade_metascore()
    for entry in results:
        st.subheader(f"Décennie {entry['_id']}s (Metascore)")
        for movie in entry["top_movies"]:
            st.markdown(f"- **{movie.get('title', 'Inconnu')}** — {movie.get('Metascore', 'N/A')}")

st.header("MongoDB - Top 3 films complets par décennie")
if st.button("Voir les 3 meilleurs films complets par décennie"):
    results = get_top_3_full_films_by_decade()
    for entry in results:
        st.subheader(f"Décennie {entry['decade']}s")
        for film in entry["topFilms"]:
            st.markdown(f"""
            **{film.get('title', 'Titre inconnu')}**  
            Note : {film.get('rating', 'N/A')}  
            Année : {film.get('year', 'Inconnue')}  
            Genre : {film.get('genre', 'Non précisé')}  
            Metascore : {film.get('Metascore', 'N/A')}  
            Revenu : {film.get('Revenue (Millions)', 'N/A')} M$
            ---
            """)

st.header("MongoDB - Film le plus long par genre")
if st.button("Afficher le film le plus long pour chaque genre"):
    results = get_longest_film_by_genre()
    for entry in results:
        genre = entry["_id"]
        film = entry["longest_film"]
        st.markdown(f"- **{genre}** → **{film['title']}** — {film['Runtime']} min")

if st.button("Créer la vue 'vue_films_80_50'"):
    create_high_score_high_revenue_view()

st.header("MongoDB - Vue films Metascore > 80 & Revenue > 50M")
if st.button("Créer la vue 'vue_films_80_50'", key="create_view_button"):
    create_high_score_high_revenue_view()
    st.success("Vue créée (ou déjà existante).")
if st.button("Afficher les films de la vue", key="show_view_button"):
    films = get_films_from_view()
    if films:
        for film in films:
            st.markdown(f"""
            **{film.get('title', 'Inconnu')}**  
            Metascore : {film.get('Metascore', 'N/A')}  
            Revenu : {film.get('Revenue (Millions)', 'N/A')} M$  
            ---
            """)
    else:
        st.warning("Aucun film trouvé dans la vue.")

if st.button("Corrélation durée vs revenu"):
    import numpy as np
    data = get_runtime_and_revenue()
    if data:
        df = pd.DataFrame(data)
        corr = np.corrcoef(df['runtime'], df['revenue'])[0, 1]
        st.write("Corrélation entre durée et revenu :", round(corr, 3))
        st.line_chart(df.set_index('runtime')['revenue'])
    else:
        st.warning("Pas de données exploitables.")

if st.button("Durée moyenne des films par décennie"):
    data = get_avg_runtime_by_decade()
    if data:
        df = pd.DataFrame(data)
        df.rename(columns={"_id": "Décennie", "avg_runtime": "Durée moyenne"}, inplace=True)
        st.line_chart(df.set_index("Décennie"))
    else:
        st.warning("Aucune donnée de durée disponible.")
