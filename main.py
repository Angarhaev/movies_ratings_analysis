import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os


def movie_ratings_json_export(movies_file, rating_file, movie_id):

    spark = SparkSession.builder.master("local").appName("movie_rats").getOrCreate()

    df_movies = spark.read.csv(movies_file, header=True, inferSchema=True)
    df_ratings = spark.read.csv(rating_file, header=True, inferSchema=True)

    film = df_movies.filter(F.col('movieId') == movie_id).select('title').collect()[0]['title']

    df_film_ratings = df_ratings.filter(F.col('movieID') == movie_id).select('rating')
    df_film_ratings_count = df_film_ratings.groupby('rating').count().orderBy('rating')

    film_ratings_count = df_film_ratings_count.collect()
    list_count_ratings_film = [x for x in range(10)]

    for index, rate in enumerate(film_ratings_count):
        list_count_ratings_film[index] = rate['count']

    all_films_ratings_count = df_ratings.groupby('rating').count().orderBy('rating').collect()
    list_count_ratings_all_films = [x for x in range(10)]

    for index, rate in enumerate(all_films_ratings_count):
        list_count_ratings_all_films[index] = rate['count']

    films_ratings = {
        film: list_count_ratings_film,
        "hist_all": list_count_ratings_all_films}

    with open('ratings.json', 'w') as file:
        json.dump(films_ratings, file, indent=4)

    spark.stop()


def movie_csv_export(movies_file, links_file, genre):
    spark = SparkSession.builder.master("local").appName("movie_to_csv").getOrCreate()

    df_movies = spark.read.csv(movies_file, header=True, inferSchema=True)
    df_links = spark.read.csv(links_file, header=True, inferSchema=True)

    filtered_movies = df_movies.filter(F.col('genres').contains(genre))

    genre_links = filtered_movies.join(df_links, how='inner', on='movieID')

    genre_links.write.csv('movies_links', header=True, mode='overwrite')

    spark.stop()


if __name__ == "__main__":

    movie_file_path = os.path.abspath('ml-25m/movies.csv')
    ratings_file_path = os.path.abspath('ml-25m/ratings.csv')
    searched_movie_id = 2011

    movie_ratings_json_export(movies_file=movie_file_path, rating_file=ratings_file_path, movie_id=searched_movie_id)

    links_file_path = os.path.abspath('ml-25m/links.csv')
    genre = 'Children'

    movie_csv_export(movies_file=movie_file_path, links_file=links_file_path, genre=genre)
