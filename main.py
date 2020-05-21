import pymongo


def zad_2(db):
    imdb_title = db['Title']
    imdb_cast = db['Cast']
    imdb_crew = db['Crew']
    imdb_rating = db['Rating']
    imdb_name = db['Name']
    print("\nZadanie 2:\n"
          "Title:\n"
          f"Pierwszy dokument:{imdb_title.find()[0]}\n"
          f"Rozmiar:{imdb_title.count()}\n"
          "Cast\n"
          f"Pierwszy dokument:{imdb_cast.find()[0]}\n"
          f"Rozmiar:{imdb_cast.count()}\n"
          "Crew\n"
          f"Pierwszy dokument:{imdb_crew.find()[0]}\n"
          f"Rozmiar:{imdb_crew.count()}\n"
          "Rating\n"
          f"Pierwszy dokument:{imdb_rating.find()[0]}\n"
          f"Rozmiar:{imdb_rating.count()}\n"
          "Name\n"
          f"Pierwszy dokument:{imdb_name.find()[0]}\n"
          f"Rozmiar:{imdb_name.count()}")


def zad_3(db):
    imdb_title = db['Title']
    results_with_limitation = imdb_title.find(
        {'startYear': 2005, 'genres': {'$regex': '.*Romance.*'}, 'runtimeMinutes': {'$gt': 100, '$lte': 120}},
        {'originalTitle': 1, 'startYear': 1, 'genres': 1, 'runtimeMinutes': 1, '_id': 0}) \
        .sort('originalTitle', 1).limit(5)
    results_without_limitation = imdb_title.find({}, {'originalTitle': 1, 'startYear': 1, 'genres': 1,
                                                      'runtimeMinutes': 1, '_id': 0}).sort('originalTitle', 1).limit(5)
    print(f"\nZadanie 3:\nZ ograniczeniami:")
    for result in results_with_limitation:
        print(result)
    print(f"Bez ograniczeń:")
    for result in results_without_limitation:
        print(result)


def zad_4(db):
    imdb_title = db['Title']
    results = imdb_title.find({'startYear': 1930, 'genres': {'$regex': '.*Comedy.*'}},
                              {'originalTitle': 1, '_id': 0, 'runtimeMinutes': 1, 'genres': 1}).sort('runtimeMinutes',
                                                                                                     -1)
    print("\nZadanie 4:")
    for result in results:
        print(result)


def zad_5(db):
    imdb_crew = db['Crew']
    results = imdb_crew.aggregate(
        [{"$lookup": {"from": "Title", 'localField': 'tconst', 'foreignField': 'tconst', 'as': 'movie_title'}},
         {'$match': {"movie_title.originalTitle": 'Casablanca', 'movie_title.startYear': 1942}},
         {"$lookup": {"from": "Name", 'localField': 'directors', 'foreignField': 'nconst', 'as': 'person_info'}},
         {'$project': {'person_info.primaryName': 1, '_id': 0, 'person_info.birthYear': 1}}
         ])
    print("\nZadanie 5:")
    for result in results:
        print(result)


def zad_6(db):
    imdb_title = db['Title']
    results = imdb_title.aggregate([
        {'$match': {'startYear': {'$gte': 2007, '$lte': 2009}}},
        {'$group': {'_id': '$titleType', 'count': {'$sum': 1}}}])
    print("\nZadanie 6:")
    for result in results:
        print(result)


def zad_7(db):
    pass


def zad_8(db):
    pass


def zad_9(db):
    pass


def zad_10(db):
    pass


if __name__ == '__main__':
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017')
    db_imdb = mongo_client['IMDB']
    # zad_2(db_imdb)
    # print('')
    # zad_3(db_imdb)
    # print('')
    # zad_4(db_imdb)
    # print('')
    zad_5(db_imdb)
    print('')
    # zad_6(db_imdb)
    # print('')
    zad_7(db_imdb)
    print('')
    zad_8(db_imdb)
    print('')
    zad_9(db_imdb)
    print('')
    zad_10(db_imdb)
    print('')
