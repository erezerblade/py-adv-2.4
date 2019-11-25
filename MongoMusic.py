import pandas as pd
from pymongo import MongoClient
from pprint import pprint

client = MongoClient('localhost', 27017)
ticket_db = client['ticket_db']
collection = ticket_db.collection


def read_data(filepath):
    data = pd.read_csv(filepath, header=0)
    collection.insert_many(data.to_dict('records'))


def find_cheapest():
    res = list(collection.find().sort('Цена', 1))
    return res


def find_by_name(name):
    names = list(collection.find().sort('Цена', -1))
    res = []
    for i in names:
        if name in i.get('Исполнитель'):
            res.append(i)
    return res


if __name__ == '__main__':
    read_data('artists.csv')
    pprint(find_cheapest())
    collection.insert_one({
        'Дата': 23.04,
        'Исполнитель': 'Вася Пупкин-Глупкин',
        'Место': 'Тверь',
        'Цена': 1200})
    pprint(find_by_name("Вася"))
