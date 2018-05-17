#!/usr/bin/python2.7

from flask import Flask
import psycopg2

app = Flask(__name__)


def get_popular_articles():
    '''returns the three most popular articles of all time'''
    db = psycopg2.connect("dbname=news")
    cursor = db.cursor()
    query = '''select title, count(*) as views from articles, log where
            slug = substring(path, 10, 100) group by title order by views
            desc limit 3;'''

    cursor.execute(query)
    results = cursor.fetchall()
    db.close()

    text = ""
    for row in results:
        text += "\"" + row[0] + "\""
        text += " - " + str(row[1]) + " views"
        text += "</br>"
    return text


def get_popular_authors():
    '''returns the most popular authors'''
    db = psycopg2.connect("dbname=news")
    cursor = db.cursor()
    query = '''select name, views from authors, (select author, count(*)
            as views from articles, log where slug = substring(path, 10, 100)
            group by author order by views desc) as top_authors where
            top_authors.author = authors.id;'''

    cursor.execute(query)
    results = cursor.fetchall()
    db.close()

    text = ""
    for row in results:
        text += "\"" + row[0] + "\""
        text += " - " + str(row[1]) + " views"
        text += "</br>"
    return text


def get_errors():
    '''returns days with more than 1% of errors'''
    db = psycopg2.connect("dbname=news")
    cursor = db.cursor()
    query = '''select ok.date,
            round(100.0*errors/(ok_responses + errors), 1)
            as errors_percentage from (select date, count(*)
            as ok_responses from (select id, status, cast(time as date)
            as date from log where status = '200 OK') as ok_table
            group by date) as ok, (select date, count(*) as errors
            from (select id, status, cast(time as date) as date
            from log where status != '200 OK') as errors_table
            group by date) as err where ok.date = err.date
            group by ok.date, ok_responses, errors
            having 100*errors/(ok_responses + errors) > 1;'''

    cursor.execute(query)
    results = cursor.fetchall()
    db.close()

    text = ""
    for row in results:
        text += "\"" + str(row[0]) + "\""
        text += " - " + str(row[1]) + "% errors"
        text += "</br>"
    return text


@app.route('/', methods=['GET'])
def main():
    res = "<strong>What are the most popular three articles of all time?"
    res += "</strong>"
    res += "</br></br>" + get_popular_articles() + "</br>"
    res += "<strong>Who are the most popular article authors of all time?"
    res += "</strong>"
    res += "</br></br>" + get_popular_authors() + "</br>"
    res += "<strong>On which days did more than 1% of requests lead to errors?"
    res += "</strong>"
    res += "</br></br>" + get_errors()

    return res

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
