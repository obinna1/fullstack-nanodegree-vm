#! /usr/bin/env python
import psycopg2 as p


DBNAME = "news"

def connect(database_name="news"):
    try:
        db = psycopg2.connect("DBNAME={}".format(database_name))
        cursor = db.cursor()
        return db, cursor
    except:
        print("Error Messages")
        

def getPopularArticles():
    db, cursor = connect()

    popular_articles = '''select articles.title, count(*) as views
           from articles, log
           where log.path = concat('/article/', articles.slug)
           group by articles.title
           order by views DESC
           limit 3'''
    
    c.execute(popular_articles)
    print("Most popular articles:")
    for (title, count) in c.fetchall():
           print("    {} - {} views".format(title, count))
           

           
def getPopularAuthors():
    db, cursor = connect()

    popular_authors = '''select authors.name as author, count(log.path) as views
           from articles, log, authors
           where log.path = concat('/article/', articles.slug)
           and articles.author = authors.id
           group by authors.name
           order by views DESC
           limit 3'''
    
    c.execute(popular_authors)
    print("Most popular authors:")
    for (title, count) in c.fetchall():
           print("    {} - {} views".format(name, count))
           

def getStatusDays():
    db, cursor = connect()

    StatusDays = '''select to_char(date, 'FMMonth FMDD, YYYY'),
                    err/total as ratio
                    from (select time::date as date,
                    count(*) as total,
                    sum((status != '200 OK')::int)::float as err
                    from log
                    group by date) as errors
                    where err/total > 0.01;'''
    
    c.execute(StatusDays)
    print("Days which more than 1% of requests lead to errors were on:")
    for (date, count) in c.fetchall():
           print("    {} - {} views".format(name, count))
    db.close()


