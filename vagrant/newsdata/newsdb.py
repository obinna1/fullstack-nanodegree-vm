#! /usr/bin/env python
import psycopg2


DBNAME = "news"


        
def getPopularArticles():

    db = psycopg2.connect(database=DBNAME)
    c= db.cursor()
    popular_articles = '''select articles.title, count(*) as views
           from articles, log
           where log.path = concat('/article/', articles.slug)
           group by articles.title
           order by views DESC
           limit 3'''
    c.execute(popular_articles)
    rows = c.fetchall()
    print "\n 1. What are the most popular three articles of all time?"
    print "Show me the data for #1! \n"
    for row in rows:
        print " ", row[0], "-", row[1], "views"
    db.close()
    
           

           
def getPopularAuthors():
    
    db = psycopg2.connect(database=DBNAME)
    c= db.cursor()
    popular_authors = '''select authors.name as author, count(log.path) as views
           from articles, log, authors
           where log.path = concat('/article/', articles.slug)
           and articles.author = authors.id
           group by authors.name
           order by views DESC
           limit 3'''
    c.execute(popular_authors)
    rows = c.fetchall()
    print "\n 2. Who are the authors with the most article views?"
    print "Show me the data for #2! \n"
    for row in rows:
        print " ", row[0], "-", row[1], "views"
    db.close()
           

def getStatusDays():
    
    db = psycopg2.connect(database=DBNAME)
    c= db.cursor()

    StatusDays = '''select to_char(date, 'FMMonth FMDD, YYYY'),
                    (err/total * 100) as ratio
                    from (select time::date as date,
                    count(*) as total,
                    sum((status != '200 OK')::int)::float as err
                    from log
                    group by date) as errors
                    where err/total > 0.01;'''
    c.execute(StatusDays)
    rows = c.fetchall()
    print "\n 3. The days with status requests more than 1%?"
    print "Show me the data for #3! \n"
    for row in rows:
        print " ", row[0], "-", row[1], "%"
        print "\n THE END"
    db.close()




getPopularArticles()
getPopularAuthors()
getStatusDays()



