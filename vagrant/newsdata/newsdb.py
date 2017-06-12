import psycopg2 as p

DBNAME = "news"


db = p.connect(database=DBNAME)
c = db.cursor()
c.execute ('''select articles.title, count(*) as views
           from articles, log
           where log.path = concat('/article/', articles.slug)
           group by articles.title
           order by views DESC
           limit 3''')
rows = c.fetchall()
for r in rows:
    print r
    

db = p.connect(database=DBNAME)
c = db.cursor()
c.execute ('''select authors.name as author, count(log.path) as views
           from articles, log, authors
           where log.path = concat('/article/', articles.slug)
           and articles.author = authors.id
           group by authors.name
           order by views DESC
           limit 3''')
rows = c.fetchall()
for r in rows:
    print r
    

db = p.connect(database=DBNAME)
c = db.cursor()
c.execute ('''select date,(error/total * 100) as percenatges
           from (select date(time), count(*) as total, count (*) filter
           (where log.status = '404 NOT FOUND')as error
           from log
           group by date) as percentcalc
           where (error/total) * 100 >= 1
           order by percenatges DESC
           limit 3''')
rows = c.fetchall()
for r in rows:
    print r 
