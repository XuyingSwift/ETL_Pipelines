import sqlite3

def setup_database():
    connections = sqlite3.connect('news.db')
    c = connections.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS news
                 (title TEXT, author TEXT, published_at TEXT, description TEXT)''')
    connections.commit()
    connections.close()

def load(transformed_data):
    connections = sqlite3.connect('news.db')
    c = connections.cursor()
    c.executemany('INSERT INTO news VALUES (:title, :author, :published_at, :description)', transformed_data)
    connections.commit()
    connections.close()

def verify_database():
    connections = sqlite3.connect('news.db')
    c = connections.cursor()
    c.execute('SELECT * FROM news LIMIT 5')
    rows = c.fetchall()
    for row in rows:
        print(row)
    connections.close()



