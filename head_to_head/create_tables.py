import psycopg2
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:admin@localhost:5432/streetfighter6')

def create_tables():
    '''create tables in the PostgreSQL database'''
    commands = (
        '''
        CREATE TABLE if not exists players (
            player_id INTEGER PRIMARY KEY,
            player_name TEXT
            )

            #
            p1_char TEXT,
            p1_mr INTEGER,
            p2_char TEXT,
            p2_mr INTEGER,
            p1_result BOOL

        ''')

    conn = None

    try:
        conn = psycopg2.connect('postgresql://postgres:admin@localhost:5432/streetfighter6')
        cur = conn.cursor()

        #for command in commands:
        cur.execute(commands)

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    finally:
        if conn is not None:
                conn.close()

if __name__ == '__main__':
    create_tables()
