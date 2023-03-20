#### test functions before implementation

import vertica_python
from settings import vertica_user, vertica_pass, database

conn_info = {'host': '51.250.75.20', 
             'port': '5433',
             'user': vertica_user,       
             'password': vertica_pass,
             'database': database,
             'autocommit': True
             }

# function that creates 2 tables in db vertica 
# loads data in staging layer

with vertica_python.connect(**conn_info) as conn:
    cur = conn.cursor()  
    cur.execute("DROP TABLE IF EXISTS SMARTFLIPYANDEXRU__STAGING.group_log;")
    cur.execute("DROP TABLE IF EXISTS SMARTFLIPYANDEXRU__STAGING.group_log_rej;")
    cur.execute("""
                CREATE TABLE SMARTFLIPYANDEXRU__STAGING.group_log
                (
                    group_id INT NOT NULL,
                    user_id INT,
                    user_id_from INT,
                    event varchar(50),
                    datetime timestamp
                );
    """)
    cur.execute("""
                COPY SMARTFLIPYANDEXRU__STAGING.group_log (group_id, user_id, user_id_from, event, datetime)
                FROM LOCAL '/Users/max/Documents/GitHub/de-project-sprint-6/data/group_log.csv'
                DELIMITER ','
                REJECTED DATA AS TABLE SMARTFLIPYANDEXRU__STAGING.group_log_rej;     
     """)
    
    # Make the changes to the database persistent
    records = cur.fetchall()
    print("Total rows are:  ", records[0][0])
    print()
    conn.commit()