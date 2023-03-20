#### creates variables to hold credentials

import os

#path = os.getcwd()
path = 'src/dags'

try:
    with open(f'{path}/.env', 'r') as file:
        # read all lines in file
        lines = file.readlines()
        # read fist row
        os.environ[lines[0].split(' = ')[0]] = lines[0].split(' = ')[1].replace('\n','')
        # read second row
        os.environ[lines[1].split(' = ')[0]] = lines[1].split(' = ')[1].replace('\n','')
        # read third row
        os.environ[lines[2].split(' = ')[0]] = lines[2].split(' = ')[1]
        vertica_user = os.environ['vertica_user']
        vertica_pass = os.environ['vertica_pass']
        database = os.environ['database']
except:
    print("You don't have .env file with credentials")



