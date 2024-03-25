from configparser import ConfigParser
import os

def config(filename='src/database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            if param[0] == 'TABLES':
                # Split the comma-separated string into a list of tables
                db[param[0]] = param[1].split(',')
            else:
                db[param[0]] = param[1]
    else:
        raise Exception(f'Section "{section}" is not found in the "{filename}" file.')
    print(db)
    return db

config()