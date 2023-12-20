import os
import pytz
import psycopg2
import logging
from psycopg2.extras import RealDictCursor
from datetime import datetime
import shelve

# Shelve file must be created as a volume in the portainer container, it is created as soon as you create the
# volume directory in the portainer 'Volumes' part

shelve_file = os.getenv('SHELVE_FILE')

tzspain = pytz.timezone("Europe/Madrid")

DDBB_INFO = {
    "user": os.getenv('POSTGRES_USER', "postgres"),
    "password": os.getenv('POSTGRES_PASSWORD', ""),
    "host": os.getenv('POSTGRES_HOST', ""),
    "port": os.getenv('POSTGRES_PORT', "5432"),
    "database": os.getenv('POSTGRES_DB', "postgres")
}
canos_dms=['DM1012','DM1014','DM1016','DM1018','DM1020','DM1022','DM1024','DM1026','DM1028','DM1030','DM1032','DM1034',
           'DM1036','DM1038','DM1040','DM1042','DM1044','DM1046','DM1048','DM1050','DM1052','DM1054','DM1056','DM1058',
           'DM1060','DM1062','DM1064','DM1066','DM1068','DM1070','DM1072','DM1074']

def get_connection_info(asset_id):
    try:
        with psycopg2.connect(**DDBB_INFO) as con:
            with con.cursor() as cur:
                cur.execute(f""" select ip, port, a.manufacturer 
                from elliot.asset_plc ap join elliot.asset a on ap.asset_id = a.id 
                where id = {asset_id}""")
                connection_data = cur.fetchall()
        return {'ip': connection_data[0][0], 'port': connection_data[0][1], 'manufacturer': connection_data[0][2]}
    except (Exception, psycopg2.Error) as error:
        logging.exception(f"Error while connecting to PostgreSQL {error, Exception}")


def get_tags_info(asset_id, connection_data):
    try:
        if connection_data['manufacturer'] == "siemens":
            columns = "m.id, dpi.db_num, dpi.offset, dpi.type"
        else:
            columns = " m.id, dpi.address, dpi.type"
        with psycopg2.connect(**DDBB_INFO) as con:
            with con.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"""select {columns} 
                from elliot.asset a join elliot.metric m on a.id = m.asset_id 
                join elliot.dms_plcs_info dpi on m.id = dpi.metric_id
                where m.asset_id = {asset_id}""")
                tags_info = cur.fetchall()
        return tags_info
    except (Exception, psycopg2.Error) as error:
        logging.exception(f"Error while connecting to PostgreSQL {error, Exception}")


def insert_plc_data(lectura, tags_data):
    logging.info(f'len lectura {len(lectura)}')
    data_to_insert = []
    time = datetime.now(tz=tzspain)
    with shelve.open(shelve_file) as previous_data:
        #   With container volume compare the last values of some variables with the lectura, then if the values are
        #   the same dont insert the data, else, insert it
        for tag in tags_data:

            try:
                if tag['address'] in canos_dms and tag['address'] not in previous_data.keys():
                    previous_data[tag['address']] = lectura[tag['address']]
                    data_to_insert.append(tuple([tag['id'], time, lectura[tag['address']]]))
                elif tag['address'] in canos_dms and previous_data[tag['address']] != lectura[tag['address']]:
                    data_to_insert.append(tuple([tag['id'], time, lectura[tag['address']]]))
                    previous_data[tag['address']] = lectura[tag['address']]
                elif tag['address'] not in canos_dms:
                    data_to_insert.append(tuple([tag['id'], time, lectura[tag['address']]]))

            except Exception as e:
                logging.exception(e)
                continue

    logging.info(f'Data to insert len: {len(data_to_insert)}')
    try:
        with psycopg2.connect(**DDBB_INFO) as con:
            with con.cursor() as cur:
                query = '''insert into elliot.metric_numeric_data(metric_id, ts, value)values %s'''
                psycopg2.extras.execute_values(cur, query, data_to_insert)
                con.commit()
                logging.info('PLC data successfully inserted.')
    except (Exception, psycopg2.Error) as error:
        logging.exception(f"Error while connecting to PostgreSQL {error, Exception}")
