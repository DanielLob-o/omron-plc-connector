import traceback
from plc_connector import *
from postgres import *
import logging
import time
import config
import platform
#import subprocess
from subprocess import Popen, PIPE, call
from re import findall
from smtp import smtp_send


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
PLC_ASSET_ID = os.getenv('PLC_ASSET_ID', '')

# DEBUG = True if os.getenv('DEBUG', '') == 'True' else False
def ping (host, ping_count):


    for ip in host:
        data = ''
        try:
            logging.info(f"Trying to decode with utf-8")
            #output = Popen(f"ping {ip} -n {ping_count}", stdout=PIPE, encoding='latin-1')
            output = Popen(f"ping {ip}", stdout=PIPE, encoding='utf-8')
            logging.info(output)
            for line in output.stdout:
                data = data + line
                ping_test = findall("TTL", data)
                if "TTL" in ping_test:
                    logging.info(f"{ip} : Successful Ping")
                    return True
                else:
                    if line == '\n' or findall(host[0], line) is not None:
                        continue
                    else:
                        logging.info(f"{ip} : Failed Ping")
                        return False
        except UnicodeDecodeError as e:
            logging.info(f"Decoding error, trying with latin-1 decoding")
            output = Popen(f"ping {ip}", stdout=PIPE, encoding='latin-1')
            logging.info(output)
            for line in output.stdout:
                data = data + line
                ping_test = findall("TTL", data)
                if "TTL" in ping_test:
                    logging.info(f"{ip} : Successful Ping")
                    return True
                else:
                    if line == '\n' or findall(host[0], line) is not None:
                        continue
                    else:
                        logging.info(f"{ip} : Failed Ping")
                        return False

            #Send alarm
PING_TIME = 5
def make_ping(ips):
    try:
        parameter = '-n' if platform.system().lower() == 'windows' else '-c'
        for ip in ips:
            command = ['ping', parameter, ip]
            response = call(command)

            if response == 0:
                logging.info(f"IP:{ip} ping OK")
                return True
            else:
                logging.warning(f"Switch:{ip} ping KO")
                return False
    except Exception as error:
        logging.warning(f"Error al realizar ping: {error}")

def main():
    plc = None
    try:
        # GET INFO DB
        connection_data = get_connection_info(PLC_ASSET_ID)
        tags_data = get_tags_info(PLC_ASSET_ID, connection_data)
    except Exception as e:
        logging.exception("Exception connecting to PostgreSQL", e)
    else:
        # CONNECT TO PLC
        try:
            config.plc = FINSConnector(connection_data['ip'])

            while not config.plc.get_connected():
                try:
                    logging.info("Attempting to connect to PLC")
                    config.plc.connect()
                    # Make ping to plc
                    # if ping([connection_data['ip']], 3):
                    # # If there's no response, do not connect
                    #     config.plc.connect()
                    # else:
                    #     logging.info(f"Can't reach PLC, ping has no response")
                    #     smtp_send(f"Can't connect to plc: {connection_data['ip']}", "Error llenadora PLC Omron",['josedaniel.sosa@bosonit.com'])
                    #     time.sleep(60)
                except:
                    traceback.print_exc()
                    time.sleep(10)
                    continue
        except Exception as e:
            logging.exception(f"Exception connecting to PLC, ip :{connection_data['ip']}", e)
        else:
            logging.info(f"Connected to PLC, ip :{connection_data['ip']}")

            items = []
            for data in tags_data:
                items.append(
                    {"offset": data["address"], "type": f'{data["type"]}'})



        # INSERT DATA INTO DDBB
        while True:
            try:
                lectura = config.plc.read_db(items)
                insert_plc_data(lectura, tags_data)
            except Exception as e:
                logging.exception(e)

                config.plc.connect()
            finally:
                time.sleep(10)


if __name__ == "__main__":
    main()
