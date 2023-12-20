import logging
import struct
from datetime import time
import re
import time
import fins
import fins.udp
import os

API_ALARMS_HOST = os.getenv('API_ALARMS_HOST', '192.168.76.103')
API_ALARMS_PORT = os.getenv('API_ALARMS_PORT', '5000')
DEBUG = True if os.getenv('DEBUG', '') == 'True' else False
SOURCE_NODE = os.getenv('SOURCE_NODE', 1)


class FINSConnector:
    assert_counter = 0
    """
    Connector for reading data from Omron PLCs using the FINS protocol.

    Note: data in an Omron PLC is not organized in DBs, but we treat it as so for consistency.
    """
    # Freq in seconds
    if DEBUG:
        DBs = [{"name": "db_alarmas", "num": 105, "freq": 2}, {"name": "db_comunicaciones", "num": 107, "freq": 5}]
    else:
        DBs = [{"name": "db_alarmas", "num": 105, "freq": 2},
               {"name": "db_comunicaciones", "num": 107, "freq": 5},
               {"name": "db_contadores_desgastes", "num": 108, "freq": 3600},
               {"name": "db_general_graficar", "num": 104, "freq": 2},
               {"name": "db_general_visualizar", "num": 109, "freq": 5},
               {"name": "db_servos_graficar", "num": 106, "freq": 10},
               {"name": "db_servos_visualizar", "num": 110, "freq": 5},
               {"name": "db_alarmas_custom", "num": 111, "freq": 60}]

    # DBs = [{"name": "db_servos_visualizar", "num": 110, "freq": 5}]
    enable_DB = {"num": 111, "size": 2, "items": [{"variable": "test_lectura", "offset": 'D1000', "type": "REAL"},
                                                  {"variable": "contador_parcial", "offset": 'D1004', "type": "INT"},
                                                  {"variable": "contador_total", "offset": 'D1008', "type": "INT"}]}

    def __init__(self, ip):
        self.ip = ip
        self.plc = None
        self._connected = False

    def connect(self):
        self.plc = fins.udp.UDPFinsConnection()
        self.plc.connect(self.ip)
        self.plc.dest_node_add = int(self.ip.split('.')[-1])
        self.plc.srce_node_add = int(SOURCE_NODE)
        self._connected = True

    def get_connected(self):
        return self._connected

    def read_db_solo(self, tags_list):
        ma = fins.FinsPLCMemoryAreas()
        mem_area = self.plc.memory_area_read(ma.DATA_MEMORY_BIT, b'\x00\x03\xE8', 1)
        print(mem_area)
        time.sleep(1)

    def read_db(self, dbitems):
        try:
            memory_area_codes = []
            memory_area_addresses = []

            ma = fins.FinsPLCMemoryAreas()
            format_string = '>'
            headers_type_bytes = []

            for item in dbitems:
                if not item['type'] or not item['offset']:
                    continue

                skip_char = 1
                num_codes = 2 if item['type'].upper() in ['DINT', 'REAL', 'UDINT'] else 1
                if 'STRING' in item['type'].upper():
                    size = re.search('string\((\\d*)\)', item['type'].upper(), re.IGNORECASE).group(1)
                    num_codes = int(size) // 2 if size else 1

                # Memory codes
                if item['type'].upper() == 'BOOL':
                    if item['offset'][0].isdigit():
                        memory_area_codes.append(ma.CIO_BIT)
                        skip_char = 0
                    if item['offset'][0:2] == 'DM':
                        memory_area_codes.append(ma.DATA_MEMORY_BIT)
                        skip_char = 2
                    if item['offset'][0] == 'D':
                        memory_area_codes.append(ma.DATA_MEMORY_BIT)
                    if item['offset'][0] == 'W':
                        memory_area_codes.append(ma.WORK_BIT)
                    if item['offset'][0] == 'H':
                        memory_area_codes.append(ma.HOLDING_BIT)
                    if item['offset'][0] == 'A':
                        memory_area_codes.append(ma.AUXILIARY_BIT)
                    if item['offset'][0] == 'T':
                        memory_area_codes.append(ma.TIMER_FLAG)
                    if item['offset'][0] == 'C':
                        memory_area_codes.append(ma.COUNTER_FLAG)
                else:
                    for i in range(0, num_codes):
                        if item['offset'][0].isdigit():
                            memory_area_codes.append(ma.CIO_WORD)
                            skip_char = 0
                        if item['offset'][0:2] == 'DM':
                            memory_area_codes.append(ma.DATA_MEMORY_WORD)
                            skip_char = 2
                        else:
                            if item['offset'][0] == 'D':
                                memory_area_codes.append(ma.DATA_MEMORY_WORD)
                            if item['offset'][0] == 'W':
                                memory_area_codes.append(ma.WORK_WORD)
                            if item['offset'][0] == 'H':
                                memory_area_codes.append(ma.HOLDING_WORD)
                            if item['offset'][0] == 'A':
                                memory_area_codes.append(ma.AUXILIARY_WORD)
                            if item['offset'][0] == 'T':
                                memory_area_codes.append(ma.TIMER_WORD)
                            if item['offset'][0] == 'C':
                                memory_area_codes.append(ma.COUNTER_WORD)

                # Get address in bytes
                address = list(map(lambda x: int(x), item['offset'][skip_char:].split('.')))
                if len(address) == 1:
                    address.append(0)
                address_format = '>hb'
                if item['type'].upper() in ['DINT', 'REAL', 'UDINT']:
                    memory_area_addresses.append(bytearray(struct.pack(address_format, *address)))
                    address[0] = address[0] + 1
                    memory_area_addresses.append(bytearray(struct.pack(address_format, *address)))
                elif 'STRING' in item['type'].upper():
                    size = re.search('string\((\\d*)\)', item['type'].upper(), re.IGNORECASE).group(1)
                    for i in range(int(size) // 2):
                        memory_area_addresses.append(bytearray(struct.pack(address_format, *address)))
                        address[0] = address[0] + 1
                else:
                    # a = bytearray(struct.pack(address_format, *address))
                    memory_area_addresses.append(bytearray(struct.pack(address_format, *address)))

                # Quedan algunos tipos que no sé si hace falta contemplar: Data Register, Index Register,
                # Task Flag, EM Area, Clock Pulses, Condition Flags, etc.

                # TODO Rellenar esto. UInt del PLC es H (unsigned short)
                # Data type formatting
                if item['type'].upper() == 'BOOL':  # Initial x corresponds to Memory Area Code pad byte
                    format_string += '?'
                    headers_type_bytes.append(1)
                if item['type'].upper() == 'DINT':
                    # format_string += 'xh'
                    format_string += 'i'
                    headers_type_bytes.append(2)
                    headers_type_bytes.append(2)
                if item['type'].upper() == 'UDINT':
                    # format_string += 'xh'
                    format_string += 'I'
                    headers_type_bytes.append(2)
                    headers_type_bytes.append(2)
                if item['type'].upper() == 'INT':
                    format_string += 'h'
                    headers_type_bytes.append(2)
                if item['type'].upper() == 'UINT_BCD':
                    format_string += 'H'
                    headers_type_bytes.append(2)
                if item['type'].upper() == 'REAL':
                    # format_string += 'xe'
                    format_string += 'f'
                    headers_type_bytes.append(2)
                    headers_type_bytes.append(2)
                if 'STRING' in item['type'].upper():  # TODO Check string length?
                    size = re.search('string\((\\d*)\)', item['type'].upper(), re.IGNORECASE).group(1)
                    if not size:
                        size = '2'
                    format_string += f'{size}s'
                    for i in range(int(size) // 2):
                        headers_type_bytes.append(2)
                if item['type'].upper() == '???':
                    format_string += ''

            data_bytes = bytes()
            chunk_size = 300
            logging.info(f"memory are addresses: {memory_area_addresses}")

            logging.info(f"Chunk size: {chunk_size}")
            for i in range(0, len(memory_area_addresses), chunk_size):
                address_batch = memory_area_addresses[i:i + chunk_size]
                codes_batch = memory_area_codes[i:i + chunk_size]

                response = self.plc.multiple_memory_area_read(codes_batch, address_batch)
                logging.info(response)
                if len(response) != 0:
                    logging.info("Correct response reading batch")
                else:
                    logging.info(f'Response was wrong:{response}')

                # Look for x01, x04 on bytes 11-12 (request type "multiple area read")
                # Look for x00, x00 on bytes 13-14 (response code OK)
                # Actual data in bytes 16-17, 19-20, 22-23, (...)
                assert self.validate_read_packet(response)

                data_bytes += response[14:]  # Remove headers
                # data_bytes += self.extract_read_data(response)
            data_bytes = bytearray(data_bytes)
            # for i in range(len(data_bytes)):
            #     if i+1 == 1 or (i+1)%3 == 0:
            #         data_bytes.pop(i)
            j = 0
            for i in headers_type_bytes:
                data_bytes.pop(j)
                j = j + i
            data_list = []
            for item in dbitems:
                if item['type'].upper() == 'BOOL':
                    data_list.append(struct.unpack('>?', data_bytes[0].to_bytes(1, 'big'))[0])
                    data_bytes.pop(0)
                if item['type'].upper() == 'DINT':
                    order_bytes = data_bytes[2].to_bytes(1, 'big') + data_bytes[3].to_bytes(1, 'big') + data_bytes[
                        0].to_bytes(1, 'big') + data_bytes[1].to_bytes(1, 'big')
                    data_list.append(struct.unpack('>i', order_bytes)[0])
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                if item['type'].upper() == 'UDINT':
                    order_bytes = data_bytes[2].to_bytes(1, 'big') + data_bytes[3].to_bytes(1, 'big') + data_bytes[
                        0].to_bytes(1, 'big') + data_bytes[1].to_bytes(1, 'big')
                    data_list.append(struct.unpack('>I', order_bytes)[0])
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                if item['type'].upper() == 'INT':
                    data_list.append(
                        struct.unpack('>h', data_bytes[0].to_bytes(1, 'big') + data_bytes[1].to_bytes(1, 'big'))[0])
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                if item['type'].upper() == 'UINT_BCD':
                    dec = struct.unpack('>H', data_bytes[0].to_bytes(1, 'big') + data_bytes[1].to_bytes(1, 'big'))[0]
                    data_list.append(int(hex(dec)[2:]))
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                if item['type'].upper() == 'REAL':
                    order_bytes = data_bytes[2].to_bytes(1, 'big') + data_bytes[3].to_bytes(1, 'big') + data_bytes[
                        0].to_bytes(1, 'big') + data_bytes[1].to_bytes(1, 'big')
                    data_list.append(struct.unpack('>f', order_bytes)[0])
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                    data_bytes.pop(0)
                if 'STRING' in item['type'].upper():
                    size = re.search('string\((\\d*)\)', item['type'].upper(), re.IGNORECASE).group(1)
                    if not size:
                        size = '2'
                    data_string = bytes()
                    # data_list.append(struct.unpack(f'>{size}s', data_bytes[0].to_bytes(1,'big')+data_bytes[1].to_bytes(1,'big'))[0])
                    for i in range(int(size)):
                        data_string += struct.unpack(f'>1s', data_bytes[0].to_bytes(1, 'big'))[0]
                        data_bytes.pop(0)
                    data_list.append(data_string)
            # data_list = struct.unpack(format_string, data_bytes)

            lectura = {}
            for item, data in zip(dbitems, data_list):  # TODO Test
                if type(data) == bytes:
                    lectura[item['offset']] = data.decode('utf-8')
                else:
                    lectura[item['offset']] = data

            return lectura
        except AssertionError:
            self.assert_counter = self.assert_counter + 1
            if self.assert_counter > 15:
                self.assert_counter = 0
                time.sleep(900)

    @staticmethod
    def validate_read_packet(packet):
        if not packet:
            return False
        if packet[10:12] != b'\x01\x04':  # Incorrect request type
            return False
        if packet[12:14] != b'\x00\x00':  # Bad response code
            return False
        return True

    @staticmethod
    def extract_read_data(packet):
        data = bytes()
        for i in range(15, len(packet), 3):
            value = packet[i:i + 2]
            data += value

        return data
