#!/usr/bin/env python3
"""
"""
import cProfile, pstats
import os
import struct
from pathlib import Path


def main():

    data_path = Path.home() / "data/ITCH_market_data/binary"
    binary_files = [file for file in os.listdir(data_path) if file.endswith(".bin")]

    file_name = Path(binary_files[2])
    file_path = data_path / file_name

    # Start profiler
    pr = cProfile.Profile()
    pr.enable()

    messages = list()

    # Read the whole file into memory
    with open(file_path, "rb") as binary_file:
        data = binary_file.read()

    current_position = 0
    number_of_bytes = len(data)

    # loop through all messages
    while True:
        message_length = data[current_position + 1]
        message_type = data[current_position + 2:current_position + 3]
        message_start = current_position + 3
        message_end = current_position + message_length + 2

        if message_type == b"A":
            message = data[message_start:message_end]
            message = struct.unpack(">iqsiii", message)
            timestamp = message[0]
            order_no = message[1]
            order_verb = message[2]
            order_quantity = message[3]
            orderbook = message[4]
            order_price = message[5]

        elif message_type == b"D":
            message = data[message_start:message_end]
            message = struct.unpack(">iq", message)
        elif message_type == b"U":
            message = data[message_start:message_end]
            message = struct.unpack(">iqqii", message)
        elif message_type == b"E":
            message = data[message_start:message_end]
            message = struct.unpack(">iqiq", message)
        elif message_type == b"I":
            message = data[message_start:message_end]
            message = struct.unpack(">iqiiiis", message)
        elif message_type == b"T":
            message = data[message_start:message_end]
            message = struct.unpack(">i", message)
        elif message_type == b"C":
            message = data[message_start:message_end]
            message = struct.unpack(">iqiqsi", message)
        elif message_type == b"P":
            message = data[message_start:message_end]
            message = struct.unpack(">iiiiqs", message)
        elif message_type == b"P":
            message = data[message_start:message_end]
            message = struct.unpack(">iiiiqs", message)
        elif message_type == b"R":
            message = data[message_start:message_end]
            message = struct.unpack(">iis12s3s8siiiiii", message)
        elif message_type == b"H":
            message = data[message_start:message_end]
            message = struct.unpack(">iiss", message)
        elif message_type == b"L":
            message = data[message_start:message_end]
            message = struct.unpack(">iiii", message)
        elif message_type == b"S":
            message = data[message_start:message_end]
            message = struct.unpack(">i8ssi", message)
        elif message_type == b"M":
            message = data[message_start:message_end]
            message = struct.unpack(">iiii", message)
        elif message_type == b"B":
            message = data[message_start:message_end]
            message = struct.unpack(">iqs", message)
        else:
            raise ValueError(f"Message type {message_type} could not be found")

        messages.append(message)

        current_position = message_end

        if current_position >= number_of_bytes:
            break

    pr.disable()

    sortby = 'cumulative'
    ps = pstats.Stats(pr).sort_stats(sortby)
    ps.print_stats()

if __name__ == "__main__":
    main()
