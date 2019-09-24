#!/usr/bin/env python3
"""
"""

# standard libraries
from collections import defaultdict, Counter
from operator import neg, itemgetter
from sortedcontainers import SortedDict
from struct import unpack

# third-party packages
import numpy as np
from tqdm import tqdm


class OrderBookSide(SortedDict):
    def __missing__(self, key):
        return 0


class SingleDayIMIData(object):
    """Class that loads and processes IMI messages for a single date"""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.current_position = 0

        self.orders = defaultdict(dict)
        self.orderbooks = defaultdict(dict)
        self.blue_chip_orderbooks = list()
        self.get_order_info = itemgetter(
            "orderbook", "order_verb", "order_entry_price", "order_quantity_outstanding"
        )

        self.metadata = defaultdict(dict)
        self.message_times = defaultdict(list)

        # Reading the binary file into memory
        with open(self.file_path, "rb") as binary_file:
            self.data = binary_file.read()
        self.number_of_bytes = len(self.data)

    def process_messages(self):
        """Convert and process all messages inside a loop"""

        # only loop if we are not yet at the end of the file
        while self.current_position < self.number_of_bytes:

            message_length = self.data[self.current_position + 1]
            message_type = self.data[
                self.current_position + 2 : self.current_position + 3
            ]
            message_start = self.current_position + 3
            message_end = self.current_position + message_length + 2

            message = self.data[message_start:message_end]

            # Time Stamp – Seconds message
            if message_type == b"T":
                message = unpack(">i", message)
                self.microseconds = int(message[0] * 1e6)
                if (
                    self.microseconds >= 9.5 * 3600e6
                    and self.microseconds < 17 * 3600e6
                ):
                    self.message_times[message_type].append(self.microseconds)

            # Orderbook Directory message
            elif message_type == b"R":
                message = unpack(">iis12s3s8siiiiii", message)
                group = message[5]
                orderbook = message[1]

                # initialize each side of the orderbook
                self.orderbooks[orderbook][b"B"] = OrderBookSide(neg)
                self.orderbooks[orderbook][b"S"] = OrderBookSide()
                self.orderbooks[orderbook][b" "] = OrderBookSide()
                self.metadata[orderbook]["group"] = group
                if group == b"ACoK    ":
                    self.blue_chip_orderbooks.append(orderbook)
                    self.metadata[orderbook]["price_type"] = message[2]
                    self.metadata[orderbook]["isin"] = message[3]
                    self.metadata[orderbook]["currency"] = message[4]
                    self.metadata[orderbook]["minimum_quantity"] = message[6]
                    self.metadata[orderbook]["quantity_tick_table_id"] = message[7]
                    self.metadata[orderbook]["price_tick_table_id"] = message[8]
                    self.metadata[orderbook]["price_decimals"] = message[9]
                    self.metadata[orderbook]["delisting_date"] = message[10]
                    self.metadata[orderbook]["delisting_time"] = message[11]

            # Orderbook Trading Action message
            elif message_type == b"H":
                message = unpack(">iiss", message)
                timestamp = int(self.microseconds + message[0] * 1e-3)
                orderbook = message[1]
                trading_state = message[2]
                book_condition = message[3]

                if orderbook in self.blue_chip_orderbooks:
                    self.message_times[message_type].append(timestamp)

            # System Event message
            elif message_type == b"S":
                message = unpack(">i8ssi", message)
                timestamp = int(self.microseconds + message[0] * 1e-3)
                group = message[1]
                event_code = message[2]
                orderbook = message[3]

                if orderbook in self.blue_chip_orderbooks:
                    self.message_times[message_type].append(timestamp)

            # Price Tick Size message
            elif message_type == b"L":
                message = unpack(">iiii", message)
                timestamp = int(self.microseconds + message[0] * 1e-3)
                price_tick_table_id = message[1]
                price_tick_size = message[2]
                price_start = message[3]

                self.message_times[message_type].append(timestamp)

            # Quantity Tick Size message
            elif message_type == b"M":
                message = unpack(">iiii", message)
                timestamp = int(self.microseconds + message[0] * 1e-3)
                quantity_tick_table_id = message[1]
                quantity_tick_size = message[2]
                quantity_start = message[3]

                self.message_times[message_type].append(timestamp)

            else:
                pass  # because message type is not relevant

            # update current position for next iteration
            self.current_position = message_end

            # # Add Order Message
            # if message_type == b"A":
            #     pass

            # # Order Delete Message
            # elif message_type == b"D":
            #     pass

            # # Order Replace Message
            # elif message_type == b"U":
            #     pass

            # # Order Executed Message
            # elif message_type == b"E":
            #     pass

            # # Order Executed With Price message
            # elif message_type == b"C":
            #     pass

            # # Indicative Price / Quantity Message
            # elif message_type == b"I":
            #     # message = unpack(">iqiiiis", message)
            #     pass # not relevant

            # # Trade message (SwissAtMid / EBBO)
            # elif message_type == b"P":
            #     pass

            # # Broken Trade message
            # elif message_type == b"B":  # not relevant
            #     pass

            # elif message_type == b"G":  # not relevant
            #     pass
