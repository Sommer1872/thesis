#!/usr/bin/env python3
"""
"""

# standard libraries
from collections import defaultdict, namedtuple
from operator import neg, itemgetter
from pathlib import Path
import struct

# third-party packages
import numpy as np
import pandas as pd
from sortedcontainers import SortedDict


class OrderBookSide(SortedDict):
    def __missing__(self, key):
        return 0
    def peekitem(self, index=-1):
        try:
            return super().peekitem(index)
        except IndexError:
            return (np.nan, np.nan)


class SingleDayIMIData(object):
    """Class that loads and processes IMI messages for a single date"""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.date = file_path.name[11:21].replace("_", "-")
        self.current_position = 0

        self.orders = defaultdict(dict)
        self.orderbooks = defaultdict(dict)
        self.price_tick_sizes = defaultdict(dict)
        self.blue_chip_orderbooks = list()
        self.metadata = defaultdict(dict)

        self.unpack = struct.unpack
        self.get_order_info = itemgetter(
            "orderbook_no", "book_side", "price", "quantity_outstanding"
        )

        self.transactions = defaultdict(list)
        self.Transaction = namedtuple("Transaction", ["timestamp", "price", "size",
            "best_bid", "best_ask", "best_bid_quantity", "best_ask_quantity"])
        self.snapshots = defaultdict(dict)
        self.Snapshot = namedtuple(
            "Snapshot",
            ["best_bid", "best_ask", "best_bid_quantity", "best_ask_quantity"],
        )
        self.best_bid_ask = defaultdict(list)
        self.NewBest = namedtuple("NewBest", ["timestamp", "book_side", "new_best_price"])

        # Reading the binary file into memory
        with open(self.file_path, "rb") as binary_file:
            self.data = binary_file.read()
        self.number_of_bytes = len(self.data)

    def process_messages(self):
        """Convert and process all messages inside a loop"""

        # as long as we haven't reached the end of the file:
        while self.current_position < self.number_of_bytes:

            message_length = self.data[self.current_position + 1]
            message_type = self.data[
                self.current_position + 2 : self.current_position + 3
            ]
            message_start = self.current_position + 3
            message_end = self.current_position + message_length + 2
            # access the message
            message = self.data[message_start:message_end]

            # Add Order Message
            if message_type == b"A":
                message = self.unpack(">iqsiii", message)
                timestamp = self.microseconds + int(message[0] * 1e-3)
                order_no = message[1]
                book_side = message[2]
                quantity = message[3]
                orderbook_no = message[4]
                price = message[5]
                this_order = self.orders[order_no]
                this_order["orderbook_no"] = orderbook_no
                this_order["book_side"] = book_side
                this_order["quantity_outstanding"] = quantity
                this_order["price"] = price

                if orderbook_no in self.blue_chip_orderbooks:
                    # update the side of the orderbook_no
                    this_orderbook = self.orderbooks[orderbook_no][book_side]
                    this_orderbook[price] += quantity
                    # record if new best bid/ask
                    best_price, best_quantity = this_orderbook.peekitem(0)
                    if (price == best_price) & (quantity == best_quantity):
                        self.best_bid_ask[orderbook_no].append(self.NewBest(
                            timestamp=timestamp,
                            book_side=book_side,
                            new_best_price=price
                        ))

            # Time Stamp – Seconds message
            elif message_type == b"T":
                message = self.unpack(">i", message)
                seconds = message[0]
                self.microseconds = int(seconds * 1e6)
                if seconds >= 8 * 3600 and seconds < 18 * 3600:
                    for orderbook_no in self.blue_chip_orderbooks:
                        this_orderbook = self.orderbooks[orderbook_no]
                        best_bid_price, best_bid_quantity = this_orderbook[b"B"].peekitem(0)
                        best_ask_price, best_ask_quantity = this_orderbook[b"S"].peekitem(0)
                        self.snapshots[orderbook_no][seconds] = self.Snapshot(
                            best_bid=best_bid_price,
                            best_ask=best_ask_price,
                            best_bid_quantity=best_bid_quantity,
                            best_ask_quantity=best_ask_quantity,
                        )

            # Order Delete Message
            elif message_type == b"D":
                message = self.unpack(">iq", message)
                timestamp = self.microseconds + int(message[0] * 1e-3)
                order_no = message[1]
                this_order = self.orders[order_no]
                # update the order book
                orderbook_no, book_side, price, quantity_outstanding = self.get_order_info(
                    this_order
                )
                if orderbook_no in self.blue_chip_orderbooks:
                    this_orderbook = self.orderbooks[orderbook_no][book_side]
                    this_orderbook[price] -= quantity_outstanding
                    if this_orderbook[price] == 0:
                        # check if price was at best
                        if this_orderbook.index(price) == 0:
                            best_price = this_orderbook.peekitem(1)[0]
                            self.best_bid_ask[orderbook_no].append(self.NewBest(
                                timestamp=timestamp,
                                book_side=book_side,
                                new_best_price=best_price
                            ))
                        this_orderbook.pop(price)
                # remove order
                self.orders.pop(order_no)

            # Order Replace Message
            elif message_type == b"U":
                message = self.unpack(">iqqii", message)
                timestamp = self.microseconds + int(message[0] * 1e-3)
                old_order_no = message[1]
                old_order = self.orders[old_order_no]
                new_order_no = message[2]
                new_order = self.orders[new_order_no]
                quantity = message[3]
                price = message[4]
                book_side = old_order["book_side"]
                orderbook_no = old_order["orderbook_no"]
                new_order["book_side"] = book_side
                new_order["quantity_outstanding"] = quantity
                new_order["orderbook_no"] = orderbook_no
                new_order["price"] = price

                # adjust orderbook
                this_orderbook = self.orderbooks[orderbook_no][book_side]
                if orderbook_no in self.blue_chip_orderbooks:
                    # new order
                    this_orderbook[price] += quantity
                    # record if new best bid/ask
                    best_price, best_quantity = this_orderbook.peekitem(0)
                    if (price == best_price) & (quantity == best_quantity):
                        self.best_bid_ask[orderbook_no].append(self.NewBest(
                            timestamp=timestamp,
                            book_side=book_side,
                            new_best_price=price
                        ))
                    # old order
                    old_order_price = old_order["price"]
                    this_orderbook[old_order_price] -= old_order["quantity_outstanding"]
                    if this_orderbook[old_order_price] == 0:
                        # check if price was at best
                        if this_orderbook.index(old_order_price) == 0:
                            best_price = this_orderbook.peekitem(1)[0]
                            self.best_bid_ask[orderbook_no].append(self.NewBest(
                                timestamp=timestamp,
                                book_side=book_side,
                                new_best_price=best_price
                            ))
                        this_orderbook.pop(old_order_price)
                # remove old order
                # self.orders.pop(old_order_no)

            # Order Executed Message
            elif message_type == b"E":
                message = self.unpack(">iqiq", message)
                timestamp = self.microseconds + int(message[0] * 1e-3)
                order_no = message[1]
                executed_quantity = message[2]
                match_number = message[3]
                # update the order entry
                this_order = self.orders[order_no]
                this_order["quantity_outstanding"] -= executed_quantity
                # order book
                orderbook_no, book_side, price, quantity_outstanding = self.get_order_info(
                    this_order
                )
                if orderbook_no in self.blue_chip_orderbooks:
                    this_orderbook = self.orderbooks[orderbook_no]
                    # info to calculate effective spreads
                    best_bid_price, best_bid_quantity = this_orderbook[b"B"].peekitem(0)
                    best_ask_price, best_ask_quantity = this_orderbook[b"S"].peekitem(0)
                    self.transactions[orderbook_no].append(self.Transaction(
                        timestamp=timestamp,
                        price=price,
                        size=executed_quantity,
                        best_bid=best_bid_price,
                        best_ask=best_ask_price,
                        best_ask_quantity=best_ask_quantity,
                        best_bid_quantity=best_bid_quantity,
                    ))
                    # update order book
                    this_orderbook = this_orderbook[book_side]
                    this_orderbook[price] -= executed_quantity
                    if this_orderbook[price] == 0:
                        # record if new best bid/ask
                        if this_orderbook.index(price) == 0:
                            best_price = this_orderbook.peekitem(1)[0]
                            self.best_bid_ask[orderbook_no].append(self.NewBest(
                                timestamp=timestamp,
                                book_side=book_side,
                                new_best_price=best_price
                            ))
                        this_orderbook.pop(price)
                if quantity_outstanding == 0:
                    self.orders.pop(order_no)

            # Order Executed With Price message
            elif message_type == b"C":
                message = self.unpack(">iqiqsi", message)
                # timestamp = self.microseconds + message[0] * 1e-3
                order_no = message[1]
                executed_quantity = message[2]
                # match_number = message[3]
                # printable = message[4]
                # execution_price = message[5]
                # update the order entry
                this_order = self.orders[order_no]
                orderbook_no, book_side, price, _ = self.get_order_info(this_order)
                this_order["quantity_outstanding"] -= executed_quantity
                # update the order book
                if orderbook_no in self.blue_chip_orderbooks:
                    this_orderbook = self.orderbooks[orderbook_no][book_side]
                    this_orderbook[price] -= executed_quantity
                    if this_orderbook[price] == 0:
                        this_orderbook.pop(price)
                if this_order["quantity_outstanding"] == 0:
                    self.orders.pop(order_no)

            # Orderbook Directory message
            elif message_type == b"R":
                message = self.unpack(">iis12s3s8siiiiii", message)

                # initialize each side of the orderbook
                orderbook_no = message[1]
                this_orderbook = self.orderbooks[orderbook_no]
                this_orderbook[b"B"] = OrderBookSide(neg)
                this_orderbook[b"S"] = OrderBookSide()
                this_orderbook[b" "] = OrderBookSide()

                group = message[5]
                this_metadata = self.metadata[orderbook_no]
                this_metadata["group"] = group
                if group == b"ACoK    ":
                    self.blue_chip_orderbooks.append(orderbook_no)
                    this_metadata["price_type"] = message[2]
                    this_metadata["isin"] = message[3]
                    this_metadata["currency"] = message[4]
                    this_metadata["minimum_quantity"] = message[6]
                    this_metadata["quantity_tick_table_id"] = message[7]
                    this_metadata["price_tick_table_id"] = message[8]
                    this_metadata["price_decimals"] = message[9]
                    this_metadata["delisting_date"] = message[10]
                    this_metadata["delisting_time"] = message[11]

            # Price Tick Size message
            elif message_type == b"L":
                message = self.unpack(">iiii", message)
                # timestamp = self.microseconds + message[0] * 1e-3
                price_tick_table_id = message[1]
                this_tick_size_table = self.price_tick_sizes[price_tick_table_id]
                # price_tick_size = message[2]
                # price_start = message[3]
                this_tick_size_table[message[2]] = message[3]

            # Quantity Tick Size message
            elif message_type == b"M":
                message = self.unpack(">iiii", message)
                # timestamp = self.microseconds + message[0] * 1e-3
                quantity_tick_table_id = message[1]
                quantity_tick_size = message[2]
                quantity_start = message[3]

            else:
                pass  # because message type is not relevant

            # update current position for next iteration
            self.current_position = message_end

            # # Indicative Price / Quantity Message
            # elif message_type == b"I":
            #     # message = self.unpack(">iqiiiis", message)
            #     pass # not relevant

            # # Trade message (SwissAtMid / EBBO)
            # elif message_type == b"P":
            #     message = self.unpack(">iiiiqs", message)
            #     timestamp = self.microseconds + message[0] * 1e-3
            #     orderbook_no = message[1]
            #     executed_quantity = message[2]
            #     execution_price = message[3]
            #     match_number = message[4]
            #     book_type = message[5]

            # # Broken Trade message
            # elif message_type == b"B":
            #     message = self.unpack(">iqs", message)
            #     timestamp = self.microseconds + message[0] * 1e-3
            #     match_number = message[1]
            #     reason = message[2]

            # # Orderbook Trading Action message
            # elif message_type == b"H":
            #     message = self.unpack(">iiss", message)
            #     timestamp = self.microseconds + message[0] * 1e-3
            #     orderbook_no = message[1]
            #     trading_state = message[2]
            #     book_condition = message[3]

            # # System Event message
            # elif message_type == b"S":
            #     message = self.unpack(">i8ssi", message)
            #     timestamp = self.microseconds + message[0] * 1e-3
            #     group = message[1]
            #     event_code = message[2]
            #     orderbook_no = message[3]

            # elif message_type == b"G":  # not relevant
            #     pass

            # else:
            #     raise ValueError(f"Message type {message_type} could not be found")

            # # update current position for next iteration
            # self.current_position = message_end
