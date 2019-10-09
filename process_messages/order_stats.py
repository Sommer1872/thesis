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
        self.get_order_info = itemgetter("orderbook", "order_verb", "order_entry_price",
                                         "order_quantity_outstanding")

        # Reading the binary file into memory
        with open(self.file_path, "rb") as binary_file:
            self.data = binary_file.read()
        self.number_of_bytes = len(self.data)

    def process_messages(self):
        """Convert and process all messages inside a loop"""

        # first check if we are at the end of the file, if so: stop
        while self.current_position < self.number_of_bytes:

            message_length = self.data[self.current_position + 1]
            message_type = self.data[self.current_position + 2:self.current_position +
                                     3]
            message_start = self.current_position + 3
            message_end = self.current_position + message_length + 2

            message = self.data[message_start:message_end]

            # Add Order Message
            if message_type == b"A":
                message = unpack(">iqsiii", message)
                orderbook = message[4]
                if orderbook in blue_chip_orderbooks:
                    order_no = message[1]
                    order_verb = message[2]
                    order_quantity = message[3]
                    order_price = message[5]
                    # aggregate with best bid/ask in orderbook
                    # best_bid_price, best_bid_quantity = self.orderbooks[orderbook][b'B'].peekitem(0)
                    # best_ask_price, best_ask_quantity = self.orderbooks[orderbook][b'S'].peekitem(0)
                    self.orders[order_no]["order_entry_time"] = (self.microseconds +
                                                                 message[0] * 1e-3)
                    self.orders[order_no]["order_verb"] = order_verb
                    self.orders[order_no]["order_quantity_entered"] = order_quantity
                    self.orders[order_no]["order_quantity_outstanding"] = order_quantity
                    self.orders[order_no]["orderbook"] = orderbook
                    self.orders[order_no]["order_entry_price"] = order_price
                    self.orders[order_no]["order_filled_price"] = np.nan
                    self.orders[order_no]["order_remove_time"] = np.nan
                    self.orders[order_no]["status"] = "open"
                    self.orders[order_no]["best_bid_quantity"] = best_bid_quantity
                    self.orders[order_no]["best_bid_price"] = best_bid_price
                    self.orders[order_no]["best_ask_price"] = best_ask_price
                    self.orders[order_no]["best_ask_quantity"] = best_ask_quantity
                    # update the side (order_verb) of the orderbook
                    self.orderbooks[orderbook][order_verb][
                        order_price] += order_quantity

            # Orderbook Directory message
            elif message_type == b"R":
                message = unpack(">iis12s3s8siiiiii", message)
                orderbook = message[1]
                # initialize each side of the orderbook
                self.orderbooks[orderbook][b"B"] = OrderBookSide(neg)
                self.orderbooks[orderbook][b"S"] = OrderBookSide()
                self.orderbooks[orderbook][b" "] = OrderBookSide()

            # Time Stamp – Seconds message
            elif message_type == b"T":
                message = unpack(">i", message)
                self.microseconds = int(message[0] * 1e6)

            # Order Delete Message
            elif message_type == b"D":
                message = unpack(">iq", message)
                order_no = message[1]
                # update the order entry
                self.orders[order_no]["order_remove_time"] = (self.microseconds +
                                                              message[0] * 1e-3)
                self.orders[order_no]["order_quantity_outstanding"] = 0
                if self.orders[order_no]["status"] is None:
                    self.orders[order_no]["status"] = "deleted"
                elif self.orders[order_no]["status"] == "partially filled":
                    self.orders[order_no]["status"] = "partially filled & deleted"
                else:
                    self.orders[order_no]["status"] = "deleted"
                # # update the order book
                # orderbook, order_verb, order_price, quantity_outstanding = self.get_order_info(self.orders[order_no])
                # self.orderbooks[orderbook][order_verb][order_price] -= quantity_outstanding
                # if self.orderbooks[orderbook][order_verb][order_price] == 0:
                #     del self.orderbooks[orderbook][order_verb][order_price]

            # Order Replace Message
            elif message_type == b"U":
                message = unpack(">iqqii", message)
                timestamp = self.microseconds + message[0] * 1e-3
                old_order_no = message[1]
                new_order_no = message[2]
                order_quantity = message[3]
                order_price = message[4]
                order_verb = self.orders[old_order_no]["order_verb"]
                orderbook = self.orders[old_order_no]["orderbook"]
                # aggregate with best bid/ask in orderbook
                best_bid_price, best_bid_quantity = self.orderbooks[orderbook][
                    b"B"].peekitem(0)
                best_ask_price, best_ask_quantity = self.orderbooks[orderbook][
                    b"S"].peekitem(0)
                self.orders[new_order_no]["order_entry_time"] = (self.microseconds +
                                                                 message[0] * 1e-3)
                self.orders[new_order_no]["order_verb"] = order_verb
                self.orders[new_order_no]["order_quantity_entered"] = order_quantity
                self.orders[new_order_no]["order_quantity_outstanding"] = order_quantity
                self.orders[new_order_no]["orderbook"] = orderbook
                self.orders[new_order_no]["order_entry_price"] = order_price
                self.orders[new_order_no]["order_filled_price"] = np.nan
                self.orders[new_order_no]["order_remove_time"] = np.nan
                self.orders[new_order_no]["status"] = "open"
                self.orders[new_order_no]["best_bid_quantity"] = best_bid_quantity
                self.orders[new_order_no]["best_bid_price"] = best_bid_price
                self.orders[new_order_no]["best_ask_price"] = best_ask_price
                self.orders[new_order_no]["best_ask_quantity"] = best_ask_quantity
                # mark old order as replaced
                self.orders[old_order_no].update({
                    "order_remove_time": timestamp,
                    "status": "replaced"
                })
                # # adjust orderbook
                # # new order
                # this_orderbook = self.orderbooks[orderbook][order_verb]
                # this_orderbook[order_price] += order_quantity
                # # old order
                # old_order_price = self.orders[old_order_no]["order_entry_price"]
                # this_orderbook[old_order_price] -= self.orders[old_order_no]["order_quantity_outstanding"]
                # if this_orderbook[old_order_price] == 0:
                #     del this_orderbook[old_order_price]

            # Order Executed Message
            elif message_type == b"E":
                message = unpack(">iqiq", message)
                timestamp = self.microseconds + message[0] * 1e-3
                order_no = message[1]
                executed_quantity = message[2]
                match_number = message[3]
                # update the order entry
                this_order = self.orders[order_no]
                this_order.update({
                    "order_remove_time": timestamp,
                    "order_filled_price": self.orders[order_no]["order_entry_price"],
                })
                this_order["order_quantity_outstanding"] -= executed_quantity
                if this_order["order_quantity_outstanding"] == 0:
                    this_order.update({
                        "status": "filled",
                        "order_remove_time": timestamp
                    })
                else:
                    this_order["status"] = "partially filled"
                # # update the order book
                # orderbook, order_verb, order_price, _ = self.get_order_info(self.orders[order_no])
                # this_orderbook = self.orderbooks[orderbook][order_verb]
                # this_orderbook[order_price] -= executed_quantity
                # if this_orderbook[order_price] == 0:
                #     del this_orderbook[order_verb][order_price]

            # Order Executed With Price message
            elif message_type == b"C":
                message = unpack(">iqiqsi", message)
                timestamp = self.microseconds + message[0] * 1e-3
                order_no = message[1]
                executed_quantity = message[2]
                match_number = message[3]
                printable = message[4]
                execution_price = message[5]
                # update the order entry
                this_order = self.orders[order_no]
                this_order.update({
                    "order_remove_time": timestamp,
                    "order_filled_price": execution_price,
                })
                this_order["order_quantity_outstanding"] -= executed_quantity
                if this_order["order_quantity_outstanding"] == 0:
                    this_order["status"] = "filled"
                    self.orders[old_order_no]["order_remove_time"] = timestamp
                else:
                    this_order["status"] = "partially filled"
                # # update the order book
                # orderbook, order_verb, order_price, _ = self.get_order_info(self.orders[order_no])
                # this_orderbook = self.orderbooks[orderbook][order_verb]
                # this_orderbook[order_price] -= executed_quantity
                # if this_orderbook[order_price] == 0:
                #     del this_orderbook[order_verb][order_price]

            # Indicative Price / Quantity Message
            elif message_type == b"I":
                # message = unpack(">iqiiiis", message)
                pass  # not relevant

            # Trade message (SwissAtMid / EBBO)
            elif message_type == b"P":
                pass
                # message = unpack(">iiiiqs", message)
                # timestamp = self.microseconds + message[0] * 1e-3
                # orderbook = message[1]
                # executed_quantity = message[2]
                # execution_price = message[3]
                # match_number = message[4]
                # book_type = message[5]

            # Broken Trade message
            elif message_type == b"B":
                message = unpack(">iqs", message)
                timestamp = self.microseconds + message[0] * 1e-3
                match_number = message[1]
                reason = message[2]

            # Orderbook Trading Action message
            elif message_type == b"H":
                message = unpack(">iiss", message)
                timestamp = self.microseconds + message[0] * 1e-3
                orderbook = message[1]
                trading_state = message[2]
                book_condition = message[3]

            # Price Tick Size message
            elif message_type == b"L":
                message = unpack(">iiii", message)
                timestamp = self.microseconds + message[0] * 1e-3
                price_tick_table_id = message[1]
                price_tick_size = message[2]
                price_start = message[3]

            # System Event message
            elif message_type == b"S":
                message = unpack(">i8ssi", message)
                timestamp = self.microseconds + message[0] * 1e-3
                group = message[1]
                event_code = message[2]
                orderbook = message[3]

            # Quantity Tick Size message
            elif message_type == b"M":
                message = unpack(">iiii", message)
                timestamp = self.microseconds + message[0] * 1e-3
                quantity_tick_table_id = message[1]
                quantity_tick_size = message[2]
                quantity_start = message[3]

            elif message_type == b"G":  # not relevant
                pass

            else:
                raise ValueError(f"Message type {message_type} could not be found")

            # update current position for next iteration
            self.current_position = message_end


def main():
    pass


if __name__ == "__main__":
    main()
