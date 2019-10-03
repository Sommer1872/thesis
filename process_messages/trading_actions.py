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


class SingleDayIMIData(object):
    """Class that loads and processes IMI messages for a single date"""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.date = file_path.name[11:21].replace("_", "-")
        self.current_position = 0


        self.trading_actions = list()
        self.unpack = struct.unpack

        # Reading the binary file into memory
        with open(self.file_path, "rb") as binary_file:
            self.data = binary_file.read()
        self.number_of_bytes = len(self.data)

    def process_messages(self):
        """Convert and process all messages inside a loop"""

        self.microseconds = 0

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

            # Time Stamp – Seconds message
            if message_type == b"T":
                message = self.unpack(">i", message)
                seconds = message[0]
                self.microseconds = int(seconds * 1e6)

            # Orderbook Trading Action message
            elif message_type == b"H":
                message = self.unpack(">iiss", message)
                timestamp = self.microseconds + message[0] * 1e-3
                orderbook_no = message[1]
                trading_state = message[2]
                book_condition = message[3]
                self.trading_actions.append((trading_state, book_condition))

            else:
                pass  # because message type is not relevant

            # update current position for next iteration
            self.current_position = message_end
