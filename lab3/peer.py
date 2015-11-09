#!/usr/bin/env python3

# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 28 January 2015
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""A simple peer implementation.

This implementation simply connects to the name service and asks for the
list of peers.
"""

import sys
import random
import socket
import argparse
import logging
sys.path.append("../modules")
from Common import orb
from Common.nameServiceLocation import name_service_address
from Common.objectType import object_type

# -----------------------------------------------------------------------------
# Initialize and read the command line arguments
# -----------------------------------------------------------------------------

rand = random.Random()
rand.seed()
description = """Simple peer."""
parser = argparse.ArgumentParser(description=description)
parser.add_argument(
    "-p", "--port", metavar="PORT", dest="port", type=int,
    default=rand.randint(1, 10000) + 40000, choices=range(40001, 50000),
    help="Set the port to listen to. Must be in the range 40001 .. 50000."
         "The default value is chosen at random."
)
parser.add_argument(
    "-t", "--type", metavar="TYPE", dest="type", default=object_type,
    help="Set the type of the client."
)
parser.add_argument(
    "-l", "--log-level", metavar="LEVEL", dest="log_level", default="INFO",
    help="The detail level of the logs, from DEBUG to CRITICAL. The default value is INFO.")

opts = parser.parse_args()

numeric_level = getattr(logging, opts.log_level.upper(), None)
if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % opts.log_level)
logging.basicConfig(format="%(levelname)s:%(filename)s: %(message)s", level=numeric_level)

local_port = opts.port
client_type = opts.type
assert client_type != "object", "Change the object type to something unique!"

# -----------------------------------------------------------------------------
# Auxiliary classes
# -----------------------------------------------------------------------------


class Client(orb.Peer):
    """Chat client class."""

    def __init__(self, local_address, ns_address, client_type):
        """Initialize the client."""

        logging.debug("Client initializing...")
        logging.debug("Client initializing Peer...")
        orb.Peer.__init__(self, local_address, ns_address, client_type)
        logging.debug("Client done initializing Peer!")
        logging.debug("Client starting Peer...")
        orb.Peer.start(self)
        logging.debug("Client done starting Peer!")
        logging.debug("Client done initializing!")

    # Public methods

    def destroy(self):
        """Destroy the peer object."""
        orb.Peer.destroy(self)

    def display_peers(self):
        """Display all the peers in the list."""
        peers = self.name_service.require_all(self.type)
        logging.info("List of peers of type '{0}':".format(self.type))
        for pid, paddr in peers:
            logging.info("    id: {:>2}, address: {}".format(pid, tuple(paddr)))

# -----------------------------------------------------------------------------
# The main program
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    
    # Initialize the client object.
    local_address = (socket.gethostname(), local_port)
    p = Client(local_address, name_service_address, client_type)
    
    logging.info("""\
    This peer:
        id: {:>2}, address: {}""".format(p.id, p.address))
    
    try:
        # Print the list of peers.
        p.display_peers()
    
        # Waiting for a key press.
        sys.stdout.write("Waiting for a key press...")
        input()
    except KeyboardInterrupt as e:
        logging.warning("Received interrupt {}".format(e))
    finally:
        # Kill our peer object.
        p.destroy()
        
    logging.info("All done!")
