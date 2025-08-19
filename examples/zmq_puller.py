#!/usr/bin/env python

import zmq
import json

# --- Configuration ---
# The address of the ZMQ PUSH server to connect to
ZMQ_PULL_ADDRESS = "tcp://localhost:5555"

if __name__ == "__main__":
    """
    Connects a ZMQ PULL socket and prints received messages.
    
    This script listens for messages sent by a ZMQ PUSH server and prints them to the console.
    It can handle both JSON messages and plain text strings, attempting to pretty-print JSON for readability
    while printing plain text strings as-is.
    """

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect(ZMQ_PULL_ADDRESS)
    print(f"✅ ZMQ PULL client connected to {ZMQ_PULL_ADDRESS}")
    print("Waiting to receive messages...")

    try:
        while True:
            # Block and wait to receive a message string
            message_str = socket.recv_string()

            # Try to pretty-print the JSON for readability
            try:
                message_obj = json.loads(message_str)
                print("\n--- Received Message ---")
                print(json.dumps(message_obj, indent=2))
            except json.JSONDecodeError:
                print("\n--- Received Non-JSON String ---")
                print(message_str)

    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Shutting down.")
    finally:
        socket.close()
        context.term()
