#!/usr/bin/env python

import zmq
import json

# --- Configuration ---
# The address of the ZMQ PUB server to connect to
ZMQ_PUB_ADDRESS = "tcp://localhost:5555"
# The topic to subscribe to. An empty string "" subscribes to all topics.
# You can specify a particular topic, e.g., "/sensor/camera"
ZMQ_TOPIC = ""

if __name__ == "__main__":
    """
    Connects a ZMQ SUB socket and prints received messages.

    This script subscribes to a ZMQ PUB server, listening for messages
    on a specific topic. It expects multipart messages containing a topic
    and a JSON payload.
    """

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(ZMQ_PUB_ADDRESS)

    # Subscribe to the specified topic.
    socket.setsockopt_string(zmq.SUBSCRIBE, ZMQ_TOPIC)

    if ZMQ_TOPIC == "":
        print(f"✅ ZMQ SUB client connected to {ZMQ_PUB_ADDRESS}, subscribing to ALL topics.")
    else:
        print(f"✅ ZMQ SUB client connected to {ZMQ_PUB_ADDRESS}, subscribing to topic: '{ZMQ_TOPIC}'")

    print("Waiting to receive messages...")

    try:
        while True:
            # Block and wait to receive a multipart message
            topic_bytes, message_str = socket.recv_multipart()
            topic = topic_bytes.decode('utf-8')

            # Try to pretty-print the JSON for readability
            try:
                message_obj = json.loads(message_str)
                print(f"\n--- Received Message on Topic: {topic} ---")
                print(json.dumps(message_obj, indent=2))
            except json.JSONDecodeError:
                print(f"\n--- Received Non-JSON String on Topic: {topic} ---")
                print(message_str)

    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Shutting down.")
    finally:
        socket.close()
        context.term()
