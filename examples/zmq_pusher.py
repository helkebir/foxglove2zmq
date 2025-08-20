#!/usr/bin/env python

import zmq
import json
import time
from datetime import datetime, timezone

# --- Configuration ---
# The address of the ZMQ PULL server to connect to (the relay's listening address)
ZMQ_PUSH_ADDRESS = "tcp://localhost:5556"

if __name__ == "__main__":
    """
    Connects a ZMQ PUSH socket and sends periodic messages.

    This script demonstrates how to send a message to the relay's listening
    socket, which will then publish it to the Foxglove server. It sends a
    dummy FrameTransform message every second.
    """

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(ZMQ_PUSH_ADDRESS)
    print(f"✅ ZMQ PUSH client connected to {ZMQ_PUSH_ADDRESS}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            # Get the current time in UTC and format it as an RFC 3339 string
            # with a 'Z' suffix, as expected by the Protobuf JSON parser for timestamps.
            now_utc = datetime.now(timezone.utc)
            timestamp_str = now_utc.isoformat().replace('+00:00', 'Z')

            # 1. Construct the message payload according to the schema
            frame_transform_payload = {
                "timestamp": timestamp_str,
                "parent_frame_id": "",
                "child_frame_id": "world",
                "translation": {
                    "x": 1.0,
                    "y": 2.0,
                    "z": 3.0
                },
                "rotation": {
                    "x": 0.0,
                    "y": 0.0,
                    "z": 0.0,
                    "w": 1.0
                }
            }

            # 2. Wrap the payload in the required structure with a topic
            message_to_send = {
                "topic": "/tf/world",
                "payload": frame_transform_payload
            }

            # 3. Convert to a JSON string and send
            message_str = json.dumps(message_to_send)
            socket.send_string(message_str)

            print(f"Sent message to topic '{message_to_send['topic']}'")

            # Wait for a second before sending the next message
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Shutting down.")
    finally:
        socket.close()
        context.term()
