#!/usr/bin/env python

import asyncio
import json
import zmq
import zmq.asyncio
import websockets
import base64
import struct

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, json_format

# Opcodes for messages received from the Foxglove server
OP_MESSAGE_DATA = 0x01
OP_TIME = 0x02
OP_CODES_SUPPORTED = [OP_MESSAGE_DATA, OP_TIME]

# Opcodes for messages sent to the Foxglove server
OP_CLIENT_PUBLISH = 0x01
OP_SERVICE_CALL_REQUEST = 0x02

ENCODINGS_SUPPORTED = ["json", "protobuf"]
CAPABILITIES_SUPPORTED = ["time", "clientPublish"]


class FoxgloveToZMQRelay:
    """
    Connects to a Foxglove WebSocket server, decodes/relays messages to a ZMQ server,
    and listens on a ZMQ socket to publish messages back to the Foxglove server.
    This is a base class.
    """

    def __init__(self, foxglove_address, zmq_address, zmq_listen_address=None, topic_blocklist=None,
                 discovery_timeout=2.0, verbosity=1):
        """
        Initializes the relay.

        Args:
            foxglove_address (str): The WebSocket URL of the Foxglove server.
            zmq_address (str): The TCP address for the outgoing ZMQ server to bind to.
            zmq_listen_address (str, optional): The TCP address for the incoming ZMQ server to bind to. Defaults to None.
            topic_blocklist (list[str], optional): A list of topics to ignore. Defaults to None.
            discovery_timeout (float, optional): Time in seconds to wait for channel advertisements. Defaults to 2.0.
            verbosity (int, optional): The verbosity level. Defaults to 1 (basic connection message).
        """
        self.foxglove_address = foxglove_address
        self.zmq_address = zmq_address
        self.zmq_listen_address = zmq_listen_address
        self.topic_blocklist = set(topic_blocklist or [])
        self.discovery_timeout = discovery_timeout
        self.verbosity = verbosity

        # ZMQ state
        self.context = zmq.asyncio.Context.instance()
        self.zmq_socket = None  # For sending messages out
        self.zmq_listen_socket = None  # For receiving messages in

        # Connection, channel, parameter, and service state
        self.websocket = None
        self.channels_by_id = {}
        self.id_by_channel_topics = {}
        self.subscriptions = {}
        self.protobuf_decoders = {}
        self.parameters_by_id = {}
        self.client_advertised_topics = set()  # Tracks topics advertised by this client

        self.services_by_id = {}
        self.protobuf_service_request_encoders = {}
        self.protobuf_service_response_decoders = {}

        # Foxglove server capabilities
        self.has_clientPublish = False
        self.has_parameters = False
        self.has_parametersSubscribe = False
        self.has_time = False
        self.has_services = False
        self.has_connectionGraph = False
        self.has_assets = False

    def _init_zmq(self):
        """Initializes the specific ZMQ socket for sending. Must be implemented by a subclass."""
        raise NotImplementedError("Subclasses must implement _init_zmq")

    def _init_zmq_listener(self):
        """Initializes the ZMQ socket for listening."""
        if self.zmq_listen_address:
            if self.verbosity >= 1:
                print("🔧 Initializing ZMQ PULL socket for listening.")
            self.zmq_listen_socket = self.context.socket(zmq.PULL)
            self.zmq_listen_socket.bind(self.zmq_listen_address)

    async def _send_msg(self, msg, topic=None):
        """Sends a message via the outgoing ZMQ socket. Must be implemented by a subclass."""
        raise NotImplementedError("Subclasses must implement _send_msg")

    async def _process_connection(self):
        """Processes the Foxglove websocket connection by checking server capabilities."""
        if self.verbosity >= 1:
            print("📶 Parsing websocket connection...")
        await self._parse_server_info()

    def _has_capabilities(self, capability):
        """Checks if the server is known to have a capability."""
        return capability in self.server_capabilities

    def _update_capabilities(self):
        for capability in self.server_capabilities:
            if capability == "clientPublish": self.has_clientPublish = True
            if capability == "parameters": self.has_parameters = True
            if capability == "parametersSubscribe": self.has_parametersSubscribe = True
            if capability == "time": self.has_time = True
            if capability == "services": self.has_services = True
            if capability == "connectionGraph": self.has_connectionGraph = True
            if capability == "assets": self.has_assets = True

    async def _parse_server_info(self):
        """Parses Foxglove server information."""
        print(f"🔎 Scanning Foxglove server connection for {self.discovery_timeout} seconds...")
        start_time = asyncio.get_running_loop().time()
        while (asyncio.get_running_loop().time() - start_time) < self.discovery_timeout:
            try:
                remaining_time = self.discovery_timeout - (asyncio.get_running_loop().time() - start_time)
                if remaining_time <= 0: break
                message = await asyncio.wait_for(self.websocket.recv(), timeout=remaining_time)
                data = json.loads(message)
                if data.get("op") == "advertise": self._process_channels(data)
                if data.get("op") == "advertiseServices": self._process_services(data)
                if data.get("op") == "serverInfo": self._process_server_info(data)
            except asyncio.TimeoutError:
                break
        await self._subscribe_to_channels()
        print("✅ Server scanning phase complete.")

    def _process_server_info(self, data):
        if self.verbosity >= 2: print(f"🔎 Parsing server information...")
        if data.get("op") == "serverInfo":
            self.server_info = data
            self.server_name = data.get("name", "")
            self.server_capabilities = data.get("capabilities")
            self.supported_encodings = data.get("supportedEncodings")
            self._update_capabilities()
            if self.verbosity >= 2:
                if self.server_name: print(f"   - Found server name: '{self.server_name}'")
                if self.supported_encodings:
                    print("   - Found the following supported encodings:")
                    for encoding in self.supported_encodings:
                        print(f"     - {encoding} {'(✅)' if encoding in ENCODINGS_SUPPORTED else '(⚠️)'}")
                if self.server_capabilities:
                    print("   - Found the following server capabilities:")
                    for capability in self.server_capabilities:
                        print(f"     - {'✅' if capability in CAPABILITIES_SUPPORTED else '⚠️'} {capability}")
        if self.verbosity >= 2: print("✅ Server info parsing phase complete.")

    def _process_channels(self, data):
        if self.verbosity >= 2: print("👂 Parsing channel advertisements...")
        if data.get("op") == "advertise":
            for channel in data.get("channels", []): self._process_advertised_channel(channel)
            if not data.get("channels", []): print("⚠️ No channels were advertised by the server...")
        if self.verbosity >= 2: print("✅ Channel discovery phase complete.")

    def _process_services(self, data):
        if self.verbosity >= 2: print("👂 Parsing service advertisements...")
        if data.get("op") == "advertiseServices":
            for service in data.get("services", []): self._process_advertised_service(service)
            if not data.get("services", []): print("⚠️ No services were advertised by the server...")
        if self.verbosity >= 2: print("✅ Service discovery phase complete.")

    def _in_blocklist(self, topic):
        for block in self.topic_blocklist:
            if "*" in block:
                if topic.startswith(block.split("*")[0]): return True
            elif topic == block:
                return True
        return False

    def _process_advertised_service(self, service):
        service_id, name = service.get("id"), service.get("name")
        if self._in_blocklist(name):
            if self.verbosity >= 2: print(f"   - 🚫 Ignoring blocklisted service: '{name}'")
            return
        if service_id not in self.services_by_id:
            self.services_by_id[service_id] = service
            if self.verbosity >= 2: print(f"   - Discovered service: '{name}' (ID: {service_id})")
            if service.get("request", {}).get("encoding") == "protobuf": raise NotImplementedError(
                "Protobuf service request encoding to be implemented.")
            if service.get("response", {}).get("encoding") == "protobuf": raise NotImplementedError(
                "Protobuf service response decoding to be implemented.")

    def _process_advertised_channel(self, channel):
        chan_id, topic = channel.get("id"), channel.get("topic")
        if self._in_blocklist(topic):
            if self.verbosity >= 2: print(f"   - 🚫 Ignoring blocklisted topic: '{topic}'")
            return
        if chan_id not in self.channels_by_id:
            self.id_by_channel_topics[topic] = chan_id
            self.channels_by_id[chan_id] = channel
            if self.verbosity >= 2: print(f"   - Discovered topic: '{topic}' (ID: {chan_id})")
            if channel.get("encoding") == "protobuf": self._prepare_protobuf_decoder(channel)

    def _prepare_protobuf_decoder(self, channel):
        chan_id, schema_name = channel["id"], channel["schemaName"]
        if self.verbosity >= 2: print(f"     - Preparing Protobuf decoder for schema '{schema_name}'")
        try:
            pool = descriptor_pool.DescriptorPool()
            fds = descriptor_pb2.FileDescriptorSet.FromString(base64.b64decode(channel['schema']))
            for fd in fds.file: pool.Add(fd)
            descriptor = pool.FindMessageTypeByName(schema_name)
            self.protobuf_decoders[chan_id] = message_factory.GetMessageClass(descriptor)
            if self.verbosity >= 2: print(f"     - Successfully prepared decoder.")
        except Exception as e:
            print(f"     - ❌ Failed to prepare Protobuf decoder for channel {chan_id}: {e}")

    async def _subscribe_to_channels(self):
        if not self.channels_by_id: return
        sub_requests = [{"id": i, "channelId": chan_id} for i, chan_id in enumerate(self.channels_by_id.keys())]
        self.subscriptions = {i: self.channels_by_id[chan_id] for i, chan_id in enumerate(self.channels_by_id.keys())}
        await self.websocket.send(json.dumps({"op": "subscribe", "subscriptions": sub_requests}))
        print(f"📢 Sent subscription request for {len(sub_requests)} channels.")

    async def _advertise_client_channel(self, channel_info):
        """Sends a client 'advertise' message to the server for a given channel."""
        if self.verbosity >= 1:
            print(f"📢 Advertising client channel for topic '{channel_info['topic']}'...")

        # Construct the channel object for the advertisement.
        # We can reuse most of the info the server sent us.
        advertised_channel = {
            "id": channel_info["id"],
            "topic": channel_info["topic"],
            "encoding": channel_info["encoding"],
            "schemaName": channel_info["schemaName"],
            "schema": channel_info["schema"],
            "schemaEncoding": channel_info["schemaEncoding"],
        }

        # The schema itself is optional for the client to send if the server already knows it
        # For simplicity, we won't resend the schema.

        message = {
            "op": "advertise",
            "channels": [advertised_channel]
        }

        await self.websocket.send(json.dumps(message))

    async def _process_foxglove_messages(self):
        """The main loop to receive, decode, and relay messages from Foxglove to ZMQ."""
        async for message_bytes in self.websocket:
            opcode = message_bytes[0]
            if opcode not in OP_CODES_SUPPORTED: continue
            try:
                if opcode == OP_MESSAGE_DATA:
                    sub_id = struct.unpack('<I', message_bytes[1:5])[0]
                    timestamp = struct.unpack('<Q', message_bytes[5:13])[0]
                    binary_payload_bytes = message_bytes[13:]
                    channel_info = self.subscriptions.get(sub_id)
                    if not channel_info:
                        if self.verbosity >= 1: print(
                            f"⚠️ Received message for unknown subscription ID {sub_id}, skipping.")
                        continue
                    payload_obj = self._decode_payload(channel_info, binary_payload_bytes)
                    if payload_obj is None: continue
                    topic = channel_info.get("topic", "unknown_topic")
                    message_to_send = json.dumps({
                        "topic": topic,
                        "type": channel_info.get("schemaName", "unknown_type"),
                        "timestamp": timestamp,
                        "payload": payload_obj
                    })
                elif opcode == OP_TIME:
                    timestamp = struct.unpack('<Q', message_bytes[1:9])[0]
                    topic = "/internal/time"
                    message_to_send = json.dumps(
                        {"topic": topic, "type": "time", "timestamp": timestamp, "payload": {}})

                if self.verbosity >= 2: print(f"Relaying message from Foxglove topic '{topic}' to ZMQ...")
                await self._send_msg(message_to_send, topic=topic)
            except (struct.error, IndexError):
                if self.verbosity >= 1: print(f"⚠️ Skipping message: Malformed binary frame received.")
            except json.JSONDecodeError:
                if self.verbosity >= 1: print(
                    f"⚠️ Skipping message on topic '{channel_info.get('topic', 'unknown')}': Payload is not valid JSON.")
            except Exception as e:
                print(
                    f"❌ An unexpected error occurred while processing a message from topic '{channel_info.get('topic', 'unknown')}': {e}")

    async def _listen_for_zmq_messages(self):
        """The main loop to receive messages from ZMQ and publish them to Foxglove."""
        if not self.zmq_listen_socket:
            if self.verbosity >= 1: print("👂 ZMQ listener not configured. Skipping.")
            return

        if not self.has_clientPublish:
            print("⚠️ Server does not support 'clientPublish' capability. Cannot publish messages from ZMQ.")
            return

        if self.verbosity >= 1: print(f"👂 Listening for incoming ZMQ messages on {self.zmq_listen_address}...")
        while True:
            try:
                message_str = await self.zmq_listen_socket.recv_string()
                data = json.loads(message_str)
                topic = data.get("topic")
                payload = data.get("payload")

                if not topic or payload is None:
                    if self.verbosity >= 1: print(f"⚠️ Received invalid ZMQ message: missing 'topic' or 'payload'.")
                    continue

                channel_id = self.id_by_channel_topics.get(topic)
                if channel_id is None:
                    if self.verbosity >= 1: print(
                        f"⚠️ Received ZMQ message for unknown topic '{topic}'. Cannot publish.")
                    continue

                channel_info = self.channels_by_id.get(channel_id)
                if not channel_info:
                    if self.verbosity >= 1: print(f"⚠️ Could not find channel info for topic '{topic}'.")
                    continue

                # Advertise the channel if this is the first time we're publishing to it.
                if topic not in self.client_advertised_topics:
                    await self._advertise_client_channel(channel_info)
                    self.client_advertised_topics.add(topic)

                payload_bytes = self._encode_payload(channel_info, payload)
                if payload_bytes is None:
                    continue

                # Construct the binary message: [opcode][channel_id][payload]
                header = struct.pack('<BI', OP_CLIENT_PUBLISH, channel_id)
                await self.websocket.send(header + payload_bytes)

                if self.verbosity >= 2: print(f"Publishing message from ZMQ to Foxglove topic '{topic}'...")

            except json.JSONDecodeError:
                if self.verbosity >= 1: print("⚠️ Received non-JSON message from ZMQ. Skipping.")
            except Exception as e:
                print(f"❌ An unexpected error occurred while processing a ZMQ message: {e}")

    def _decode_payload(self, channel_info, binary_payload):
        """Decodes a message payload based on its encoding (JSON or Protobuf)."""
        encoding, topic = channel_info.get("encoding"), channel_info.get("topic")
        if encoding == "protobuf":
            chan_id = channel_info.get("id")
            message_class = self.protobuf_decoders.get(chan_id)
            if message_class:
                proto_message = message_class()
                proto_message.ParseFromString(binary_payload)
                return json.loads(json_format.MessageToJson(proto_message, preserving_proto_field_name=True))
            else:
                if self.verbosity >= 1: print(
                    f"⚠️ Skipping Protobuf message on topic '{topic}': Decoder not available.")
                return None
        elif encoding == "json":
            return json.loads(binary_payload.decode('utf-8'))
        else:
            if self.verbosity >= 1: print(f"⚠️ Skipping message on topic '{topic}': Unsupported encoding '{encoding}'.")
            return None

    def _encode_payload(self, channel_info, payload_obj):
        """Encodes a JSON payload object to binary based on the channel's encoding."""
        encoding, topic = channel_info.get("encoding"), channel_info.get("topic")

        if encoding == "json":
            return json.dumps(payload_obj).encode('utf-8')

        elif encoding == "protobuf":
            chan_id = channel_info.get("id")
            message_class = self.protobuf_decoders.get(chan_id)
            if message_class:
                try:
                    proto_message = message_class()
                    json_format.Parse(json.dumps(payload_obj), proto_message)
                    return proto_message.SerializeToString()
                except Exception as e:
                    if self.verbosity >= 1: print(f"❌ Failed to encode Protobuf message for topic '{topic}': {e}")
                    return None
            else:
                if self.verbosity >= 1: print(
                    f"⚠️ Skipping Protobuf message on topic '{topic}': Encoder not available.")
                return None
        else:
            if self.verbosity >= 1: print(
                f"⚠️ Skipping message on topic '{topic}': Unsupported encoding for publishing '{encoding}'.")
            return None

    async def run(self):
        """Connects to servers and starts the bidirectional message relay loops."""
        self._init_zmq()
        self._init_zmq_listener()

        try:
            async with websockets.connect(self.foxglove_address, subprotocols=["foxglove.sdk.v1"],
                                          max_size=None) as websocket:
                self.websocket = websocket
                if self.verbosity >= 1: print(f"✅ Connected to Foxglove WebSocket at {self.foxglove_address}")
                await self._process_connection()

                # Run both listeners concurrently
                await asyncio.gather(
                    self._process_foxglove_messages(),
                    self._listen_for_zmq_messages()
                )
        except (ConnectionRefusedError, websockets.exceptions.ConnectionClosed) as e:
            print(f"❌ Connection to Foxglove server at {self.foxglove_address} failed: {e}")
        except asyncio.CancelledError:
            print("🔌 Task was cancelled, shutting down.")
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")
        finally:
            self.close()

    def close(self):
        """Cleans up ZMQ resources."""
        if self.verbosity >= 1: print("🧹 Cleaning up ZMQ sockets and context.")
        if self.zmq_socket and not self.zmq_socket.closed: self.zmq_socket.close()
        if self.zmq_listen_socket and not self.zmq_listen_socket.closed: self.zmq_listen_socket.close()
        if not self.context.closed: self.context.term()


class FoxgloveToZMQPushRelay(FoxgloveToZMQRelay):
    """A relay that uses a ZMQ PUSH socket for sending and a PULL socket for listening."""

    def _init_zmq(self):
        if self.verbosity >= 1: print("🔧 Initializing ZMQ PUSH socket for sending.")
        self.zmq_socket = self.context.socket(zmq.PUSH)
        self.zmq_socket.bind(self.zmq_address)
        if self.verbosity >= 1: print(f"✅ Connected to ZMQ PUSH socket at {self.zmq_address}")

    async def _send_msg(self, msg, topic=None):
        await self.zmq_socket.send_string(msg)


class FoxgloveToZMQPubSubRelay(FoxgloveToZMQRelay):
    """A relay that uses a ZMQ PUB socket for sending and a PULL socket for listening."""

    def _init_zmq(self):
        if self.verbosity >= 1: print("🔧 Initializing ZMQ PUB socket for sending.")
        self.zmq_socket = self.context.socket(zmq.PUB)
        self.zmq_socket.bind(self.zmq_address)
        if self.verbosity >= 1: print(f"✅ Connected to ZMQ PUB socket at {self.zmq_address}")

    async def _send_msg(self, msg, topic=None):
        if topic: await self.zmq_socket.send_multipart([topic.encode('utf-8'), msg.encode('utf-8')])


if __name__ == "__main__":
    relay = FoxgloveToZMQPushRelay(
        foxglove_address="ws://localhost:8765",
        zmq_address="tcp://*:5555",
        zmq_listen_address="tcp://*:5556",  # New address for listening
        topic_blocklist=["/hh/*", "/viper/*"],
        discovery_timeout=2.0,
        verbosity=2,
    )
    try:
        asyncio.run(relay.run())
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Shutting down.")
