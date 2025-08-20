#!/usr/bin/env python

import asyncio
import json
import zmq
import zmq.asyncio
import websockets
import base64
import struct

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, json_format

# Opcodes for the Foxglove binary protocol
OP_MESSAGE_DATA = 0x01
OP_TIME = 0x02

OP_CLIENT_PUBLISH = 0x01
OP_SERVICE_CALL_REQUEST = 0x02

OP_CODES_SUPPORTED = [OP_MESSAGE_DATA, OP_TIME]

ENCODINGS_SUPPORTED = ["json", "protobuf"]
CAPABILITIES_SUPPORTED = ["time", "clientPublish"]


class FoxgloveToZMQRelay:
    """
    Connects to a Foxglove WebSocket server, decodes JSON and Protobuf messages,
    and relays them to a ZMQ server. This is a base class.
    """

    def __init__(self, foxglove_address, zmq_address, topic_blocklist=None, discovery_timeout=2.0, verbosity=1):
        """
        Initializes the relay.

        Args:
            foxglove_address (str): The WebSocket URL of the Foxglove server.
            zmq_address (str): The TCP address for the ZMQ server to bind to.
            topic_blocklist (list[str], optional): A list of topics to ignore. Defaults to None.
            discovery_timeout (float, optional): Time in seconds to wait for channel advertisements. Defaults to 2.0.
            verbosity (int, optional): The verbosity level. Defaults to 1 (basic connection message).

        Notes:
            The verbosity level controls the amount of logging output:
                - Level 0: Errors only
                - Level 1: Basic connection and error messages
                - Level 2: Detailed message relay information
        """
        self.foxglove_address = foxglove_address
        self.zmq_address = zmq_address
        self.topic_blocklist = set(topic_blocklist or [])
        self.discovery_timeout = discovery_timeout
        self.verbosity = verbosity

        # ZMQ state
        self.context = zmq.asyncio.Context.instance()
        self.zmq_socket = None  # To be initialized by subclass

        # Connection, channel, parameter, and service state
        self.websocket = None
        self.channels_by_id = {}
        self.id_by_channel_topics = {}
        self.subscriptions = {}
        self.protobuf_decoders = {}
        self.parameters_by_id = {}

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
        """Initializes the specific ZMQ socket. Must be implemented by a subclass."""
        raise NotImplementedError("Subclasses must implement _init_zmq")

    async def _send_msg(self, msg, topic=None):
        """Sends a message via the ZMQ socket. Must be implemented by a subclass."""
        raise NotImplementedError("Subclasses must implement _send_msg")

    async def _process_connection(self):
        """Processes the Foxglove websocket connection by checking server capabilities.

        - Retrieves server info
        - Discovers channels and prepares message decoders
        - Registers parameters and services
        """
        print("📶 Parsing websocket connection...")
        await self._parse_server_info()


    def _has_capabilities(self, capability):
        """Checks if the server is known to have a capability.

        Args:
            capability (str): The capability to check.

        Notes:
            Supported capabilities are:
                - clientPublish: Allow clients to advertise channels to send data messages to the server
                - parameters: Allow clients to get & set parameters
                - parametersSubscribe: Allow clients to subscribe to parameter changes
                - time: The server may publish binary time messages
                - services: Allow clients to call services
                - connectionGraph: Allow clients to subscribe to updates to the connection graph
                - assets: Allow clients to fetch assets
        """
        return capability in self.server_capabilities

    def _update_capabilities(self):
        for capability in self.server_capabilities:
            if capability == "clientPublish":
                self.has_clientPublish = True
            if capability == "parameters":
                self.has_parameters = True
            if capability == "parametersSubscribe":
                self.has_parametersSubscribe = True
            if capability == "time":
                self.has_time = True
            if capability == "services":
                self.has_services = True
            if capability == "connectionGraph":
                self.has_connectionGraph = True
            if capability == "assets":
                self.has_assets = True


    async def _parse_server_info(self):
        """Parses Foxglove server information."""
        print(f"🔎 Scanning Foxglove server connection for {self.discovery_timeout} seconds...")
        start_time = asyncio.get_running_loop().time()

        while (asyncio.get_running_loop().time() - start_time) < self.discovery_timeout:
            try:
                remaining_time = self.discovery_timeout - (asyncio.get_running_loop().time() - start_time)
                if remaining_time <= 0:
                    break

                message = await asyncio.wait_for(self.websocket.recv(), timeout=remaining_time)
                data = json.loads(message)
                if data.get("op") == "advertise":
                    self._process_channels(data)

                if data.get("op") == "advertiseServices":
                    self._process_services(data)

                if data.get("op") == "serverInfo":
                    self._process_server_info(data)

            except asyncio.TimeoutError:
                break

        await self._subscribe_to_channels()
        print("✅ Server scanning phase complete.")

    def _process_server_info(self, data):
        if self.verbosity >= 2:
            print(f"🔎 Parsing server information...")

        if data.get("op") == "serverInfo":
            self.server_info = data
            self.server_name = data.get("name", "")
            self.server_capabilities = data.get("capabilities")
            self.supported_encodings = data.get("supportedEncodings")
            self._update_capabilities()

            if self.verbosity >= 2:
                if self.server_name != "":
                    print(f"   - Found server name: '{self.server_name}'")
                if self.supported_encodings:
                    print("   - Found the following supported encodings:")
                    for encoding in self.supported_encodings:
                        if encoding in ENCODINGS_SUPPORTED:
                            print(f"     - {encoding} (✅)")
                        else:
                            print(f"     - {encoding} (⚠️)")
                if self.server_capabilities:
                    print("   - Found the following server capabilities:")
                    for capability in self.server_capabilities:
                        if capability in CAPABILITIES_SUPPORTED:
                            print(f"     - ✅ {capability}")
                        else:
                            print(f"     - ⚠️ {capability}")

        if self.verbosity >= 2:
            print("✅ Server info parsing phase complete.")


    def _process_channels(self, data):
        print("👂 Parsing channel advertisements...")
        if data.get("op") == "advertise":
            for channel in data.get("channels", []):
                self._process_advertised_channel(channel)

            if data.get("channels", []) == []:
                print("⚠️ No channels were advertised by the server...")
        else:
            print("✅ Channel discovery phase complete.")

    def _process_services(self, data):
        print("👂 Parsing service advertisements...")
        if data.get("op") == "advertiseServices":
            for service in data.get("services", []):
                self._process_advertised_service(service)

            if data.get("services", []) == []:
                print("⚠️ No services were advertised by the server...")
        else:
            print("✅ Service discovery phase complete.")

    async def _discover_channels(self):
        """Listens for channel advertisements and prepares Protobuf decoders."""
        print(f"👂 Listening for channel advertisements for {self.discovery_timeout} seconds...")
        start_time = asyncio.get_running_loop().time()

        while (asyncio.get_running_loop().time() - start_time) < self.discovery_timeout:
            try:
                remaining_time = self.discovery_timeout - (asyncio.get_running_loop().time() - start_time)
                if remaining_time <= 0:
                    break

                message = await asyncio.wait_for(self.websocket.recv(), timeout=remaining_time)
                data = json.loads(message)
                if data.get("op") == "advertise":
                    for channel in data.get("channels", []):
                        self._process_advertised_channel(channel)

            except asyncio.TimeoutError:
                break
        print("✅ Channel discovery phase complete.")

    def _in_blocklist(self, topic):
        # interpret blocklist elements as regex expressions if they include *, otherwise directly compare strings
        flagged = False

        for block in self.topic_blocklist:
            if not flagged:
                if "*" in block:
                    block = block.split("*")[0]
                    if block in topic:
                        flagged = True
                else:
                    if topic == block:
                        flagged = True
                        break
            else:
                break

        return flagged

    def _process_advertised_service(self, service):
        """Processes a single advertised channel, ignoring blocklisted topics and setting up decoders."""
        service_id = service.get("id")
        name = service.get("name")

        if self._in_blocklist(name):
            if self.verbosity >= 2:
                print(f"   - 🚫 Ignoring blocklisted service: '{name}'")
            return

        if service_id not in self.services_by_id:
            self.services_by_id[service_id] = service
            if self.verbosity >= 2:
                print(f"   - Discovered service: '{name}' (ID: {service_id})")

            if service.get("request").get("encoding") == "protobuf":
                raise NotImplementedError("Protobuf service request encoding to be implemented.")

            if service.get("response").get("encoding") == "protobuf":
                raise NotImplementedError("Protobuf service response decoding to be implemented.")

    def _process_advertised_channel(self, channel):
        """Processes a single advertised channel, ignoring blocklisted topics and setting up decoders."""
        chan_id = channel.get("id")
        topic = channel.get("topic")

        if self._in_blocklist(topic):
            if self.verbosity >= 2:
                print(f"   - 🚫 Ignoring blocklisted topic: '{topic}'")
            return

        if chan_id not in self.channels_by_id:
            self.id_by_channel_topics[topic] = chan_id
            self.channels_by_id[chan_id] = channel
            if self.verbosity >= 2:
                print(f"   - Discovered topic: '{topic}' (ID: {chan_id})")

            if channel.get("encoding") == "protobuf":
                self._prepare_protobuf_decoder(channel)

    def _prepare_protobuf_decoder(self, channel):
        """Creates and stores a Protobuf message class from a schema."""
        chan_id = channel["id"]
        schema_name = channel["schemaName"]
        print(f"     - Preparing Protobuf decoder for schema '{schema_name}'")
        try:
            b64_schema = channel['schema']
            fds_bytes = base64.b64decode(b64_schema)
            fds = descriptor_pb2.FileDescriptorSet.FromString(fds_bytes)

            pool = descriptor_pool.DescriptorPool()
            for fd in fds.file:
                pool.Add(fd)

            descriptor = pool.FindMessageTypeByName(schema_name)
            message_class = message_factory.GetMessageClass(descriptor)
            self.protobuf_decoders[chan_id] = message_class
            print(f"     - Successfully prepared decoder.")
        except Exception as e:
            print(f"     - ❌ Failed to prepare Protobuf decoder for channel {chan_id}: {e}")

    async def _subscribe_to_channels(self):
        """Subscribes to all non-blocklisted channels discovered."""
        if not self.channels_by_id:
            print("⚠️ No channels were advertised by the server. Exiting.")
            return

        sub_id_counter = 0
        sub_requests = []
        for chan_id, chan_info in self.channels_by_id.items():
            sub_id = sub_id_counter
            sub_requests.append({"id": sub_id, "channelId": chan_id})
            self.subscriptions[sub_id] = chan_info
            sub_id_counter += 1

        subscribe_msg = {"op": "subscribe", "subscriptions": sub_requests}
        await self.websocket.send(json.dumps(subscribe_msg))
        print(f"📢 Sent subscription request for {len(sub_requests)} channels.")

    async def _process_messages(self):
        """The main loop to receive, decode, and relay messages."""
        async for message_bytes in self.websocket:
            opcode = message_bytes[0]
            if opcode not in OP_CODES_SUPPORTED:
                continue



            try:
                if opcode == OP_MESSAGE_DATA:
                    sub_id = struct.unpack('<I', message_bytes[1:1+4])[0]
                    timestamp = struct.unpack('<Q', message_bytes[1+4:1+4+8])[0]
                    binary_payload_bytes = message_bytes[1+4+8:]

                    channel_info = self.subscriptions.get(sub_id)
                    if not channel_info:
                        print(f"⚠️ Received message for unknown subscription ID {sub_id}, skipping.")
                        continue

                    payload_obj = self._decode_payload(channel_info, binary_payload_bytes)
                    if payload_obj is None:
                        continue

                    topic = channel_info.get("topic", "unknown_topic")
                    wrapped_message = {
                        "topic": topic,
                        "type": channel_info.get("schemaName", "unknown_type"),
                        "timestamp": timestamp,
                        "payload": payload_obj
                    }
                    message_to_send = json.dumps(wrapped_message)
                elif opcode == OP_TIME:
                    timestamp = struct.unpack('<Q', message_bytes[1:1+8])[0]

                    topic = "/internal/time"
                    wrapped_message = {
                        "topic": topic,
                        "type": "time",
                        "timestamp": timestamp,
                        "payload": "{}"
                    }
                    message_to_send = json.dumps(wrapped_message)

                if self.verbosity >= 2:
                    print(f"Relaying wrapped message from topic '{topic}'...")
                await self._send_msg(message_to_send, topic=topic)

            except (struct.error, IndexError):
                if self.verbosity >= 1:
                    print(f"⚠️ Skipping message: Malformed binary frame received.")
            except json.JSONDecodeError:
                if self.verbosity >= 1:
                    print(
                        f"⚠️ Skipping message on topic '{channel_info.get('topic', 'unknown')}': Payload is not valid JSON.")
            except Exception as e:
                print(
                    f"❌ An unexpected error occurred while processing a message from topic '{channel_info.get('topic', 'unknown')}': {e}")

    def _decode_payload(self, channel_info, binary_payload):
        """Decodes a message payload based on its encoding (JSON or Protobuf)."""
        encoding = channel_info.get("encoding")
        topic = channel_info.get("topic")

        if encoding == "protobuf":
            chan_id = channel_info.get("id")
            message_class = self.protobuf_decoders.get(chan_id)
            if message_class:
                proto_message = message_class()
                proto_message.ParseFromString(binary_payload)
                json_payload_str = json_format.MessageToJson(
                    proto_message,
                    preserving_proto_field_name=True
                )
                return json.loads(json_payload_str)
            else:
                if self.verbosity >= 1:
                    print(f"⚠️ Skipping Protobuf message on topic '{topic}': Decoder not available.")
                return None

        elif encoding == "json":
            return json.loads(binary_payload.decode('utf-8'))

        else:
            if self.verbosity >= 1:
                print(f"⚠️ Skipping message on topic '{topic}': Unsupported encoding '{encoding}'.")
            return None

    async def run(self):
        """Connects to the servers and starts the message relay loop."""
        self._init_zmq()
        self.zmq_socket.bind(self.zmq_address)
        if self.verbosity >= 1:
            print(f"✅ ZMQ server is listening on {self.zmq_address}")

        try:
            async with websockets.connect(
                    self.foxglove_address,
                    subprotocols=["foxglove.sdk.v1"],
                    max_size=None
            ) as websocket:
                self.websocket = websocket
                if self.verbosity >= 1:
                    print(f"✅ Connected to Foxglove WebSocket at {self.foxglove_address}")

                await self._process_connection()
                await self._process_messages()

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
        if self.zmq_socket and not self.zmq_socket.closed:
            if self.verbosity >= 1:
                print("🧹 Cleaning up ZMQ socket and context.")
            self.zmq_socket.close()
            self.context.term()


class FoxgloveToZMQPushRelay(FoxgloveToZMQRelay):
    """A relay that uses a ZMQ PUSH socket, sending all messages to any PULL client."""

    def _init_zmq(self):
        if self.verbosity >= 1:
            print("🔧 Initializing ZMQ PUSH socket.")
        self.zmq_socket = self.context.socket(zmq.PUSH)

    async def _send_msg(self, msg, topic=None):
        # The topic is ignored in a PUSH/PULL pattern
        await self.zmq_socket.send_string(msg)


class FoxgloveToZMQPubSubRelay(FoxgloveToZMQRelay):
    """A relay that uses a ZMQ PUB socket, publishing messages under their original topic."""

    def _init_zmq(self):
        if self.verbosity >= 1:
            print("🔧 Initializing ZMQ PUB socket.")
        self.zmq_socket = self.context.socket(zmq.PUB)

    async def _send_msg(self, msg, topic=None):
        # In a PUB/SUB pattern, the topic is sent as a separate frame
        if topic:
            await self.zmq_socket.send_multipart([topic.encode('utf-8'), msg.encode('utf-8')])


if __name__ == "__main__":
    # --- Configuration ---
    # Choose which relay to run by uncommenting it.

    # PUSH/PULL Example: Sends all messages to any connected PULL client.
    relay = FoxgloveToZMQPushRelay(
        foxglove_address="ws://localhost:8765",
        zmq_address="tcp://*:5555",
        topic_blocklist=[
            # "/hh/points/all",
            "/hh/*",
            "/viper/*"
        ],
        discovery_timeout=2.0,
        verbosity=2,
    )

    # # PUB/SUB Example: Publishes messages on topics for SUB clients to filter.
    # relay = FoxgloveToZMQPubSubRelay(
    #     foxglove_address="ws://localhost:8765",
    #     zmq_address="tcp://*:5555",
    #     topic_blocklist=[
    #         "/some/topic/to/ignore",
    #         "/another/debug/topic",
    #     ],
    #     discovery_timeout=2.0
    # )

    try:
        asyncio.run(relay.run())
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Shutting down.")
