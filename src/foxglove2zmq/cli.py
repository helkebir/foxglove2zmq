import asyncio
import argparse
from .relay import FoxgloveToZMQPushRelay, FoxgloveToZMQPubSubRelay

def main():
    """The main entry point for the command-line interface."""
    parser = argparse.ArgumentParser(description="Relay Foxglove WebSocket messages to a ZMQ server.")
    parser.add_argument(
        "-p", "--pattern",
        choices=["push", "pub"],
        required=True,
        help="The ZMQ socket pattern to use ('push' for PUSH/PULL or 'pub' for PUB/SUB)."
    )
    parser.add_argument(
        "-w", "--foxglove-ws",
        required=False,
        default="ws://localhost:8765",
        help="The WebSocket URL of the Foxglove server (e.g., ws://localhost:8765)."
    )
    parser.add_argument(
        "-i", "--incoming",
        required=True,
        help="The TCP address for the incoming ZMQ server to bind to (e.g., tcp://localhost:5555)."
    )
    parser.add_argument(
        "-o", "--outgoing",
        required=False,
        default=None,
        help="The TCP address for the outgoing ZMQ server to bind to (e.g., tcp://localhost:5556)."
    )
    parser.add_argument(
        "-v", "--verbosity",
        required=False,
        type=int,
        default=1,
        help="The verbosity level (0 = errors only, 1 = info, 2 = debug)."
    )
    parser.add_argument(
        "-b", "--blocklist",
        nargs='*',
        default=[],
        help="A space-separated list of topics to ignore (e.g., /diagnostics /debug)."
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=2.0,
        help="Time in seconds to wait for channel advertisements."
    )

    args = parser.parse_args()

    if args.pattern == "push":
        relay_class = FoxgloveToZMQPushRelay
    else: # pub
        relay_class = FoxgloveToZMQPubSubRelay

    relay = relay_class(
        foxglove_address=args.foxglove_ws,
        zmq_address=args.outgoing,
        zmq_listen_address=args.incoming,
        topic_blocklist=args.blocklist,
        discovery_timeout=args.timeout,
        verbosity=args.verbosity
    )

    try:
        asyncio.run(relay.run())
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Shutting down.")

if __name__ == "__main__":
    main()
