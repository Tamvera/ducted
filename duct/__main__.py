"""
Duct - asyncio entry point

Usage:
    ductd -c duct.yml
    python -m duct -c duct.yml
"""

import argparse
import asyncio
import logging
import signal

from duct.configuration import ConfigFile
from duct.service import DuctService


def main():
    """Entry point for the duct daemon."""
    parser = argparse.ArgumentParser(
        description='Duct - A monitoring agent and event processor'
    )
    parser.add_argument(
        '-c', '--config',
        default='duct.yml',
        help='Configuration file (default: duct.yml)',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose/debug logging',
    )
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
    )

    asyncio.run(_run(args.config))


async def _run(config_path):
    log = logging.getLogger(__name__)
    log.info("Starting Ductd service.")
    config = ConfigFile(config_path)

    svc = DuctService(config)

    loop = asyncio.get_running_loop()

    stop_event = asyncio.Event()

    def _handle_signal():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    await svc.startService()
    log.info("Service running.")

    await stop_event.wait()

    await svc.stopService()


if __name__ == '__main__':
    main()
