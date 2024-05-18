import asyncio
import signal

from brq.consumer import Consumer
from brq.log import logger


class Daemon:
    """
    Tool for daemonizing a consumer.
    """

    def __init__(self, consumer: Consumer):
        self.consumer = consumer

    async def run_forever(self, stop_signals: list = [signal.SIGINT, signal.SIGTERM]):
        loop = asyncio.get_event_loop()

        stop_event = asyncio.Event()

        async def _stop():
            logger.debug("Signal received")
            stop_event.set()

        for sig in stop_signals:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(_stop()))

        await self.consumer.start()
        logger.info(f"Consumer started, waiting for signals {stop_signals}...")
        await stop_event.wait()

        logger.info(f"Wait consumer to stop...")
        await self.consumer.stop()


if __name__ == "__main__":
    asyncio.run(Daemon(Consumer()).run_forever())
