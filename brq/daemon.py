import asyncio
import copy
import signal

from brq.consumer import Consumer, RunnableMixin
from brq.log import logger


class Daemon:
    """
    Tool for daemonizing a consumer.
    """

    def __init__(self, *runnables: list[RunnableMixin]):
        self.runnables = runnables

    async def run_forever(self, stop_signals: list = [signal.SIGINT, signal.SIGTERM]):
        loop = asyncio.get_event_loop()

        stop_event = asyncio.Event()

        async def _stop():
            logger.debug("Signal received")
            stop_event.set()

        for sig in stop_signals:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(_stop()))

        logger.info(f"Starting {len(self.runnables)} consumer...")
        for runnable in self.runnables:
            await runnable.start()
        logger.info(f"Consumer started, waiting for signals {stop_signals}...")
        await stop_event.wait()

        logger.info(f"Terminating consumer...")
        for runnable in self.runnables:
            await runnable.stop()
        logger.info(f"Consumer terminated")


if __name__ == "__main__":

    class DummyConsumer(RunnableMixin):
        async def initialize(self):
            print("init")

        async def run(self):
            print("run")
            await asyncio.sleep(1)

        async def cleanup(self):
            print("cleanup")

    asyncio.run(Daemon(DummyConsumer()).run_forever())
