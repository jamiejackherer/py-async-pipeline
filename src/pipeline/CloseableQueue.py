from asyncio import Queue, Event


class CloseableQueue(Queue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__closed__ = Event()

    def is_closed(self):
        return self.__closed__.is_set()

    def close(self):
        self.__closed__.set()

    async def wait_until_closed(self):
        if self.__closed__.is_set():
            return True
        else:
            await self.__closed__.wait()
            return True

    @staticmethod
    def from_iter(itr):
        c = CloseableQueue()
        for i in itr:
            c.put_nowait(i)
        c.close()
        return c
