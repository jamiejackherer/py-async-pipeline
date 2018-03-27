from asyncio import Event, ensure_future, gather, CancelledError
from contextlib import contextmanager, suppress
import inspect

from pipeline.CloseableQueue import CloseableQueue
from pipeline.Utils import identity


class QueueProcessor:
    '''
    Base class for asynchronously processing an input queue and putting the results
    into an output queue. 

    The primary subclass hooks are __process_nxt__ and __before_stop__. Using these
    functions, subclasses can specify how to use the `proc_fn` to map the input queue
    onto and output queue. See QueueMapper, QueueReducer, and QueueFlattener for examples.
    '''

    def __init__(self,
                 proc_fn=identity,
                 inq=None,
                 outq=None,
                 auto_close=True,
                 error_handler_fn=None,
                 n_workers=5,
                 auto_start=True,
                 name=None) -> None:

        self.proc_fn = proc_fn
        self.error_handler_fn = error_handler_fn
        self.n_workers = n_workers
        self.name = name or proc_fn.__name__

        self.inq = inq or CloseableQueue()
        self.outq = outq or CloseableQueue()
        
        self.auto_close = auto_close

        # Events that signal the processing state of this queue processor.
        self.started_processing = Event()
        self.finished_processing = Event()

        # -- Asynchronous Tasks --- 

        # Will be set to a list of worker tasks that
        # do the actual work of processing the queue.
        self.worker_tasks = None

        # Task that waits for the input queue to be closed
        # and fully processed. When that happens, it cancels
        # the worker tasks and sets `finished_processing`.
        self.listen_for_input_done = ensure_future(self.__on_input_done__())

        # -- / Asynchronous Tasks ---

        self.pipe_next = None
        self.pipe_prev = None

        if auto_start:
            self.start_processing()

    async def wait(self):
        '''
        Blocks until this QueueProcessor has finished processing all
        of its input and sent it to the output queue.
        '''
        await gather(self.finished_processing.wait(), *self.worker_tasks)
        return True

    def is_done(self):
        return self.finished_processing.is_set()

    async def __process_nxt__(self, worker_num, nxt):
        '''
        Subclass hook for processing the next item in the queue.
        '''
        await self.outq.put(nxt)

    async def __before_stop__(self):
        '''
        Provides a subclass hook that is called after the input queue is closed
        and fully processed, but before `finished_processing` is set and the
        output queue is closed.
        '''
        pass

    def start_processing(self):
        '''
        Starts worker tasks.
        '''
        if not self.started_processing.is_set():
            self.started_processing.set()
            self.worker_tasks = [ensure_future(self.__processing_task__(i)) for i in range(self.n_workers)]
            
    async def __processing_task__(self, i):
        '''
        Encapsulates a single worker task. Reads from the input queue, 
        awaits the result from the processing function, and then
        sends the output to the output queue. 
            
        Runs continuously until it has been cancelled.
        '''

        # When the input queue is done and has been fully processed,
        # the processor tasks will be cancelled.
        #
        # Cancellation is not an error. This method should simply
        # return at that point.
        with suppress(CancelledError):
            while not self.finished_processing.is_set():
                nxt = await self.inq.get()
                with self.error_handling_context(nxt):
                    await self.__process_nxt__(i, nxt)
                self.inq.task_done()

    def __stop_working__(self):
        '''
        Stops all worker tasks and sets `finished_processing`.
        '''
        if self.worker_tasks:
            for w in self.worker_tasks:
                w.cancel()
        self.finished_processing.set()

    def __stop_waiting_for_input_done__(self):
        '''
        Cancels the task that waits for and handles the closing of the input queue.
        '''
        self.listen_for_input_done.cancel()

    def __auto_close__(self):
        '''
        Closes the output queue if `auto_close` is True.
        '''
        if self.auto_close:
            self.outq.close()

    @contextmanager
    def error_handling_context(self, ctx=None):
        '''
        Context manager that sends Exceptions to the `error_handler_fn`.
        If the `error_handler_fn` raises an Exception in turn, then
        the QueueProcessor is stopped and the Exception is raised.
        '''
        try:
            yield
        except Exception as e:
            if self.error_handler_fn:
                try:
                    self.error_handler_fn({
                        'exception': e,
                        'processor': self,
                        'processor_name': self.name,
                        'payload': ctx
                    })
                except Exception as e:
                    self.__stop_working__()
                    self.__auto_close__()
                    self.__stop_waiting_for_input_done__()
                    raise e
            else:
                raise e

    async def __on_input_done__(self):
        '''
        Waits for the input queue to be closed and fully processed, and
        then stops worker tasks.
        '''
        while not self.inq.is_closed():
            await self.inq.wait_until_closed()
        await self.inq.join()
        with self.error_handling_context():
            await self.__before_stop__()
        self.__stop_working__()
        self.__auto_close__()

    @staticmethod
    def pipe(q1, q2, fn=identity, auto_close=True):
        return QueueMapper(fn, q1, q2, n_workers=1, auto_start=True, auto_close=auto_close, name="pipe")
        
    def __or__(self, other):
        '''
        Pipes the output of this QueueProcessor into the input of the other one.
        '''
        qpipe = QueueProcessor.pipe(self.outq, other.inq)
        self.pipe_next = qpipe
        qpipe.pipe_prev = self
        qpipe.pipe_next = other
        other.pipe_prev = qpipe
        return other


class QueueFlattener(QueueProcessor):
    def __init__(self, *args, **kwargs):
        self.pipes = []
        super().__init__(*args, **kwargs)

    async def __process_nxt__(self, worker_num, nxt):
        self.pipes.append(QueueProcessor.pipe(nxt, self.outq, fn=self.proc_fn, auto_close=False))
    
    async def __before_stop__(self):
        wait_for_all = [pipe.wait() for pipe in self.pipes]
        await gather(*wait_for_all)


class QueueMapper(QueueProcessor):
    async def __process_nxt__(self, worker_num, nxt):
        if inspect.isasyncgenfunction(self.proc_fn):
            async for res in self.proc_fn(nxt):
                await self.outq.put(res)
        else:
            res = await self.proc_fn(nxt)
            await self.outq.put(res)


class QueueReducer(QueueProcessor):
    def __init__(self, *args, **kwargs):
        self.n_workers = kwargs.get('n_workers', 1)
        self.accums = {}
        super().__init__(*args, **kwargs)

    async def __process_nxt__(self, worker_num, nxt):
        self.accums[worker_num] = await self.proc_fn(self.accums.get(worker_num, None), nxt)

    async def __before_stop__(self):
        accum = None
        for worker_num, nxt in self.accums.items():
            if not accum:
                accum = nxt
            else:
                accum = await self.proc_fn(accum, nxt)
        await self.outq.put(accum)
