import asyncio

from pipeline.QueueProcessor import QueueMapper, QueueReducer, QueueFlattener
from pipeline.ErrorHandlers import ErrorHandler
from pipeline.Loggers import LogDecorator
from pipeline.Utils import identity

class PipelineComponent:
    '''
    A stage in a pipeline. Specifies how to transform one stream of values
    into another.

    Design note: this class is a factory for instances of QueueProcessor.

    PipelineComponents and the Pipelines they compose are reusable. Think of them 
    like blueprints used by a PipelineExecution to build and link the QueueProcessors that 
    actually execute the transformation on a specific input.

    Calling .build() will create a PipelineExecution for this Pipeline.
    '''

    def __init__(self, fn, n_workers=5):
        """
        Args:
            fn (awaitable / async iterator): The function that transforms each value of the input queue to
                values for the output queue. Can be an awaitable or an async iterator (allowing a single input
                value to generate multipleoutput values -- or none!).
            n_workers (`int`, optional): The number of workers that should be allowed to process the input
                queue using the above function.

        TODO: Allow an option to choose an executor, specifying that threads should be used to avoid blocking
              during the execution of the function. (E.g., if a blocking database call must be made).

        """
        self.fn = fn
        self.n_workers = n_workers
        self.err_fn = ErrorHandler()

    def as_pipeline(self):
        return Pipeline([self])

    def build(self):
        return self.as_pipeline().build()

    def __or__(self, other):
        return self.as_pipeline() | other.as_pipeline()


class Pipeline:
    '''
    An ordered series of pipeline components.

    Uses the composite pattern. Callers should only ever create PipelineComponents. 

    Whenever they are composed together, they will be automatically and transparently converted
    to Pipelines where necessary.
    '''

    def __init__(self, pipeline_components):
        self.pcomponents = pipeline_components

    def as_pipeline(self):
        return self

    def build(self):
        return PipelineExecution(self)

    def __or__(self, other):
        other = other.as_pipeline()
        return Pipeline(self.pcomponents + other.pcomponents)


class PipelineMapper(PipelineComponent):
    '''
    Specifies the mapping from one queue of values to another. Each input value
    can be transformed into zero, one, or multiple output values.
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __call__(self):
        return QueueMapper(
            self.fn,
            error_handler_fn=self.err_fn,
            n_workers=self.n_workers,
            name=self.fn.__name__)


class PipelineReducer(PipelineComponent):
    '''
    Specifies a reduce on a queue of values to a single value. The provided
    processor function should take two values and return a single value.
    '''

    def __init__(self, *args, **kwargs):
        if 'n_workers' not in kwargs:
            kwargs['n_workers'] = 1
        super().__init__(*args, **kwargs)

    def __call__(self):
        return QueueReducer(
            self.fn,
            error_handler_fn=self.err_fn,
            n_workers=self.n_workers,
            name=self.fn.__name__)


class PipelineFlattener(PipelineComponent):
    '''
    Specifies the "flattening" of a queue of queues into a single queue.
    '''

    def __call__(self):
        return QueueFlattener(
            self.fn,
            error_handler_fn=self.err_fn)


class PipelineExecution:
    '''
    Takes a Pipeline as a blueprint and builds a chain of queue processors
    that can be used to execute the transformation on an input queue.
    '''

    def __init__(self, pipeline):
        self.qprocessors = []

        for pc in pipeline.pcomponents:
            qprocessor = pc()
            self.qprocessors.append(qprocessor)

        self.inq = self.qprocessors[0].inq
        self.outq = self.qprocessors[-1].outq

        pipeline = self.qprocessors[0]
        for qproc in self.qprocessors[1:]:
            pipeline = pipeline | qproc

    async def run_then_close(self, inp_iter):
        '''
        Send each input in inp_iter to the pipeline's input queue,
        then close the pipeline's input queue.
        '''
        for i in inp_iter:
            await self.inq.put(i)
        self.inq.close()
        return

    async def send(self, nxt):
        '''
        Send input to the pipeline's input queue.
        '''
        await self.inq.put(nxt)

    def close(self):
        '''
        This method signals that no more input will be added to the input
        queue. When the input is closed and all processing is finished in
        the pipeline, then `is_done` will return true and `wait_until_done`
        will resolve.
        '''
        self.inq.close()

    def is_done(self):
        '''
        Returns true if all processing is finished in the pipeline. Will 
        not return true until the pipeline input queue has been closed.
        '''
        return self.qprocessors[-1].is_done()

    async def wait_until_done(self):
        '''
        Coroutine that blocks until the pipeline input queue is closed and 
        all processing is finished. Returns the pipeline's output queue.
        '''
        await self.qprocessors[-1].wait()
        return self.outq


def sub_pipeline(sub_pipeline_spec):
    '''
    Sub pipelines are used to operate on a queue of queues. Each sub-queue will be run 
    through its own instance of the sub-pipeline.
    '''
    @PipelineMapper
    @LogDecorator
    async def _sub_pipeline(nxt):
        sub_pipeline = sub_pipeline_spec.build()
        while nxt.qsize() > 0:
            await sub_pipeline.send(await nxt.get())
        sub_pipeline.close()
        return await sub_pipeline.wait_until_done()

    return _sub_pipeline
        

'''
`flatten` is a re-usable pipelne componenet that flattens
a queue of queues into a single queue.
'''
flatten = PipelineFlattener(identity)

async def run_pipeline_async(pipeline_blueprint, inp):
    # build an executable instance of the pipeline
    pipeline_execution = pipeline_blueprint.build()

    # start sending input
    for i in inp:
        await pipeline_execution.send(i)

    # signal that no more input is coming
    pipeline_execution.close()

    # wait for all the input to be fully processed by the pipeline
    await pipeline_execution.wait_until_done()

    # return the output asyncio.Queue
    return pipeline_execution.outq

def run_pipeline_blocking(pipeline_blueprint, inp):
    '''
    Run the provided pipeline on the provided inp iterable.
    '''
    # TODO: For now, just sets/uses default asyncio event loop.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    out_queue = loop.run_until_complete(run_pipeline_async(pipeline_blueprint, inp))
    return out_queue