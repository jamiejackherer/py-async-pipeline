import asyncio
from pipeline.CloseableQueue import CloseableQueue
from pipeline.Pipeline import PipelineMapper, PipelineReducer, PipelineFlattener
from pipeline.Loggers import LogDecorator

@PipelineMapper
@LogDecorator
async def addone(x):
    return x + 1


@PipelineMapper
@LogDecorator
async def mock_network(nxt):
    await asyncio.sleep(1)
    results = [str(nxt) + "a", str(nxt) + "b"]
    return results


@PipelineMapper
@LogDecorator
async def iter_flatten(nxt):
    for n in nxt:
        yield n


@PipelineMapper
@LogDecorator
async def print_it(nxt):
    print(f'print_it: {nxt}')
    return nxt


@PipelineMapper
@LogDecorator
async def filter_small(nxt):
    if nxt >= 4:
        yield nxt


@PipelineReducer
@LogDecorator
async def join(accum, nxt):
    if accum is None:
        return nxt
    else:
        return ' '.join([accum, nxt])


@PipelineMapper
@LogDecorator
async def duplicate(nxt):
    return [nxt, nxt]


@PipelineFlattener
@LogDecorator
async def queueflatten(nxt):
    return nxt


@PipelineMapper
@LogDecorator
async def list_to_queue(nxt):
    q = CloseableQueue()
    for n in nxt:
        q.put_nowait(n)
    q.close()
    return q


@PipelineMapper
@LogDecorator
async def queue_to_iter(nxt):
    li = []
    while nxt.qsize() > 0:
        li.append(await nxt.get())
    return li
