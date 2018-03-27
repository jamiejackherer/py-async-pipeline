import asyncio
import pytest
from pipeline.ErrorHandlers import PipelineStopException
from pipeline.Pipeline import PipelineMapper, PipelineReducer, sub_pipeline, flatten
from pipeline.Loggers import LogDecorator


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


@PipelineMapper
@LogDecorator
async def exception(next_id: int):
    raise Exception()


def test_exception(event_loop):

    async def amain():
        pipeline_spec = exception
        pipeline_execution = pipeline_spec.build()
        asyncio.ensure_future(pipeline_execution.run_then_close(
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))

        with pytest.raises(PipelineStopException):
            await pipeline_execution.wait_until_done()

        return pipeline_execution

    event_loop.run_until_complete(amain())

    
    
