import asyncio
import pytest
from pipeline.Pipeline import sub_pipeline, flatten
from pipeline.test.ExamplePipelineComponents import addone, filter_small, mock_network, \
    join, print_it, duplicate, list_to_queue, iter_flatten
from pipeline.test.ExamplePipeline import example_pipeline


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


def test_resource_pipeline(event_loop):
    async def amain():
        pipeline_execution = example_pipeline.build()
        asyncio.ensure_future(pipeline_execution.run_then_close(
            [1, 2, 3, 4]))
        await pipeline_execution.wait_until_done()
        return pipeline_execution

    res = event_loop.run_until_complete(amain())
    res_list = []
    while res.outq.qsize() > 0:
        res_value = res.outq.get_nowait()
        res_list.append(res_value)
    for r in res_list:
        print()
        print(r)


def test_short_pipeline(event_loop):

    async def amain():
        pipeline_spec = (addone | addone)
        pipeline_execution = pipeline_spec.build()
        asyncio.ensure_future(pipeline_execution.run_then_close(
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
        await pipeline_execution.wait_until_done()
        return pipeline_execution

    res = event_loop.run_until_complete(amain())

    assert res.outq.qsize() == 11
    res_list = []
    while res.outq.qsize() > 0:
        res_value = res.outq.get_nowait()
        res_list.append(res_value)
    for i in range(2, 13):
        assert i in res_list


def test_long_pipeline(event_loop):

    async def amain():
        pipeline_spec = (
              addone 
            | addone 
            | filter_small 
            | mock_network 
            | iter_flatten 
            | join 
            | print_it
        )
        pipeline_execution = pipeline_spec.build()
        asyncio.ensure_future(pipeline_execution.run_then_close(
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
        await pipeline_execution.wait_until_done()
        return pipeline_execution

    res = event_loop.run_until_complete(amain())
    assert res.outq.qsize() == 1
    res_value = res.outq.get_nowait()
    for i in range(4, 11):
        assert f'{i}a' in res_value
        assert f'{i}b' in res_value


def test_flatten_queues(event_loop):

    async def amain():
        pipeline_spec = (
            addone 
            | duplicate 
            | list_to_queue 
            | flatten
        )
        pipeline_execution = pipeline_spec.build()
        asyncio.ensure_future(pipeline_execution.run_then_close(
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
        await pipeline_execution.wait_until_done()
        return pipeline_execution

    res = event_loop.run_until_complete(amain())

    res_list = []

    while res.outq.qsize() > 0:
        res_value = res.outq.get_nowait()
        res_list.append(res_value)

    print(type(res_list))
    print(res_list)


def test_sub_pipeline(event_loop):

    async def amain():
        pipeline_spec = (
              addone 
            | duplicate 
            | list_to_queue 
            | sub_pipeline( 
                             addone 
                           | mock_network
            ) 
            | flatten
        )
        pipeline_execution = pipeline_spec.build()
        asyncio.ensure_future(pipeline_execution.run_then_close(
            [0, 1, 2, 3]))
        await pipeline_execution.wait_until_done()
        return pipeline_execution

    res = event_loop.run_until_complete(amain())

    res_list = []

    while res.outq.qsize() > 0:
        res_value = res.outq.get_nowait()
        res_list.append(res_value)
