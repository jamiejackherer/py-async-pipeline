import asyncio
import pytest
import random
from pipeline.Pipeline import PipelineMapper, run_pipeline_blocking
from pipeline.test.ExamplePipelineComponents import addone, filter_small, mock_network, \
    join, print_it, duplicate, list_to_queue, iter_flatten
from pipeline.test.ExamplePipeline import example_pipeline
from pipeline.Loggers import LogDecorator


@PipelineMapper
@LogDecorator
async def build_url(next_id: int) -> str:
    return f'https://www.example.com/resources/{next_id}'


@PipelineMapper
@LogDecorator
async def fetch_resource(next_url: str) -> str:
    await asyncio.sleep(random.randint(0, 2))
    # use async network library like aiohttp
    return f'mock_response::{next_url}'

@PipelineMapper
@LogDecorator
async def process_resource(response: str) -> object:
    await asyncio.sleep(random.randint(0, 2))
    return {'processed_field': f'PROCESSED::{response}'}

@PipelineMapper
@LogDecorator
async def save_to_db(resource: object) -> object:
    await asyncio.sleep(random.randint(0, 2))
    # use async database library like aiopg
    resource['saved_to_db'] = True
    return resource

def test_readme_example():
    # define the pipeline blueprint (resuable)
    pipeline_blueprint = build_url | fetch_resource | process_resource | save_to_db

    # create input generator
    inp = range(50)

    # run the pipeline on the generator 
    run_pipeline_blocking(pipeline_blueprint, inp)





