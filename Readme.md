# Python Asyncio Pipeline

A toolkit for using asyncio to implement a queue-based asynchronous pipeline in Python.

## Example

The stages in the simple pipeline below will execute concurrently. For example, while the
`fetch_resource` stage waits for more responses, the `process_resource` stage will be processing
the responses that have already arrived.

```python

# define the pipeline components
@PipelineMapper
async def build_url(next_id: int) -> str:
    return f'https://www.example.com/resources/{next_id}'


@PipelineMapper
async def fetch_resource(next_url: str) -> str:
    await asyncio.sleep(random.randint(0, 2))
    # use async network library like aiohttp
    return f'mock_response::{next_url}'

@PipelineMapper
async def process_resource(response: str) -> object:
    await asyncio.sleep(random.randint(0, 2))
    # can fire off / await result of processing step in another thread (beware GIL) or process.
    return {'processed_field': f'PROCESSED::{response}'}

@PipelineMapper
async def save_to_db(resource: object) -> object:
    await asyncio.sleep(random.randint(0, 2))
    # use async database library like aiopg
    resource['saved_to_db'] = True
    return resource


# define the pipeline blueprint (resuable)
pipeline_blueprint = build_url | fetch_resource | process_resource | save_to_db

# create input generator
inp = range(50)

# run the pipeline on the generator.
# blocks until complete, and returns the pipeline's output queue.
output_queue = run_pipeline_blocking(pipeline_blueprint, inp)

# or, in an async context, 
# > await run_pipeline_async(pipeline_blueprint, inp)
```