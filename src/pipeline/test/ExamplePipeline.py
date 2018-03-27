import random
from typing import Tuple, NamedTuple, List, Optional
import asyncio

from pipeline.Pipeline import PipelineMapper, PipelineReducer, sub_pipeline, flatten
from pipeline.Loggers import LogDecorator
from pipeline.CloseableQueue import CloseableQueue


class ResourceResponse(NamedTuple):
    id: int
    url: str
    sub_resource_ids: List[int]

    def __str__(self):
        return f'[ResourceResponse id={self.id}]'

    def __repr__(self):
        return f'[ResourceResponse id={self.id}]'


class SubResourceResponse(NamedTuple):
    id: int
    url: str
    name: str

    def __str__(self):
        return f'[SubResourceResponse id={self.id}]'

    def __repr__(self):
        return f'[SubResourceResponse id={self.id}]'


class FullResource(NamedTuple):
    resource: ResourceResponse
    sub_resources: List[SubResourceResponse]

    def __str__(self):
        return f'[FullResource id={self.resource.id} subids={[sr.id for sr in self.sub_resources]}]'

    def __repr__(self):
        return f'[FullResource id={self.resource.id} subids={[sr.id for sr in self.sub_resources]}]'


@PipelineMapper
@LogDecorator
async def build_url(next_id: int) -> Tuple[int, str]:
    return (next_id, f'https://www.example.com/resources/{next_id}')


@PipelineMapper
@LogDecorator
async def fetch_resource(next_resource: Tuple[int, str]) -> ResourceResponse:
    await asyncio.sleep(random.randint(0, 2))
    rid = next_resource[0]
    sub_resource_ids = list(range(rid * 100, rid * 100 + 5))
    return ResourceResponse(next_resource[0], next_resource[1], sub_resource_ids)


@PipelineMapper
@LogDecorator
async def split_into_subqueues(next_resource: ResourceResponse) -> CloseableQueue:
    sub_resource_itr = [(next_resource, srid) for srid in next_resource.sub_resource_ids]
    return CloseableQueue.from_iter(sub_resource_itr)


@PipelineMapper
@LogDecorator
async def build_sub_resource_url(next_resource: Tuple[ResourceResponse, int]) -> Tuple[ResourceResponse, int, str]:
    resource = next_resource[0]
    subid = next_resource[1]
    url = f'https://www.example.com/resources/{resource.id}/subresources/{subid}'
    return (resource, subid, url)


@PipelineMapper
@LogDecorator
async def fetch_sub_resource(next_resource: Tuple[ResourceResponse, int, str]) -> Tuple[ResourceResponse, SubResourceResponse]:
    await asyncio.sleep(random.randint(0, 2))
    resource = next_resource[0]
    subid = next_resource[1]
    url = next_resource[2]
    sub_resource = SubResourceResponse(subid, url, f'SubResource{subid}')
    return (resource, sub_resource)


@PipelineMapper
@LogDecorator
async def process_sub_resource(next_resource: Tuple[ResourceResponse, SubResourceResponse]) -> Tuple[ResourceResponse, SubResourceResponse]:
    await asyncio.sleep(random.randint(0, 5))
    # Imaginary processing in another thread...
    return next_resource


@PipelineReducer
@LogDecorator
async def merge_sub_resources(accum: Optional[FullResource],
                              nxt: Tuple[ResourceResponse, SubResourceResponse]
                              ) -> FullResource:

    resource_response = nxt[0]    
    next_subresource = nxt[1]

    if not accum:
        return FullResource(resource_response, [next_subresource])

    accum_sub_resources = accum.sub_resources
    return FullResource(resource_response, accum_sub_resources + [next_subresource])


example_pipeline = (
      build_url 
    | fetch_resource 
    | split_into_subqueues 
    | sub_pipeline(  
                     build_sub_resource_url 
                   | fetch_sub_resource 
                   | process_sub_resource 
                   | merge_sub_resources
    ) 
    | flatten
)
