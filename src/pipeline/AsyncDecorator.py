import functools
import inspect


def BuildAsyncDecorator(before_fn, after_fn):
    def decorator_builder(fn):
        if inspect.isasyncgenfunction(fn):
            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                before_fn(fn, args, kwargs)
                async for res in fn(*args, **kwargs):
                    after_fn(fn, args, kwargs, res)
                    yield res
        else:
            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                before_fn(fn, args, kwargs)
                res = await fn(*args, **kwargs)
                after_fn(fn, args, kwargs, res)
                return res
        return wrapper
    return decorator_builder