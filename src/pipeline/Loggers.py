from pipeline.AsyncDecorator import BuildAsyncDecorator

__all__ = ["LogDecorator"]


def before_fn(fn, args, kwargs):
    print(f'-> {fn.__name__}({args}, {kwargs})')


def after_fn(fn, args, kwargs, res):
    print(f'-> {fn.__name__}({args}, {kwargs}) -> {res}')


LogDecorator = BuildAsyncDecorator(before_fn, after_fn)