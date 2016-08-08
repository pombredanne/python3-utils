import sys
import types
import copyreg
import traceback
from copy import deepcopy
from functools import wraps
from collections import abc


from multiprocess import cpu_count, Pool


def _pool_map_more_than_one_arg(method):
    """Use this decorator when using pool with a function that
    accepts more than one argument. Note that args_or_kwargs must
    consists of either an iterable or a mapping"""
    @wraps(method)
    def wrapper(args_or_kwargs):
        if isinstance(args_or_kwargs, abc.Mapping):
            return method(**args_or_kwargs)
        else:
            return method(*args_or_kwargs)
    return wrapper


def _error_wrapper_pool(method):
    """Make sure that your multiprocessing pool workers report
    their full traceback when crashing."""

    @wraps(method)
    def wrapper(*args, **kwargs):
        try:
            resp = method(*args, **kwargs)
        except Exception as exception:
            trace = traceback.format_exception(*sys.exc_info())
            raise exception.__class__(''.join(trace))
        return resp

    return wrapper


def _group_args(worker_args):
    if isinstance(worker_args, abc.Mapping):
        # turns {1:a,b,c,d, 2:f,g,h,i} into
        # [{1:a, 2:f}, {1:b, 2:g}, {1:c, 2:h}, {1:d, 2:i}]
        worker_args_split = [
            [(k, e) for e in v] for k, v in worker_args.items()
        ]
        worker_args = [
            dict(partial)
            for partial in zip(*worker_args_split)
        ]
    else:
        # turns [[1, 2, 3, 4], [a, b, c, d]] into
        # [[1, a], [2, b], [3, c], [4, d]]
        worker_args = [list(partial) for partial in zip(*worker_args)]

    return worker_args


def pool_map(
        worker, worker_args, constant_args=None,
        cpu_ratio=0.75, single_thread=False):

    worker_args = _group_args(worker_args)

    if constant_args is not None:
        try:
            [args.update(deepcopy(constant_args)) for args in worker_args]
        except AttributeError:
            [args.extend(deepcopy(constant_args)) for args in worker_args]

    worker = _pool_map_more_than_one_arg(_error_wrapper_pool(worker))

    if single_thread:
        resp = map(worker, worker_args)
    else:
        count_cpu_workers = int(
            (cpu_ratio * cpu_count())
            if cpu_ratio <= 1.0 else cpu_ratio
        )
        pool = Pool(count_cpu_workers)
        resp = pool.imap(worker, worker_args)
        pool.close()
        pool.join()

    return list(resp)
