"""This module provides caching to file in multiple formats."""

# built in modules
import os
import sys
import gzip
import json
import pickle
import string
import inspect
import functools

# project modules
from .hashing import hash_obj


class CacheError(RuntimeError):
    """Error for caching function"""

    def __init__(self, *args, **kwargs):
        super(CacheError, self).__init__(*args, **kwargs)


def _write_cache(data, extension, filepath):
    if 'json' in extension:
        dump = bytes(json.dumps(data), 'utf-8')
    elif 'pickle' in extension:
        dump = pickle.dumps(data)
    else:
        msg = '"{}" is not a supported extension'.format(extension)
        raise CacheError(msg)

    if 'gzip' in extension:
        io_handler = gzip.open
    else:
        io_handler = open

    try:
        with io_handler(filepath, 'wb') as f:
            f.write(dump)
    except Exception:
        os.remove(filepath)
        raise


def _read_cache(extension, filepath):
    if 'gzip' in extension:
        io_handler = gzip.open
    else:
        io_handler = open

    try:
        with io_handler(filepath, 'rb') as f:
            dump = f.read()
    except Exception:
        os.remove(filepath)
        raise

    if 'json' in extension:
        data = json.loads(dump.decode('utf-8'))
    elif 'pickle' in extension:
        data = pickle.loads(dump)
    else:
        msg = '"{}" is not a supported extension'.format(extension)
        raise CacheError(msg)

    return data


def simple_caching(
        cachedir=None, include_args=False, cache_comment=None,
        invalidate=False, cache_ext='json.gzip', callback_func_hit=None,
        callback_func_miss=None, quiet=False, no_caching=False,
        cache_name=None):
    """ Caching decorator

    Args:
        include_args (bool, default=False): determine whether
            arguments passed to the function should be included as
            cache comment
        cachedir (str, default=None): location of the folder where to cache.
            cachedir doesn't need to be configured if simple_caching is
            caching a method of a class with cachedir attribute.
        cache_comment (str, default=None): a comment to add to the name of
            the cache. If no comment is provided, the name of the cache
            is the name of the method that is being cached.
        invalidate (bool, default=False): re-builds cache if set to True
        cache_ext (str, default='json.gz', choices=['pickle', 'json',
            'json.gz', 'pickle.gz']): format and encoding of the cache
        callback_func_hit (function, default=None): function to call if
            cached element is found
        callback_func_miss (function, default=None): function to call if
            cached element is not found
        quiet (bool, default=False): if true, no messages are printed
        cache_name (string, default=None): name of cache file; if none,
            the name of the function is used
        no_caching (bool, default=False): set it to true to prevent caching
            (useful during debugging sessions)

    Notes:
        The kwargs can be set either (a) at decoration time
        or (b) when the decorated method is called:

        example (a):
        @simple_caching(cachedir='/path/to/cache')
        def foo(s):
        ...

            example (b):
            @simple_caching()
            def foo(s):
        ...
            ...

            foo('baz', cachedir='/path/to/cache')

        A combination of both is also fine, of course.
        kwargs provided at call time have precedence, though.
    """

    def caching_decorator(method):
        # cachedir, cache_comment and autodetect are out
        # of scope for method_wrapper, thus local variables
        # need to be instantiated.
        local_cachedir = cachedir
        local_cache_comment = (cache_comment or '')
        local_invalidate = invalidate
        local_cache_ext = cache_ext
        local_include_args = include_args
        local_quiet = quiet
        local_no_caching = no_caching
        local_cache_name = cache_name

        # if not callback functions are specified, they are simply set to
        # the identity function
        local_callback_func_miss = (callback_func_miss or (lambda e: e))
        local_callback_func_hit = (callback_func_hit or (lambda e: e))

        # collect the deafault parameters for the method.
        # if include_args is true, this values are used if kwargs
        # are not provided
        method_params = inspect.signature(method).parameters
        default_params = {
            k: v.default for k, v in method_params.items()
            if v.default != inspect._empty
        }

        @functools.wraps(method)
        def method_wrapper(*args, **kwargs):

            # looks for cachedir folder in self instance
            # if not found, it looks for it in keyword
            # arguments.
            try:
                cachedir = args[0].cachedir
            except (IndexError, AttributeError) as e:
                cachedir = kwargs.pop('cachedir', local_cachedir)

            # if no cachedir is specified, then it simply returns
            # the original method and does nothing
            if not cachedir:
                if not local_quiet:
                    msg = (
                        '[cache] destination not provided; '
                        'method "{}" will not be be cached'
                        ''.format(method.__name__)
                    )
                    print(msg, file=sys.stderr)
                return method(*args, **kwargs)

            # checks if the global parameters are overwritten by
            # values @ call time or if some of the missing parameters
            # have been provided at call time
            invalidate = kwargs.pop('invalidate', local_invalidate)
            include_args = kwargs.pop('include_args', local_include_args)
            callback_func_miss = kwargs.pop('callback_func_miss',
                                            local_callback_func_miss)
            callback_func_hit = kwargs.pop('callback_func_hit',
                                           local_callback_func_hit)
            cache_comment = kwargs.pop('cache_comment', local_cache_comment)
            no_caching = kwargs.pop('no_caching', local_no_caching)
            cache_name = kwargs.pop('cache_name', local_cache_name)

            if no_caching:
                return method(*args, **kwargs)

            # include underscore to separate cache_comment
            # from the rest of the filename if the cache comment
            # is present
            cache_comment = (
                '_{}'.format(cache_comment) if cache_comment else ''
            )

            if not os.path.exists(cachedir):
                msg = (
                    '[cache] folder "{}" does not exists; creating it'
                    ''.format(cachedir)
                )
                print(msg, file=sys.stderr)
                os.makedirs(cachedir)

            cache_ext = kwargs.pop('cache_ext', local_cache_ext)

            name = (
                method.__name__.strip(string.punctuation)
                if cache_name is None else cache_name
            )

            if include_args:
                # include default values to kwargs!
                kwargs_with_default = {**default_params, **kwargs}
                to_hash = hash_obj([args, kwargs_with_default])
                args_comment = ('_0{}'.format(to_hash) if to_hash > 0
                                else '_1{}'.format(to_hash * -1))
            else:
                # makes sure that there is an underscore
                # between cache file name and cache comment
                args_comment = ''

            cachename = '{}{}{}.cache.{}'.format(
                name, args_comment, cache_comment, cache_ext)
            cachepath = os.path.join(cachedir, cachename)

            if os.path.exists(cachepath) and not invalidate:
                loaded = callback_func_hit(_read_cache(cache_ext, cachepath))
            else:
                resp = method(*args, **kwargs)
                loaded = callback_func_miss(resp)

                if not local_quiet:
                    msg = '[cache] generating {}'.format(cachepath)
                    print(msg, file=sys.stderr)

                _write_cache(resp, cache_ext, cachepath)

            return loaded
        return method_wrapper
    return caching_decorator
