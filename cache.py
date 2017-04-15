"""This module provides caching to file in multiple formats."""

# built in modules
import os
import re
import sys
import gzip
import json
import pickle
import string
import functools

# installed modules
import numpy


class CacheError(RuntimeError):
    """Error for caching function"""

    def __init__(self, *args, **kwargs):
        super(CacheError, self).__init__(*args, **kwargs)


class Writers:
    @staticmethod
    def json(data, filepath, io_handler):
        """Writes data to filepath using io_handler in json format"""

        dump = bytes(json.dumps(data), 'utf-8')
        try:
            with io_handler(filepath, 'wb') as f:
                f.write(dump)
        except Exception:
            os.remove(filepath)
            raise

    @staticmethod
    def pickle(data, filepath, io_handler):
        """Writes data to filepath using io_handler in pickle format"""

        dump = pickle.dumps(data)
        try:
            with io_handler(filepath, 'wb') as f:
                f.write(dump)
        except Exception:
            os.remove(filepath)
            raise

    @staticmethod
    def numpy(data, filepath, io_handler):
        """Writes data to filepath using io_handler as numpy object
        NOTE: this will raise an error if data is not a numpy object"""

        try:
            with io_handler(filepath, 'wb') as f:
                numpy.save(f, data, allow_pickle=False)
        except Exception:
            os.remove(filepath)
            raise


def _choose_writer(extension, compression):
    """Return the correct writer and io handler given the extension
    and whether data should be compressed or not"""
    try:
        writer = getattr(Writers, extension)
    except AttributeError:
        msg = '"{}" is not a supported extension'.format(extension)
        raise CacheError(msg)

    if compression:
        io_hadler = gzip.open
    else:
        io_hadler = open

    return writer, io_hadler


class Readers:
    @staticmethod
    def json(filepath, io_handler):
        """Loads data in json format from filepath using io_handler"""

        with io_handler(filepath, 'rb') as f:
            dump = f.read()
        return json.loads(dump.decode('utf-8'))

    @staticmethod
    def pickle(filepath, io_handler):
        """Loads data in pickle format from filepath using io_handler"""

        with io_handler(filepath, 'rb') as f:
            dump = f.read()
        return pickle.loads(dump)

    @staticmethod
    def numpy(filepath, io_handler):
        """Loads data from filepath using io_handler as numpy object"""

        with io_handler(filepath, 'rb') as f:
            data = numpy.load(f)
        return data


def _choose_reader(extension, compression):
    try:
        reader = getattr(Readers, extension)
    except AttributeError:
        msg = '"{}" is not a supported extension'.format(extension)
        raise CacheError(msg)

    if compression:
        io_hadler = gzip.open
    else:
        io_hadler = open

    return reader, io_hadler


def simple_caching(
        cachedir=None, cache_comment=None, invalidate=False,
        cache_format='json', cache_compression=True, callback_func_hit=None,
        callback_func_miss=None, quiet=False, no_caching=False,
        cache_name=None, cache_ext=None
):
    """ Caching decorator

    Args:
        cachedir (str, default=None): location of the folder where to cache.
            cachedir doesn't need to be configured if simple_caching is
            caching a method of a class with cachedir attribute.
        cache_comment (str, default=None): a comment to add to the name of
            the cache. If no comment is provided, the name of the cache
            is the name of the method that is being cached.
        invalidate (bool, default=False): re-builds cache if set to True
        cache_format (str, default='json', choices=['pickle', 'json',
            'numpy']): format of the encoded data
        cache_compression (bool, default=True): compress the data with
            gzip
        callback_func_hit (function, default=None): function to call if
            cached element is found
        callback_func_miss (function, default=None): function to call if
            cached element is not found
        quiet (bool, default=False): if true, no messages are printed
        cache_name (string, default=None): name of cache file; if none,
            the name of the function is used
        no_caching (bool, default=False): set it to true to prevent caching
            (useful during debugging sessions)
        cache_ext (str, default=None): format and compression can also be
            expressed by supplying the extension as in the previous API

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

        A combination of both is also fine, of course; kwargs provided 
        at call time have precedence, though.
    """

    def caching_decorator(method):
        # cachedir, cache_comment and autodetect are out
        # of scope for method_wrapper, thus local variables
        # need to be instantiated.
        local_cachedir = cachedir
        local_cache_comment = (cache_comment or '')
        local_invalidate = invalidate
        local_quiet = quiet
        local_no_caching = no_caching
        local_cache_name = cache_name
        local_cache_compresison = cache_compression
        local_cache_ext = cache_ext
        local_cache_format = cache_format

        # if not callback functions are specified, they are simply set to
        # the identity function
        local_callback_func_miss = (callback_func_miss or (lambda e: e))
        local_callback_func_hit = (callback_func_hit or (lambda e: e))

        @functools.wraps(method)
        def method_wrapper(*args, **kwargs):

            # looks for cachedir folder in self instance
            # if not found, it looks for it in keyword
            # arguments.
            try:
                inner_cachedir = args[0].cachedir
            except (IndexError, AttributeError):
                inner_cachedir = kwargs.pop('cachedir', local_cachedir)

            # if no cachedir is specified, then it simply returns
            # the original method and does nothing
            if not inner_cachedir:
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
            inner_invalidate = kwargs.pop('invalidate', local_invalidate)
            inner_no_caching = kwargs.pop('no_caching', local_no_caching)
            inner_cache_name = kwargs.pop('cache_name', local_cache_name)
            inner_cache_comment = kwargs.pop(
                'cache_comment', local_cache_comment
            )
            inner_callback_func_miss = kwargs.pop(
                'callback_func_miss', local_callback_func_miss
            )
            inner_callback_func_hit = kwargs.pop(
                'callback_func_hit', local_callback_func_hit
            )

            if inner_no_caching:
                return method(*args, **kwargs)

            # include underscore to separate cache_comment
            # from the rest of the filename if the cache comment
            # is present
            inner_cache_comment = (
                '_{}'.format(inner_cache_comment)
                if inner_cache_comment else ''
            )

            if not os.path.exists(inner_cachedir):
                msg = (
                    '[cache] folder "{}" does not exists; creating it'
                    ''.format(inner_cachedir)
                )
                print(msg, file=sys.stderr)
                os.makedirs(inner_cachedir)

            inner_cache_ext = kwargs.pop('cache_ext', local_cache_ext)

            # we use the boolean `cache_compression` if `cache_ext` is
            # None; otherwise we try to infer the whether compression
            # is required from `cache_ext` (that is, we look for 'gzip'
            # or 'gz').
            inner_cache_compression = (
                kwargs.pop('cache_compression', local_cache_compresison)
                if inner_cache_ext is None
                else ('gzip' in inner_cache_ext or 'gz' in inner_cache_ext)
            )

            # we use the cache format provided if `cache_ext` is None;
            # otherwise we try to infer the cache format from cache_ext
            # (accepted formats are `json`, `pickle`, and `numpy`).
            #
            # Note that, because getattr returns None if no match is
            # found, we can't call `.group` directly on its output bacause
            # it might not be a `_sre.SRE_Match` object; therefore, we
            # use a mock lambda function that always return None
            inner_cache_format = (
                kwargs.pop('cache_format', local_cache_format)
                if inner_cache_ext is None
                else getattr(
                    re.search(r'(json|pickle|numpy)', inner_cache_ext),
                    'group', lambda v: None
                )(0)
            )

            name = (
                method.__name__.strip(string.punctuation)
                if inner_cache_name is None else inner_cache_name
            )

            cachename = '{}{}.cache.{}{}'.format(
                name,
                inner_cache_comment,
                inner_cache_format,
                '.gzip' if inner_cache_compression else ''
            )
            cachepath = os.path.join(inner_cachedir, cachename)

            # check if cache exists!
            if os.path.exists(cachepath) and not inner_invalidate:

                # get reader and io_handler giver format and compression
                # options for the cache
                reader, io_handler = _choose_reader(
                    inner_cache_format, inner_cache_compression
                )

                # use the reader to load the data, then apply the
                # closure for a funciton hit
                loaded = inner_callback_func_hit(reader(cachepath, io_handler))
            else:

                # get the data from the method
                resp = method(*args, **kwargs)

                # generate notice message in case caching is occurring
                if not local_quiet:
                    msg = '[cache] generating {}'.format(cachepath)
                    print(msg, file=sys.stderr)

                # get the writer for caching to disk, then write the data
                writer, io_handler = _choose_writer(
                    inner_cache_format, inner_cache_compression
                )
                writer(resp, cachepath, io_handler)

                # apply callback funciton before returning data
                loaded = inner_callback_func_miss(resp)

            return loaded
        return method_wrapper
    return caching_decorator
