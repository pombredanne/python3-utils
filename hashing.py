# built in modules
import json
import random
import string
import numbers
import hashlib
import collections
import argparse

# project modules
from .core import is_list_or_tuple


SYMBOLS = '0123456789abcdefghujklmnopqrstuvwxyzABCDEFGHUJKLMNOPQRSTUVWXYZ_='
NEG_SYMBOL = '-'
BASE = len(SYMBOLS)
SYMBOLS_VALUE = {s: i for i, s in enumerate(SYMBOLS)}


def encode_compact64(value):
    """Generate a compat base 64 encoding of value"""
    if value < 0:
        return '{}{}'.format(NEG_SYMBOL, encode_compact64(-value))

    encoding = []
    while True:
        value, remainder = divmod(value, BASE)
        encoding.append(SYMBOLS[remainder])
        if value == 0:
            break

    return ''.join(reversed(encoding))


def decode_compact64(value):
    """Return the value of a number ecoded using base 64"""
    if value[0] == NEG_SYMBOL:
        return -decode_compact64(value[1:])

    number = 0
    for i, symbol in enumerate(reversed(value)):
        number += SYMBOLS_VALUE[symbol] * (BASE ** i)

    return number


def randstr(n):
    """generates a random string of length N
    from http://stackoverflow.com/a/23728630
    """
    vals = string.ascii_uppercase + string.digits
    s = ''.join(random.SystemRandom().choice(vals) for _ in range(n))
    return s


def compact_hash_obj(obj, ignore_unhashable=False):
    h = hash_obj(obj, ignore_unhashable)
    return encode_compact64(h)


def hash_obj(obj, ignore_unhashable=False):
    '''Returns hash for object obj'''
    if isinstance(obj, numbers.Number):
        obj = str(obj)

    if obj is None:
        obj = 'null'

    try:
        return int(hashlib.md5(obj.encode('utf-8')).hexdigest(), 16)
    except (TypeError, AttributeError):
        pass

    if isinstance(obj, collections.Mapping):
        outobj = collections.OrderedDict()
        for k in sorted(obj.keys()):
            outobj[k] = hash_obj(obj[k])
    elif type(obj) in (list, tuple, set):
        try:
            sorted_obj = sorted(obj)
        except TypeError:
            sorted_obj = obj
        outobj = [hash_obj(elem) for elem in sorted_obj]
    elif ignore_unhashable:
        return 0
    else:
        raise TypeError('[error] obj can not be hashed')

    json_dump = json.dumps(outobj, sort_keys=True).encode('utf-8')
    hash_value = int(hashlib.md5(json_dump).hexdigest(), 16)
    return hash_value


class HashableNamespace(argparse.Namespace):
    def __init__(self, *args, **kwargs):
        hash_ignore = kwargs.pop('hash_ignore', [])
        if not (
                is_list_or_tuple(hash_ignore) or
                isinstance(hash_ignore, collections.abc.Set)
        ):
            msg = '"hash_ignore" must be list, tuple, or set'
            raise ValueError(msg)

        if isinstance(args[0], argparse.Namespace):
            self.__dict__.update(args[0].__dict__)
        else:
            super(HashableNamespace, self).__init__(*args, **kwargs)

        self.hash_ignore = set(hash_ignore)

    def __hash__(self):
        return hash_obj([
            (k, v) for k, v in sorted(self.__dict__.items())
            if k not in self.hash_ignore and k != 'hash_ignore'
        ])
