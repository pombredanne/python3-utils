import math
import json
import random
import string
import numbers
import hashlib
import collections

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
        outobj = [hash_obj(elem) for elem in obj]
    elif ignore_unhashable:
        return 0
    else:
        raise TypeError('[error] obj can not be hashed')

    json_dump = json.dumps(outobj, sort_keys=True).encode('utf-8')
    hash_value = int(hashlib.md5(json_dump).hexdigest(), 16)
    return hash_value
