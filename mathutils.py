# installed libraries
import numpy
import scipy.stats
from gensim.matutils import unitvec


def jensen_shannon_divergence(p, q):
    """Implementation of the [Jensen-Shannon divergence metric]
    (https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence)
    from http://stackoverflow.com/a/27432724"""
    _p = p / numpy.linalg.norm(p, ord=1)
    _q = q / numpy.linalg.norm(q, ord=1)
    _m = 0.5 * (_p + _q)
    return 0.5 * (scipy.stats.entropy(_p, _m) + scipy.stats.entropy(_q, _m))


def try_float(s):
    try:
        return float(s)
    except ValueError:
        return s


def vecsim(v1, v2):
    """Calculate similarity between two vectors"""
    return numpy.dot(unitvec(v1), unitvec(v2))


def vstack(v1, v2):
    if v1 is None:
        return v2

    if v2 is None:
        return v1

    return numpy.vstack((v1, v2))


def hstack(v1, v2):
    if v1 is None:
        return v2

    if v2 is None:
        return v1

    return numpy.hstack((v1, v2))
