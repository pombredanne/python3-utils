import numpy.linalg
import scipy.stats


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
