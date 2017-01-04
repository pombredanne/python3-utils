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


def slice_on_axis(m, axis, start, end=None):
    """Slices a numpy array along axis axis
    Adapted from http://stackoverflow.com/a/37729566
    """
    if end is None:
        start, end = 0, start

    slc = [slice(None)] * len(m.shape)
    slc[axis] = slice(start, end)
    return m[slc]


def rolling_mean(x, n, axis=None):
    """Calculates the rolling mean of size n of vector x
    Adapted from http://stackoverflow.com/a/27681394

    Args:
        x (numpy.ndarray): input vector
        n (int): size of window
        axis (int, optional): the axis along which to calculate the mean
    """
    if axis is None:
        axis = 0

    cumsum = numpy.cumsum(numpy.insert(x, 0, 0, axis=axis), axis=axis)
    return (cumsum[n:] - cumsum[:-n]) / n


def flip(m, axis=None):
    """
    Reverse the order of elements in an array along the given axis.
    from current dev branch of numpy (1.1.2dev0)
    https://docs.scipy.org/doc/numpy-dev/reference/generated/numpy.flip.html
    """
    if axis is None:
        axis = 0

    if not hasattr(m, 'ndim'):
        m = numpy.asarray(m)
    indexer = [slice(None)] * m.ndim
    try:
        indexer[axis] = slice(None, None, -1)
    except IndexError:
        raise ValueError(
            "axis=%i is invalid for the %i-dimensional input array"
            % (axis, m.ndim))
    return m[tuple(indexer)]


def get_context_average(x, window, axis=None, include_context=False):
    """Calculate the context average of x."""
    if axis is None:
        axis = 0

    # size of the window plus element
    n = 2 * window + 1

    # length of the averaging axis
    l = x.shape[axis]

    norm_vec = numpy.concatenate((
        numpy.arange(window, n - 1),
        numpy.ones(l - n + 1) * (n - 1),
        numpy.arange(n - 2, window - 1, -1)
    )).reshape(
        [l if i == axis else 1 for i in range(x.ndim)]
    )

    center = rolling_mean(x, n, axis=axis)
    front = slice_on_axis(
        numpy.cumsum(slice_on_axis(x, axis, n - 1), axis=axis),
        axis, window, n - 1
    )

    back = flip(slice_on_axis(
        numpy.cumsum(flip(slice_on_axis(x, axis, l - n + 1, l), axis=axis),
                     axis=axis),
        axis=axis, start=window, end=n - 1
    ), axis=axis)

    context_sum = numpy.insert(
        numpy.insert(center, center.shape[axis], back, axis=axis),
        0, front, axis=axis
    )

    if include_context:
        context_average = context_sum / (norm_vec + 1)
    else:
        context_average = (context_sum - x) / norm_vec

    return context_average
