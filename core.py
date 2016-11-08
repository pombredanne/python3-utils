import collections
import random
import itertools
import copy


def getset(s):
    """Returns an element from a set"""
    for x in s:
        return x


def interleave_lists(lst_a, lst_b, prob_a=None, seed=None):
    """Randomly interleaves two lists according to prob_a"""
    if prob_a is None:
        prob_a = len(lst_a) / (len(lst_a) + len(lst_b))

    if seed is None:
        seed = random.random()
    random.seed(seed)

    dest = []
    a = b = 0
    while True:
        if a == len(lst_a):
            dest.extend(lst_b[b:])
            break
        if b == len(lst_b):
            dest.extend(lst_a[a:])
            break
        p = random.random()
        if p > prob_a:
            dest.append(lst_b[b])
            b += 1
        else:
            dest.append(lst_a[a])
            a += 1
    return dest


def slice_list(lst, slice_size, func_while_slicing=None):
    """Slices list lst in slices of size slice_size;

    If func_while_slicing is specified, then the function is
    applied to each example before slicing."""
    if func_while_slicing is None:
        def func_while_slicing(member):
            return member

    current_slice = []

    for elem in lst:
        current_slice.append(func_while_slicing(elem))
        if len(current_slice) == slice_size:
            yield current_slice
            current_slice = []

    yield current_slice


def is_list_or_tuple(obj):
    """Returns True if obj is a list or a tuple"""
    return (
        isinstance(obj, collections.abc.Sequence) and
        not(isinstance(obj, str) or isinstance(obj, bytes))
    )


class Bunch(dict):
    """Collect elements"""

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.__dict__ = self

    def __copy__(self):
        return self.__class__({k: v for k, v in self.__dict__.items()})

    def __deepcopy__(self, memodict=None):
        return self.__class__({k: copy.deepcopy(v, memodict)
                               for k, v in self.__dict__.items()})


class RecBunch(Bunch):
    """Just like Bunch, but recursive"""

    def __init__(self, *args, **kwargs):
        super(RecBunch, self).__init__(*args, **kwargs)
        for k, v in self.items():
            try:
                # uses self.__class__() instead of RecBunch()
                # to make sure that, when inheriting from this,
                # objects of the inheriting class are created
                # rather than RecBunch objects
                self[k] = self.__class__(v)
            except (ValueError, TypeError):
                self[k] = v

    def __getattr__(self, name):
        if name not in self:
            self[name] = RecBunch()

        return self[name]

    # def __setattr__(self, name, val):
    #     print(name, val)


def merge_dicts(a, b, path=None):
    """recursively merges b into a;
    from http://stackoverflow.com/a/7205107"""
    if path is None:
        path = []

    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dicts(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def powerset(li, include_empty=False):
    if include_empty:
        yield tuple()

    for i in range(1, len(li) + 1):
        for comb in itertools.combinations(li, i):
            yield comb


def flatten(li, n=1):
    """Flattens a list n times"""
    if n == 0:
        return li
    else:
        return flatten(itertools.chain(*li), n=(n - 1))
