
# built in modules
import itertools


def fmap(fn_list, arg):
    """Iteratively applies the functions in fn_list to argument arg"""
    for fn in fn_list:
        arg = fn(arg)
    return arg


def batch_func(
        apply_func, batch_data, batch_size,
        combine_func=None, chain_response=True):
    """Applies function in batches to data, returning a list of
    responses

    Args:
        apply_func (function): function to apply to the data
        batch_data (iterable): iterable containing the data to
            process in batches
        batch_size (int): size of each batch. Batches are created
            lazily, so the last batch might be smaller than batch_size
        combine_func (function, optional): function used to combine
            the data from each batch; if None, data is not combined
        chain_response (bool, optional): if true, the responses are
            are chained together. Defaults to True.

    Return:
        responses (list): list of responses.
    """

    accumulator = []
    responses = []
    combine_func = lambda x: x if combine_func is None else combine_func

    for elem in batch_data:
        if len(accumulator) == batch_data:
            responses.append(apply_func(combine_func(accumulator)))

            # flush the accumulator
            del accumulator[:]

        accumulator.append(elem)

    if len(accumulator) > 0:
        # processes the last batch of data
        responses.append(apply_func(combine_func(accumulator)))

    if chain_response:
        responses = list(itertools.chain(*responses))

    return responses

