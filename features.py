# built-in modules

# installed modules
import numpy as np

# project modules
from . import elastic
from .core import deep_iterate


def get_colleciton_idf(terms, index_name, doc_type, field_name, cache=None):
    es_client = elastic.get_client(
        index_name=index_name, doc_type=doc_type, field_name=field_name
    )

    if cache is None:
        cache = {}

    terms_to_fetch = [t for t in terms if t not in cache]

    dfs = elastic.get_terms_df(terms_to_fetch , es_client=es_client)
    max_docs = elastic.stats(es_client)['count']

    idfs_dict = {
        t: 1 + np.log(max_docs / (df + 1))
        for t, df in zip(terms_to_fetch,dfs)
    }

    cache.update(idfs_dict)
    idfs = [cache[t] for t in terms]
    return idfs


def get_collections_term_odds(
        terms, index1_name, index2_name, doc1_type, field1_name,
        doc2_type=None, field2_name=None, cache=None
):
    """Get the odds of terms between two indexes for terms"""

    if doc2_type is None:
        doc2_type = doc1_type

    if field2_name is None:
        field2_name = field1_name

    if cache is None:
        cache = {}

    # get two clients for the indices of interest
    # (one would do, but this way is more clear)
    es1 = elastic.get_client(
        index_name=index1_name, doc_type=doc1_type, field_name=field1_name
    )
    es2 = elastic.get_client(
        index_name=index2_name, doc_type=doc2_type, field_name=field2_name
    )

    # get subset of terms not in cache to count
    terms_to_count = [t for t in terms if t not in cache]

    # get the count of all terms in the two indices to normalize
    all1 = elastic.stats(es1)['count']
    all2 = elastic.stats(es2)['count']

    # get the counts of terms not in cache for the two indices
    count1 = elastic.batch_count(terms_to_count, es1)
    count2 = elastic.batch_count(terms_to_count, es2)

    # probability of term in health wikipedia
    p1 = (h / all1 for h in count1)

    # probability of term in non-health wikipedia
    p2 = (t2 / all2 for t2 in count2)

    # log odds of terms of appearing in health wikipedia
    this_odds = (np.log2(t1 / t2 + 1.0) for t1, t2 in zip(p1, p2))

    # we create a dictionary of the odds of new terms
    new_odds = dict(zip(terms_to_count, this_odds))

    # update the cache with all new ters
    cache.update(new_odds)

    # finally, get the list of odds for the requested terms
    odds = [cache[t] for t in terms]

    return odds


def get_one_hot_encoding(data, dtype=float, valid_symbols=None):

    if valid_symbols is None:
        valid_symbols = set(deep_iterate(data))

    values_map = {k: p for p, k in enumerate(sorted(valid_symbols))}

    m = np.zeros(shape=(len(data), len(values_map)), dtype=dtype)

    for val, pos in deep_iterate(data, yield_pos=True):
        if val not in values_map:
            continue

        # we ignore the last coordinate
        *part_pos, _ = pos
        new_pos = tuple(part_pos) + (values_map[val], )
        m[new_pos] = 1.

    return m
