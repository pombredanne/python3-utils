#!/usr/bin/env python3

"""Utilites to manage an Elasticsearch index"""

# built in modules
import os
import re
import sys
import time
import json
import gzip
import warnings
import itertools
import collections
from functools import wraps

# installed modules
import elasticsearch
import elasticsearch.exceptions
from elasticsearch.helpers import scan

# project modules
from .hashing import hash_obj, encode_compact64
from .stringutils import len_utf8
from .core import is_list_or_tuple
from .iterutils import batch_func


def escape(text):
    return re.sub(r'([-=&|><!\(\){}\[\]\^"~*\+?:\\/])', '\\\1', text)


class ElasticsearchClientError(RuntimeError):
    """Error raised by functions and methods in this module"""

    def __init__(self, *args, **kwargs):
        super(ElasticsearchClientError, self).__init__(*args, **kwargs)


class EsClient(elasticsearch.Elasticsearch):
    """A variant of the Elasticsearch client that provides caching of
    searches."""

    def __init__(self, *args, **kwargs):

        self.signature = hash_obj([args, kwargs])

        self.cache = kwargs.pop('cache', None)
        self.cachedir = kwargs.pop('cachedir', 'cache')
        self.safesleep_timer = kwargs.pop('safesleep_timer', 1)

        self._field_name = kwargs.pop('field_name', None)
        self._doc_type = kwargs.pop('doc_type', None)
        self._index_name = kwargs.pop('index_name', None)
        self._analyzer_name = kwargs.pop('analyzer_name', None)

        super(EsClient, self).__init__(*args, **kwargs)

        if self.cache is not None:
            self._configure_cache()

    def _configure_cache(self):
        """Configure cache mode for the client"""

        if self.cache == 'memcache':
            # cache will be stored in this dictionary
            self._cache_dict = {}
            cache_decorator = self._memcache_wrapper

            self.search = self._memcache_wrapper(self.search)
            self.count = self._memcache_wrapper(self.count)
            self.indices.analyze = self._memcache_wrapper(
                self.indices.analyze)
        elif self.cache == 'diskcache':
            # create cache directory if one does not exists
            try:
                os.makedirs(self.cachedir)
            except OSError:
                pass

            cache_decorator = self._diskcache_wapper

            self.search = self._diskcache_wapper(self.search)
            self.count = self._diskcache_wapper(self.count)
            self.indices.analyze = self._diskcache_wapper(
                self.indices.analyze)
        else:
            err = ('cache mode "{}" not recognized '
                   '(choose "memcache" or "diskcache")')
            raise ElasticsearchClientError(err)

        self.search, self.count, self.indices.analyze = [
            cache_decorator(f) for f in
            (self.search, self.count, self.indices.analyze)
        ]

    @property
    def field_name(self):
        if self._field_name is None:
            err = ('You request the default field name, but none has been '
                   'provided to this Elasticsearch client.')
            raise ElasticsearchClientError(err)

        return self._field_name

    @field_name.setter
    def field_name(self, value):
        self._field_name = value

    @property
    def analyzer_name(self):
        if self._analyzer_name is None:
            err = ('You request the default analyzer name, but none has been '
                   'provided to this Elasticsearch client.')
            raise ElasticsearchClientError(err)

        return self._analyzer_name

    @analyzer_name.setter
    def analyzer_name(self, value):
        self._analyzer_name = value

    @property
    def doc_type(self):
        if self._doc_type is None:
            err = ('You request the default document type, but none has '
                   'been provided to this Elasticsearch client.')
            raise ElasticsearchClientError(err)

        return self._doc_type

    @doc_type.setter
    def doc_type(self, value):
        self._doc_type = value

    @property
    def index_name(self):
        if self._index_name is None:
            err = ('You request the default index name, but none has '
                   'been provided to this Elasticsearch client.')
            raise ElasticsearchClientError(err)

        return self._index_name

    @index_name.setter
    def index_name(self, value):
        self._index_name = value

    def _memcache_wrapper(self, method):
        @wraps(method)
        def wrapper(*args, **kwargs):
            invalidate = kwargs.pop('invalidate', False)
            comment = encode_compact64(
                int(hash_obj(args), 16) + int(hash_obj(kwargs), 16))
            cache_key = '{}_{}'.format(method.__name__, comment)

            if cache_key in self._cache_dict and not invalidate:
                resp = self._cache_dict[cache_key]
            else:
                resp = method(*args, **kwargs)
                self._cache_dict[cache_key] = resp

            return resp
        return wrapper

    def _saferead(self, fp):
        has_read = False
        resp = None

        while not has_read:
            with gzip.open(fp, 'rb') as f:
                try:
                    resp = json.loads(f.read().decode('utf-8'))
                    has_read = True
                except json.JSONDecodeError:
                    print(resp, file=sys.stderr)
                    time.sleep(self.safesleep_timer)
        return resp

    def _diskcache_wapper(self, method):
        @wraps(method)
        def wrapper(*args, **kwargs):
            invalidate = kwargs.pop('invalidate', False)
            comment = hash_obj(args) + hash_obj(kwargs)
            name = method.__name__

            # we split cached data into 256 subfolders to prevent
            # hitting limits of file system
            cache_subdir = os.path.join(
                self.cachedir, encode_compact64(comment % 256))
            if not os.path.exists(cache_subdir):
                os.makedirs(cache_subdir)

            # cache_subdir = self.cachedir

            fp = os.path.join(
                cache_subdir,
                'es-{}-{}.cache.gzip'.format(
                    name, encode_compact64(comment)))

            if os.path.exists(fp) and not invalidate:
                resp = self._saferead(fp)
            else:
                resp = method(*args, **kwargs)
                with gzip.open(fp, 'wb') as f:
                    f.write(json.dumps(resp).encode('utf-8'))

            return resp
        return wrapper

    def __hash__(self):
        return self.signature

    def __repr__(self):
        self_repr = '<Elasticsearch([{}, {}])'.format(
            self.transport.hosts,
            {
                'index_name': self._index_name,
                'doc_type': self._doc_type,
                'field_name': self._field_name
            }
        )
        return self_repr

    def __str__(self):
        return self.__repr__()


def get_client(*config_paths, **config_kwargs):
    """Get an index client"""

    es_config = {}

    # check if a default configuration exists in the
    # home folder of the user, use it in case it does.
    default_config_path = os.path.join(
        os.path.expanduser('~'), '.elasticsearch'
    )
    if not config_paths and os.path.exists(default_config_path):
        config_paths = [default_config_path]

    # update es_config using all files in configuration_pats
    for fp in config_paths:
        with open(fp) as f:
            es_config.update(json.load(f))

    # arguments passed as keyword have precedence
    es_config.update(config_kwargs)

    es_host = es_config.pop('host', os.environ.get('ES_HOST', None))
    es_port = int(es_config.pop('port', os.environ.get('ES_PORT', None)))

    es_user = es_config.pop('username', os.environ.get('ES_USER', None))
    es_passwd = es_config.pop('password', os.environ.get('ES_PASSWD', None))
    es_protocol = es_config.pop('protocol', 'http')

    if es_user is not None and es_passwd is not None:
        url_auth_sec = '{}:{}@'.format(es_user, es_passwd)
    else:
        url_auth_sec = ''

    url_dest_sec = '{}:{}'.format(es_host, es_port)
    full_es_url = '{}://{}{}'.format(es_protocol, url_auth_sec, url_dest_sec)

    return EsClient(full_es_url, **es_config)


def tokenize(
        string, es_client,
        field_name=None, index_name=None, analyzer_name=None):
    """Tokenize a string based on analyzer of the provided field

    Args:
        string (string): the string to tokenize
        es_client (elasticsearch.client.Elasticsearch): elasticsearch client.
        field_name (string): the field whose analyzer is used to
            tokenize
        index_name (string): name of the index
    Returns:
        tokens (list): a list of tokens

    Raises:
        ElasticsearchClientError: if no field name or index name
            are available.
    """
    if field_name is None:
        field_name = es_client.field_name

    if index_name is None:
        index_name = es_client.index_name

    req = {'body': string, 'index': index_name}
    if analyzer_name is None:
        req['field'] = field_name
    else:
        req['analyzer'] = analyzer_name

    try:
        response = es_client.indices.analyze(**req)
        tokens = [d['token'] for d in response['tokens']]
    except elasticsearch.exceptions.RequestError:
        tokens = []

    return tokens


def phrase_search(
        query_string, es_client, field_name=None, slop=0, in_order=True,
        retrieved_fields=None, maxsize=None, index_name=None):
    """ Perform phrase search

    Args:
        query_string (string): the query to submit to elasticseach
        es_client (elasticsearch.client.Elasticsearch): elasticsearch client.
        field_name (string): the field in which to search query_string
        slop (int): maximum distance between non-consecutive terms in the
            query; by default, no distance is allowed (all terms must be
            consecutive)
        in_order (bool): whether terms should appear in order
        retrieved_fields (list or None): optional list of fields to return.
            If None, all fields are returned.
        maxsize (int or None): maximum number of results to retrieve.
        index_name (string): name of the index

    Returns:
        results (list): a list of search results

    Raises:
        ValueError: Phrase has length 1; simple_search should be used
            instead of phrase_search
        ElasticsearchClientError: if no field name or index name
            are available.
    """
    if field_name is None:
        field_name = es_client.field_name

    phrase_terms = tokenize(string=query_string, field_name=field_name,
                            es_client=es_client, index_name=index_name)

    if len(phrase_terms) < 2:
        raise ValueError('Phrase has length 1; use simple search')

    query_dsl = {
        "query": {
            "span_near": {
                "clauses": [
                    {"span_term": {field_name: term}}
                    for term in phrase_terms
                ],
                "slop": slop,
                "in_order": in_order,
                "collect_payloads": False
            }
        }
    }

    if retrieved_fields is not None:
        query_dsl['fields'] = retrieved_fields

    results = raw_search(query_dsl=query_dsl, es_client=es_client,
                         maxsize=maxsize, index_name=index_name)
    return results


def phrase_count(
        query_string, es_client,
        field_name=None, slop=0, in_order=True, index_name=None):
    """ Perform phrase count

    Args:
        query_string (string): the query to submit to elasticseach
        es_client (elasticsearch.client.Elasticsearch): elasticsearch client.
        field_name (string): the field in which to search query_string
        slop (int): maximum distance between non-consecutive terms in the
            query; by default, no distance is allowed (all terms must be
            consecutive)
        in_order (bool): whether terms should appear in order
        index_name (string): name of the index

    Returns:
        count (int): returns the number of terms matching the query

    Raises:
        ElasticsearchClientError: if no field name or index name
            are available.
    """
    if field_name is None:
        field_name = es_client.field_name

    phrase_terms = tokenize(string=query_string, field_name=field_name,
                            es_client=es_client, index_name=index_name)

    if len(phrase_terms) < 2:
        # surprise: this is not a phrase count!
        return count(
            ' '.join(phrase_terms),
            es_client=es_client, field_name=field_name, index_name=index_name
        )

    query_dsl = {
        "query": {
            "span_near": {
                "clauses": [{"span_term": {field_name: term}}
                            for term in phrase_terms],
                "slop": slop,  # max number of intervening unmatched pos.
                "in_order": in_order,
                "collect_payloads": False
            }
        }
    }

    return raw_count(query_dsl, es_client, index_name)


def stats(es_client, index_name=None):
    """Returns count of documents and size in bytes on an index
    Args:
        es_client (elasticsearch.client.Elasticsearch): elasticsearch client.
        index_name (string): name of the index

    Returns:
        stats (dict): a dictionary with stats about the index
    """
    if index_name is None:
        index_name = es_client.index_name

    resp = es_client.indices.stats(index=index_name)

    stats = {
        'count': resp['_all']['primaries']['docs']['count'],
        'size': resp['_all']['primaries']['store']['size_in_bytes']
    }

    return stats


def simple_search(
        query_string, es_client, field_name=None, operator='or',
        retrieved_fields=None, maxsize=None, index_name=None):
    """ Perform simple search

    Args:
        query_string (string): the query to submit to elasticseach
        field_name (string or sequence): the field in which to search
            query_string; if multiple fields are provided, they will get
            combined into a boolean should query
        operator (string): operator to use in search (can be 'and' or 'or');
            by default, 'or' is used
        es_client (elasticsearch.client.Elasticsearch): elasticsearch client.
        retrieved_fields (list or None): optional list of fields to return.
            If None, all fields are returned.
        maxsize (int or None): maximum number of results to retrieve;
            If None, 1000 is used.
        index_name (string): name of the index

    Returns:
        results (list): a list of search results
    """

    if field_name is None:
        field_name = es_client.field_name

    if not is_list_or_tuple(field_name):
        field_name = [field_name]

    query_dsl = {
        'query': {
            'query_string': {
                'query': query_string,
                'fields': field_name,
                'default_operator': operator
            }
        },
    }

    if retrieved_fields is not None:
        query_dsl['fields'] = retrieved_fields

    results = raw_search(
        query_dsl=query_dsl, es_client=es_client, maxsize=maxsize
    )

    return results


def raw_search(
        query_dsl, es_client, maxsize=None, index_name=None, offset=None,
        invalidate=True):
    """Subroutine to perform search

    Args:
        query_dsl (dict): the query in Query DSL language
        es_client (elasticsearch.client.Elasticsearch): elasticsearch client.
        maxsize (int or None): maximum number of results to retrieve;
            If None, 1000 is used.
        index_name (string): name of the index to use.

    Returns:
        results (list): a list of search results
    """

    if maxsize is None:
        maxsize = query_dsl.pop('size', 1000)

    if offset is None:
        offset = 0

    if index_name is None:
        index_name = es_client.index_name

    raw_results = es_client.search(
        index=index_name, body=query_dsl, size=maxsize)
    results = raw_results['hits']['hits']

    return results


def count(query, es_client, operator='or', field_name=None, index_name=None):
    """ Count the matches of query in the index

    Args:
        query (str): search string
        es_client (elasticsearch.client.Elasticsearch): elasticsearch client.
        operator (string): operator to use in query. Can be "or" or "and".
        index_name (string): name of the index to use.
        field_name (string): name of the field to match in search

    Returns:
        count (int): a list of search results
    """
    if index_name is None:
        index_name = es_client.index_name

    if field_name is None:
        field_name = es_client.field_name

    # if a single field is specified, we make a list out of it
    if not is_list_or_tuple(field_name):
        field_name = [field_name]

    query_dsl = {
        'query': {
            'multi_match': {
                'query': query,
                'operator': operator,
                'fields': field_name
            }
        }
    }

    return raw_count(query_dsl, es_client, index_name)


def raw_count(query_dsl, es_client, index_name=None):
    if index_name is None:
        index_name = es_client.index_name
    raw_results = es_client.count(index=index_name, body=query_dsl)
    count = int(raw_results['count'])
    return count


def create_index(
        es_client, index_name=None, index_settings=None,
        allow_if_not_deleted=False):
    """Create index in Elasticsearch

    Args:
        index_name (string)
        index_settings_path (string)
        es_client (utils.elastic.EsClient)

    Returns:
        success (bool)
    """

    if not isinstance(index_settings, dict):

        # load settings and mapping for Elasticsearch
        with open(index_settings) as f:
            index_settings = json.load(f)

    if index_name is None:
        index_name = es_client.index_name

    # delete current index if index extists already
    if es_client.indices.exists(index=index_name):
        msg = 'Index "{}" exists; type name of index to delete it: '.format(
            index_name)
        resp = input(msg)
        if resp.strip() == index_name:
            es_client.indices.delete(index=index_name)
        elif allow_if_not_deleted:
            return False
        else:
            msg = 'ABORTING: "{}" != "{}".'.format(index_name, resp.strip())
            print(msg, file=sys.stderr)
            exit(1)

    # create new index
    es_client.indices.create(index=index_name, body=index_settings)

    return True


def retrieve_termvectors(
        documents_ids, es_client, doc_type=None, index_name=None, fields=None,
        term_offsets=False, term_positions=False, term_statistics=False,
        field_statistics=False):

    if len(documents_ids) == 0:
        return []

    if index_name is None:
        index_name = es_client.index_name

    if doc_type is None:
        doc_type = es_client.doc_type

    req = {
        'ids': ','.join(documents_ids), 'doc_type': doc_type,
        'index': index_name, 'term_statistics': term_statistics,
        'positions': term_positions, 'field_statistics': field_statistics,
        'offsets': term_offsets
    }

    if fields is not None:
        req['fields'] = ','.join(fields)

    termvectors = es_client.mtermvectors(**req)['docs']
    return termvectors


def retrieve_documents(
        documents_ids, es_client, index_name=None, doc_type=None, source=True):
    """Retrieves multiple documents in one hit

    Args:
        documents_ids (list): list of identifiers of documents
        es_client (elasticsearch.client.Elasticsearch): elasticsearch client.
        index_name (string): name of the index to use; if no index name
            is provided, the name in opts.index_name is used.
        doc_type (string): type of document to retrieve; if none is provided,
            the one in opts.doc_type is used.
        source (bool or list): if True, all fields of the document are
            retrieved; if False, none is retrieved. If source is a list,
            the fields whose names are in the list are retrieved. Defaults
            to True.
    """

    if len(documents_ids) == 0:
        return []

    if index_name is None:
        index_name = es_client.index_name

    if doc_type is None:
        doc_type = es_client.doc_type

    results = es_client.mget(index=index_name, doc_type=doc_type,
                             body={'ids': documents_ids}, _source=source)

    return results['docs']


def index_in_bulk(
        documents, es_client,
        index_name=None, bulk_size_in_bytes=5000000, all_docs=False,
        verbose=False):
    """Index data in documents in bulk

    Args:
        documents (dict or iterable): dictionary of <doc_id:content> or
            iterable returning tuples (doc_id, content)
        es_client (elasticsearch.client.Elasticsearch): elasticsearch client.
        index_name (str): name of the index; if none is provided,
            index_in_bulk will attempt at using es_client.index_name instead.
        bulk_size_in_bytes (int): size of each operation in bytes. This
            parameter is ignored in all_docs is set to True.
        all_docs (bool): whether to scan lall mo

    Returns:
        cnt_total (integer): total number of indexed documents
        skipped (list): list of documents that have been skipped
    """

    skipped = []

    if not all_docs and bulk_size_in_bytes > 104857600:
        # this is the max size of transmission for a default
        # configuration of Elasticsearch
        msg = 'Bulk size exceeds {:.2f} MB'.format(104.857600)
        raise ValueError(msg)

    if index_name is None:
        index_name = es_client.index_name

    # initialize an operations counter and an operations collector
    cnt_total = size_ops = 0
    operations = []

    if isinstance(documents, collections.Iterable):
        documents_it = documents
    else:
        documents_it = documents.items()

    start = time.time()
    cnt_docs = 0

    for doc in documents_it:
        cnt_docs += 1

        # append appropriate operations
        new_op = [
            {
                'create': {
                    '_index': index_name,
                    '_type': doc.pop('_type'),
                    '_id': doc.pop('_id')
                }
            },
            doc
        ]

        size_new_op = len_utf8(json.dumps(new_op))

        if not all_docs and size_new_op > bulk_size_in_bytes:
            skipped.extend(
                [e['create']['_id'] for e in new_op if 'create' in e]
            )

            msg = (
                'WARNING: size of {} is {:,} bytes, maximum size is {:,}; '
                'the document will be ignored.'.format(
                    new_op[0]['create']['_id'],
                    size_new_op, bulk_size_in_bytes))
            warnings.warn(msg)
            continue

        if not all_docs and (size_ops + size_new_op > bulk_size_in_bytes):
            es_client.bulk(body=operations)

            if verbose:
                delta = time.time() - start
                msg = (
                    'processed {:,} in {:.0f} s ({:.1e} per doc)'
                    ''.format(
                        cnt_docs, delta,
                        delta / cnt_docs if cnt_docs > 0 else 0
                    )
                )
                print(msg)

            # empty list
            del operations[:]
            size_ops = 0

        size_ops += size_new_op
        operations.extend(new_op)
        cnt_total += 1

    if len(operations) > 1:

        # do last operation (which could be the only operation this
        # method does.)
        es_client.bulk(body=operations)

    if verbose:
        delta = time.time() - start
        msg = (
            'total processed: {:,} in {:.0f} s ({:.1e} per doc)'
            ''.format(
                cnt_docs, delta, delta / cnt_docs if cnt_docs > 0 else 0
            )
        )
        print(msg)

    return cnt_total, skipped


def get_scroll(query_dsl, es_client, index_name=None, keep_alive='1m'):
    """Returns an iterator for results matching query_dsl."""

    if index_name is None:
        index_name = es_client.index_name

    scroll = scan(
        es_client, query=query_dsl, scroll=keep_alive, index=index_name)

    return scroll


def batch_count(
        terms, es_client, index_name=None, field_name=None, batch_size=50,
        operator='and'):
    if index_name is None:
        index_name = es_client.index_name
    if field_name is None:
        field_name = es_client.field_name

    if not is_list_or_tuple(field_name):
        field_name = [field_name]

    # create and format queries to work with the batch apis
    queries_dsl = [
        {
            'size': 0,
            'query': {
                'query_string': {
                    'query': escape(term),
                    'default_operator': operator,
                    'fields': field_name
                }
            },
        }
        for term in terms
    ]

    resp = msearch(queries_dsl, es_client, index_name, batch_size)

    terms_cnt = [r['hits']['total'] for r in resp['responses']]
    return terms_cnt


def batch_search(
        queries, es_client, index_name=None, field_name=None, batch_size=50,
        operator='or', retrieved_fields=None, maxsize=None):
    if index_name is None:
        index_name = es_client.index_name
    if field_name is None:
        field_name = es_client.field_name

    if not is_list_or_tuple(field_name):
        field_name = [field_name]

    # create and format queries to work with the batch apis
    # just like for simple_search, we use a multi match query

    queries_dsl = [
        {
            'query': {
                'query_string': {
                    'query': str(query),
                    'default_operator': operator,
                    'fields': field_name
                }
            },
            'fields': retrieved_fields if retrieved_fields else [],
            'size': maxsize if maxsize else 1000
        }
        for query in queries
    ]

    resp = msearch(queries_dsl, es_client, index_name, batch_size)

    doc_matches = [r['hits']['hits'] for r in resp['responses']]
    return doc_matches


def msearch(queries_dsl, es_client, index_name=None, batch_size=50):
    if index_name is None:
        index_name = es_client.index_name

    index_dsl = {'index': index_name}

    formatted_queries_dsl = [
        '{}\n{}'.format(json.dumps(index_dsl), json.dumps(query_dsl))
        for query_dsl in queries_dsl
    ]

    # send queries in batches
    resp = batch_func(
        apply_func=es_client.msearch,
        combine_func=lambda x: '\n'.join(x),
        batch_data=formatted_queries_dsl,
        batch_size=batch_size,
        chain_func=lambda rs: {
            'responses': list(
                itertools.chain(*(r['responses'] for r in rs))
            )
        }
    )

    return resp
