# built-in modules
import os
import math
import shlex
from subprocess import Popen, PIPE
from collections import OrderedDict
from tempfile import NamedTemporaryFile


TREC_EVAL_PATH = 'bin/trec_eval'
SAMPLE_EVAL_PATH = 'bin/sample_eval.pl'
UBIRE_PATH = 'bin/ubire-v0.1.0.jar'


def __guess_type(e):
    try:
        e = float(e)
        if e == math.floor(e):
            e = int(e)
    except ValueError:
        pass
    return e


def __call_trec_eval(
        formatted_results, qrels_path, trec_eval_path, trec_eval_flags=None):
    with NamedTemporaryFile(mode='w', delete=False) as tmpf:
        tmpf.write('\n'.join(formatted_results))
        results_path = tmpf.name

    if trec_eval_flags is None:
        trec_eval_flags = []
    cmd = (['./{}'.format(trec_eval_path)] +
           ['-q'] + trec_eval_flags +
           [qrels_path, results_path])

    try:
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        resp = proc.communicate()
        msg_out, msg_err = (msg.decode('utf-8') for msg in resp)
    except Exception:
        raise
    finally:
        os.remove(results_path)

    if len(msg_err) > 0:
        raise IOError(msg_err)

    results = {}
    for ln in msg_out.split('\n'):
        ln = ln.strip()
        if not ln:
            continue
        ln_split = ln.split()[:3]
        metric, qid, value = (__guess_type(elem.strip()) for elem in ln_split)
        results.setdefault(qid, OrderedDict()).setdefault(metric, value)
    return results


def __call_sample_eval(
        formatted_results, qrels_path, sample_eval_path,
        sample_eval_metrics=None):

    if sample_eval_metrics is not None:
        sample_eval_metrics = set(sample_eval_metrics)

    with NamedTemporaryFile(mode='w', delete=False) as tmpf:
        tmpf.write('\n'.join(formatted_results))
        results_path = tmpf.name

    cmd = (['./{}'.format(sample_eval_path), '-q', qrels_path, results_path])

    try:
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        resp = proc.communicate()
        msg_out, msg_err = (msg.decode('utf-8') for msg in resp)
    except Exception:
        raise
    finally:
        os.remove(results_path)

    if len(msg_err) > 0:
        raise IOError(msg_err)

    results = {}
    for ln in msg_out.split('\n'):
        ln = ln.strip()
        if not ln:
            continue
        ln_split = ln.split()[:3]
        metric, qid, value = (__guess_type(elem.strip()) for elem in ln_split)

        valid_metric = (
            sample_eval_metrics is None or
            metric in sample_eval_metrics
        )
        if not valid_metric:
            continue

        results.setdefault(qid, OrderedDict()).setdefault(metric, value)
    return results


def __call_ubire(
        formatted_results, qrels_path, ubire_path,
        qread_path, ubire_flags=None):

    with NamedTemporaryFile(mode='w', delete=False) as tmpf:
        tmpf.write('\n'.join(formatted_results))
        results_path = tmpf.name

    if ubire_flags is None:
        ubire_flags = []
    cmd = (
        'java -jar {ubire} --qrels-file={qrels} --qread-file={qread} '
        '--ranking-file={results} {flags}'.format(
            ubire=ubire_path, qrels=qrels_path, qread=qread_path,
            results=results_path, flags=' '.join(ubire_flags)))

    try:
        proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        resp = proc.communicate()
        msg_out, msg_err = (msg.decode('utf-8') for msg in resp)
    except Exception:
        raise
    finally:
        os.remove(results_path)

    if msg_err:
        raise IOError(msg_err)

    results = {}
    for ln in msg_out.split('\n'):
        ln = ln.strip()
        if not ln:
            continue
        data = ln.split()

        # the data on the line with the average metrics has 3 entries,
        # the data on the per-query metric has four entries.
        if len(data) == 3:
            metric, qid, value = data
        else:
            metric, qid, _, value = data

        results.setdefault(__guess_type(qid), OrderedDict()).setdefault(
            metric.strip().lower(), __guess_type(value.strip()))
    return results


def __make_trec_eval_results(run_name, queries_ids, elasticsearch_results):
    lines = []

    if all(len(r) == 0 for r in elasticsearch_results):
        raise ValueError("No result retrieved for any query")

    for qid, results in zip(queries_ids, elasticsearch_results):
        for rank, result in enumerate(results, start=0):
            lines.append('{q} 0 {d} {r} {s} {n}'.format(
                q=qid, d=result['_id'], r=rank,
                s=result['_score'], n=run_name))

    return lines


def run_ubire(
        run_name, queries_ids, elasticsearch_results,
        qrels_path, qread_path, ubire_path=UBIRE_PATH, ubire_flags=None):

    if ubire_flags is None:
        ubire_flags = ['--readability', '--rbp-p=0.8']

    formatted_results = __make_trec_eval_results(
        run_name, queries_ids, elasticsearch_results)

    output = __call_ubire(
        formatted_results, qrels_path, ubire_path, qread_path, ubire_flags)

    return output


def run_trec_eval(
        run_name, queries_ids, elasticsearch_results,
        qrels_path, trec_eval_path=TREC_EVAL_PATH, trec_eval_flags=None):

    formatted_results = __make_trec_eval_results(
        run_name, queries_ids, elasticsearch_results)

    output = __call_trec_eval(
        formatted_results, qrels_path, trec_eval_path, trec_eval_flags)

    return output


def run_sample_eval(
        run_name, queries_ids, elasticsearch_results,
        qrels_path, sample_eval_path=SAMPLE_EVAL_PATH,
        sample_eval_metrics=None):

    formatted_results = __make_trec_eval_results(
        run_name, queries_ids, elasticsearch_results)

    output = __call_sample_eval(
        formatted_results, qrels_path, sample_eval_path, sample_eval_metrics)

    return output
