# built-in modules
import os
import math
from subprocess import Popen, PIPE
from collections import OrderedDict
from tempfile import NamedTemporaryFile


TREC_EVAL_PATH = 'bin/trec_eval'
SAMPLE_EVAL_PATH = 'bin/sample_eval.pl'

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
