# built-in modules
import re
import os
import shutil
import shlex
import subprocess
import tempfile

# project modules
from .hashing import hash_obj
from .core import is_windows


class TerrierOptions(object):
    """Store options for terrier"""

    def __init__(self, _value=None, _parent=None, _name=None, **kwargs):
        self._value = _value
        self._parent = _parent
        self._name = _name
        self._children = {}

        for kw, val in kwargs.items():
            self[kw] = val

    def __getitem__(self, kw):
        return self._children[kw]

    def __setitem__(self, kw, val):
        try:
            head, other = re.split(r'[_\.]', kw, maxsplit=1)
            child = self._children.setdefault(
                head, self.__class__(_parent=self, _name=head)
            )
            child[other] = val
        except ValueError:
            ch = self._children.setdefault(
                kw, self.__class__(_value=val, _parent=self, _name=kw)
            )
            ch._value = val
            ch._name = kw
            ch._parent = self

    @property
    def name(self):
        if self._parent is None:
            return self._name
        else:
            parent_name = self._parent.name
            if parent_name is None:
                return '-D{}'.format(self._name)
            else:
                return '{}.{}'.format(parent_name, self._name)

    @property
    def value(self):
        return self._value

    def __str__(self):
        output = []
        if self.value is not None:
            output.append('{}={}'.format(self.name, self.value))

        for ch in self._children.values():
            output.append(str(ch))

        return ' '.join(output)

    def __repr__(self):
        return self.__str__()

    def update(self, other):
        if self._name == other._name:
            self._value = other._value

        for kw, ch in other._children.items():
            if kw in self._children:
                self._children[kw].update(ch)
            else:
                self._children[kw] = ch


def load_options(dict_or_fp):
    """Load options for terrier"""

    raw_config = {}

    if isinstance(dict_or_fp, str):

        # configuration is a string, so we interpret it as a path
        # to the configuration file
        with open(dict_or_fp) as f:

            for ln in f:
                # we clean the line
                ln = ln.strip()

                # we ignore lines that are empty or are comment
                if not(ln) or ln.startswith('#'):
                    continue

                # parsing the
                kw, val = ln.split('=')
                raw_config[kw.replace('.', '_')] = val

    else:
        raw_config = dict_or_fp

    return TerrierOptions(**raw_config)


def _prepare_queries_for_terrier(queries):
    """
        Write the queries to a file to be used
        with SingleLineTRECQuery parser
    """
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
        for i, query in enumerate(queries):
            qid = getattr(query, 'id', i)
            f.write('{} {}'.format(qid, query))
            f.write('\n')
        dest_fp = f.name

    return dest_fp


def _generate_results_fp(queries):
    fp = os.path.join(
        tempfile.gettempdir(), '{}.terrier'.format(hash_obj(queries))
    )
    return fp


def _parse_results_fp(results_fp):
    """Parse results file"""
    results = []
    prev_qid = None
    with open(results_fp) as f:
        for ln in f:
            qid, _, did, rank, score, _ = ln.strip().split()
            if prev_qid != qid:
                results.append([])

            results[-1].append({'_id': did, '_score': float(score)})
            prev_qid = qid

    return results


def query(queries, terrier_path, terrier_options=None):
    query_fp = _prepare_queries_for_terrier(queries)
    results_fp = _generate_results_fp(queries)
    terrier_trec = 'trec_terrier.bat' if is_windows() else 'trec_terrier.sh'

    if terrier_options is None:
        terrier_options = TerrierOptions()

    terrier_options['trec.results.file'] = results_fp
    terrier_options['trec.topics'] = query_fp
    terrier_options['trec.topics.parser'] = 'SingleLineTRECQuery'
    terrier_options['SingleLineTRECQuery.tokenise'] = 'true'

    cmd = shlex.split(
        '{trec_terrier} -r {terrier_options}'.format(
            trec_terrier=os.path.join(terrier_path, 'bin', terrier_trec),
            terrier_options=terrier_options
        )
    )
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    msgout, msgerr = proc.communicate()

    if not os.path.exists(results_fp):
        # something went wrong, output not created
        raise IOError(msgerr.decode('utf-8'))

    results = _parse_results_fp(results_fp)

    os.remove(results_fp)
    os.remove('{}.settings'.format(results_fp))

    return results
