# built-in modules
import re
import string

# installed modules
import numpy


STOPWORDS = [
    'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for',
    'from', 'has', 'he', 'in', 'is', 'its', 'of', 'on', 'or',
    'that', 'the', 'to', 'was ', 'were', 'will', 'with'
]


def len_utf8(s):
    """Return the length of s in bytes"""
    return len(s.encode('utf-8'))


class SimpleTokenizer(object):
    def __init__(
            self, stopwords=None, min_length=1, split_sym=None, numbers=True):
        if not stopwords:
            stopwords = STOPWORDS
        self.stopwords = set(stopwords)

        if not numbers:
            self.is_number = lambda s: re.match('\d+', s) is not None
        else:
            self.is_number = lambda s: False

        if split_sym is None:
            split_sym = []

        split_sym = string.punctuation + ''.join(split_sym)

        self.min_length = min_length
        # self.re_tokenize = re.compile(
        #     r'\s|' + r'|'.join(re.escape(p) for p in split_sym))
        self.re_tokenize = re.compile(r'&\w+;|\W+|_')

    def __call__(self, text):
        return self.tokenize(text)

    def tokenize(self, text):
        """Tokenize text

        Args:
            text (unicode): text to tokenizer

        Yields:
            tok (unicode): token
        """
        for tok in self.re_tokenize.split(text.lower()):
            if (
                len(tok) >= self.min_length and
                tok not in self.stopwords and
                not(self.is_number(tok))
            ):
                yield tok


def make_ngrams(s, n):
    s = u'{out}{s}{out}'.format(out='$' * (n - 1), s=s)
    return (s[i:i + n] for i in range(len(s) - n + 1))


def string_similarity(x, y, similarity_name, n=3):
    """Calculate approximate string similarity; for convinience reasons,
    strings are split into n-grams"""
    X, Y = set(make_ngrams(x, n)), set(make_ngrams(y, n))
    if similarity_name == 'dice':
        return 2 * len(X & Y) / (len(X) + len(Y))
    elif similarity_name == 'jaccard':
        return len(X & Y) / len(X | Y)
    elif similarity_name == 'cosine':
        return len(X & Y) / numpy.sqrt(len(X) * len(Y))
    elif similarity_name == 'overlap':
        return len(X & Y) / max(len(X), len(Y))
    elif similarity_name == 'levenshtein':
        return 1 - levenshtein(x, y) / max(len(x), len(y))
    else:
        msg = 'Similarity {} not recognized'.format(similarity_name)
        raise TypeError(msg)


def levenshtein(s1, s2):
    if len(s1) < len(s2):
        return levenshtein(s2, s1)

    # len(s1) >= len(s2)
    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # j+1 instead of j since previous_row and
            # current_row are one character longer
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1       # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]
