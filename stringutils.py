import re
import string

def len_utf8(s):
    """Return the length of s in bytes"""
    return len(s.encode('utf-8'))


class SimpleTokenizer(object):
    def __init__(self, stopwords=None, min_length=1, split_sym=None):
        if not stopwords:
            stopwords = [
                'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for',
                'from', 'has', 'he', 'in', 'is', 'its', 'of', 'on', 'or',
                'that', 'the', 'to', 'was ', 'were', 'will', 'with'
            ]
        self.stopwords = set(stopwords)

        if split_sym is None:
            split_sym = []

        split_sym = string,punctuation + ''.join(split_sym)

        self.min_length = min_length
        # self.re_tokenize = re.compile(
        #     r'\s|' + r'|'.join(re.escape(p) for p in split_sym))
        self.re_tokenize = re.compile(r'&\w+;|\W+|_')

    def tokenize(self, text):
        """Tokenize text

        Args:
            text (unicode): text to tokenizer

        Yields:
            tok (unicode): token
        """
        for tok in self.re_tokenize.split(text.lower()):
            if len(tok) >= self.min_length and tok not in self.stopwords:
                yield tok


def string_similarity(x, y, similarity_name, n=3):
    """Calculate approximate string similarity; for convinience reasons,
    strings are split into n-grams"""
    X, Y = set(make_ngrams(x, n)), set(make_ngrams(y, n))
    if similarity_name == 'dice':
        return 2 * len (X & Y) / (len(X) + len(Y))
    elif similarity_name == 'jaccard':
        return len(X & Y) / len(X | Y)
    elif similarity_name == 'cosine':
        return len(X & Y) / numpy.sqrt(len(X) * len(Y))
    elif similarity_name == 'overlap':
        return len(X & Y)
    else:
        msg = 'Similarity {} not recognized'.format(similarity_name)
        raise TypeError(msg)
