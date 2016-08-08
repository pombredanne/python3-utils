import re
import string

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

