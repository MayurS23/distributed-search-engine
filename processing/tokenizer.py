"""
processing/tokenizer.py - Text to normalised token stream.
CRITICAL: This same pipeline must be used at both index time and query time.
"""
import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional
from utils.logger import get_logger

log = get_logger(__name__)

try:
    from nltk.stem import PorterStemmer as _PorterStemmer
    _stemmer_class = _PorterStemmer
except ImportError:
    class _PorterStemmer:
        def stem(self, word):
            for s in ["ing","tion","ed","er","ly","ness","ment","s"]:
                if word.endswith(s) and len(word)-len(s)>2:
                    return word[:-len(s)]
            return word
    _stemmer_class = _PorterStemmer

_TOKEN_RE = re.compile(r"[a-z0-9]+(?:['\-][a-z0-9]+)*")

_STOP_WORDS = frozenset({
    "i","me","my","myself","we","our","ours","ourselves","you","your","yours",
    "yourself","yourselves","he","him","his","himself","she","her","hers",
    "herself","it","its","itself","they","them","their","theirs","themselves",
    "what","which","who","whom","this","that","these","those","am","is","are",
    "was","were","be","been","being","have","has","had","having","do","does",
    "did","doing","a","an","the","and","but","if","or","because","as","until",
    "while","of","at","by","for","with","about","against","between","into",
    "through","during","before","after","above","below","to","from","up","down",
    "in","out","on","off","over","under","again","further","then","once","here",
    "there","when","where","why","how","all","both","each","few","more","most",
    "other","some","such","no","nor","not","only","own","same","so","than","too",
    "very","s","t","can","will","just","don","should","now","d","ll","m","o",
    "re","ve","y","ain","aren","couldn","didn","doesn","hadn","hasn","haven",
    "isn","ma","mightn","mustn","needn","shan","shouldn","wasn","weren","won","wouldn",
    "http","https","www","com","org","net","edu","gov","html","htm","php",
    "click","read","see","also","via","page","site","website","web","link","url",
    "copyright","rights","reserved","privacy","policy","cookie","cookies","terms",
})

_MIN_TOKEN_LEN = 2
_MAX_TOKEN_LEN = 64

@dataclass(frozen=True)
class Token:
    term: str
    raw: str
    position: int

@dataclass
class TokenStream:
    tokens: list[Token]
    token_count: int

    @property
    def terms(self) -> list[str]:
        return [t.term for t in self.tokens]

    @property
    def unique_terms(self) -> set[str]:
        return set(self.terms)

    def term_frequencies(self) -> dict[str, int]:
        freq: dict[str, int] = {}
        for token in self.tokens:
            freq[token.term] = freq.get(token.term, 0) + 1
        return freq

    def term_positions(self) -> dict[str, list[int]]:
        pos: dict[str, list[int]] = {}
        for token in self.tokens:
            pos.setdefault(token.term, []).append(token.position)
        return pos

class Tokenizer:
    def __init__(self) -> None:
        self._stemmer = _stemmer_class()

    def tokenize(self, text: str, count_all: bool = True) -> TokenStream:
        if not text:
            return TokenStream(tokens=[], token_count=0)
        text = unicodedata.normalize("NFC", text).lower()
        raw_tokens = _TOKEN_RE.findall(text)
        total_count = len(raw_tokens) if count_all else 0
        tokens: list[Token] = []
        position = 0
        for raw in raw_tokens:
            if not (_MIN_TOKEN_LEN <= len(raw) <= _MAX_TOKEN_LEN):
                continue
            if raw in _STOP_WORDS:
                continue
            stemmed = self._stem(raw)
            tokens.append(Token(term=stemmed, raw=raw, position=position))
            position += 1
        return TokenStream(tokens=tokens, token_count=total_count if count_all else position)

    def tokenize_query(self, query: str) -> list[str]:
        return self.tokenize(query, count_all=False).terms

    @lru_cache(maxsize=50_000)
    def _stem(self, word: str) -> str:
        return self._stemmer.stem(word)

    def is_stop_word(self, word: str) -> bool:
        return word.lower() in _STOP_WORDS

_tokenizer_instance: Optional[Tokenizer] = None

def get_tokenizer() -> Tokenizer:
    global _tokenizer_instance
    if _tokenizer_instance is None:
        _tokenizer_instance = Tokenizer()
    return _tokenizer_instance
