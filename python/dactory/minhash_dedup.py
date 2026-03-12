from datasketch import MinHash, MinHashLSH

from dactory import compute_minhash_signature


class MinHashDeduplicator:
    def __init__(self, threshold: float = 0.8, num_perm: int = 128, ngram_size: int = 5):
        self.threshold = threshold
        self.num_perm = num_perm
        self.ngram_size = ngram_size
        self.lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self._counter = 0

    def is_duplicate(self, text: str) -> bool:
        sig = compute_minhash_signature(text, self.num_perm, self.ngram_size)
        mh = MinHash(num_perm=self.num_perm)
        mh.hashvalues[:] = sig
        if self.lsh.query(mh):
            return True
        self.lsh.insert(str(self._counter), mh)
        self._counter += 1
        return False
