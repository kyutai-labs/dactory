from dactory import compute_minhash_signature


class TestComputeMinHashSignature:
    def test_basic_output_length(self):
        sig = compute_minhash_signature("hello world this is a test", 8, 5)
        assert len(sig) == 8

    def test_identical_texts_same_signature(self):
        text = "The quick brown fox jumps over the lazy dog"
        sig1 = compute_minhash_signature(text, 8, 5)
        sig2 = compute_minhash_signature(text, 8, 5)
        assert sig1 == sig2

    def test_different_texts_different_signatures(self):
        sig1 = compute_minhash_signature("completely different text one", 8, 5)
        sig2 = compute_minhash_signature("another unrelated text two", 8, 5)
        assert sig1 != sig2

    def test_similar_texts_similar_signatures(self):
        text1 = "The quick brown fox jumps over the lazy dog near the park"
        text2 = "The quick brown fox jumps over the lazy cat near the park"
        sig1 = compute_minhash_signature(text1, 8, 5)
        sig2 = compute_minhash_signature(text2, 8, 5)
        # Similar texts should have some matching hash values
        matches = sum(1 for a, b in zip(sig1, sig2) if a == b)
        assert matches > 0

    def test_short_text_returns_max(self):
        sig = compute_minhash_signature("hi", 8, 5)
        assert all(v == 2**64 - 1 for v in sig)

    def test_empty_text_returns_max(self):
        sig = compute_minhash_signature("", 4, 5)
        assert len(sig) == 4
        assert all(v == 2**64 - 1 for v in sig)

    def test_num_perm_respected(self):
        for n in [1, 4, 8, 16, 64]:
            sig = compute_minhash_signature("some longer text for testing", n, 5)
            assert len(sig) == n
