from dactory import compute_long_words


class TestComputeLongWords:
    """Tests for `compute_long_words` function"""

    error_margin = 1e-6

    def test_empty_string(self):
        text = ""
        res = compute_long_words(text, min_length=0)
        expected = 0.0
        assert res == expected

    def test_no_long_words(self):
        text = "shrot words only"
        res = compute_long_words(text, min_length=6)
        expected = 0.0
        assert res == expected

    def test_all_long_words(self):
        text = "these are some longwords"
        res = compute_long_words("these are some longwords", min_length=3)
        expected = 21 / len(text)
        assert abs(res - expected) < self.error_margin

    def test_mixed_words(self):
        text = "this sentence contains some longwords"
        res = compute_long_words(text, min_length=8)
        expected = 25 / len(text)
        assert abs(res - expected) < self.error_margin
