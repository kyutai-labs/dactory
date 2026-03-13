from dactory import compute_c4_metrics


class TestComputeC4Metrics:
    error_margin = 1e-3

    def test_basic_clean_text(self):
        text = "The quick brown fox jumps over the lazy dog.\nThis is a test sentence.\nAnother line here.\nMore content follows.\nFinal sentence here."
        metrics = compute_c4_metrics(text)
        assert metrics["has_curly_brackets"] == 0.0
        assert metrics["has_javascript"] == 0.0
        assert metrics["has_lorem_ipsum"] == 0.0
        assert metrics["num_sentences"] == 5.0
        assert metrics["policy_ratio"] == 0.0

    def test_empty_text(self):
        metrics = compute_c4_metrics("")
        assert metrics["has_curly_brackets"] == 0.0
        assert metrics["has_javascript"] == 0.0
        assert metrics["has_lorem_ipsum"] == 0.0
        assert metrics["num_sentences"] == 0.0
        assert metrics["policy_ratio"] == 0.0

    def test_curly_brackets_detected(self):
        text = "function test() { return true; }"
        metrics = compute_c4_metrics(text)
        assert metrics["has_curly_brackets"] == 1.0

    def test_no_curly_brackets(self):
        text = "This is normal text without any code."
        metrics = compute_c4_metrics(text)
        assert metrics["has_curly_brackets"] == 0.0

    def test_javascript_detected_lowercase(self):
        text = "Please enable javascript in your browser."
        metrics = compute_c4_metrics(text)
        assert metrics["has_javascript"] == 1.0

    def test_javascript_detected_mixed_case(self):
        text = "This page requires JavaScript to work."
        metrics = compute_c4_metrics(text)
        assert metrics["has_javascript"] == 1.0

    def test_no_javascript(self):
        text = "This is a normal web page."
        metrics = compute_c4_metrics(text)
        assert metrics["has_javascript"] == 0.0

    def test_lorem_ipsum_detected(self):
        text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
        metrics = compute_c4_metrics(text)
        assert metrics["has_lorem_ipsum"] == 1.0

    def test_lorem_ipsum_case_insensitive(self):
        text = "This page contains LOREM IPSUM placeholder text."
        metrics = compute_c4_metrics(text)
        assert metrics["has_lorem_ipsum"] == 1.0

    def test_no_lorem_ipsum(self):
        text = "This is real content, not placeholder text."
        metrics = compute_c4_metrics(text)
        assert metrics["has_lorem_ipsum"] == 0.0

    def test_num_sentences_counts_terminal_punctuation(self):
        text = 'First sentence.\nSecond sentence!\nThird sentence?\nNo punctuation here\nQuoted end"'
        metrics = compute_c4_metrics(text)
        assert metrics["num_sentences"] == 4.0

    def test_num_sentences_zero(self):
        text = "No terminal punctuation\nAnother line without"
        metrics = compute_c4_metrics(text)
        assert metrics["num_sentences"] == 0.0

    def test_policy_ratio_single_policy_line(self):
        text = "Normal content.\nSee our privacy policy for details.\nMore content.\nEven more."
        metrics = compute_c4_metrics(text)
        assert abs(metrics["policy_ratio"] - 0.25) < self.error_margin

    def test_policy_ratio_multiple_terms(self):
        text = "Terms of use apply.\nCookie policy here.\nPrivacy policy.\nCopyright 2024."
        metrics = compute_c4_metrics(text)
        assert metrics["policy_ratio"] == 1.0

    def test_policy_ratio_case_insensitive(self):
        text = "Please read our PRIVACY POLICY.\nNormal line."
        metrics = compute_c4_metrics(text)
        assert abs(metrics["policy_ratio"] - 0.5) < self.error_margin

    def test_policy_ratio_zero(self):
        text = "Clean content.\nNo legal stuff.\nJust text."
        metrics = compute_c4_metrics(text)
        assert metrics["policy_ratio"] == 0.0
