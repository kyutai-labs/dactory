from dactory import compute_gopher_metrics


class TestComputeGopherMetrics:
    error_margin = 1e-3

    def test_basic_english_text(self):
        text = "The quick brown fox jumps over the lazy dog. This is a test sentence."
        metrics = compute_gopher_metrics(text, "en")
        assert 3.0 <= metrics["mean_word_length"] <= 10.0
        assert metrics["frac_words_with_alpha"] > 0.9
        assert metrics["frac_lines_end_punctuation"] > 0.0
        assert metrics["has_stop_words"] == 1.0

    def test_empty_text(self):
        metrics = compute_gopher_metrics("", "en")
        assert metrics["mean_word_length"] == 0.0
        assert metrics["frac_words_with_alpha"] == 0.0

    def test_bullet_lines(self):
        text = "- item one\n- item two\n- item three\n* item four"
        metrics = compute_gopher_metrics(text, "en")
        assert metrics["frac_lines_start_bullet"] == 1.0

    def test_numbered_bullet(self):
        text = "1. First item\n2. Second item\nNormal line"
        metrics = compute_gopher_metrics(text, "en")
        assert abs(metrics["frac_lines_start_bullet"] - 2.0 / 3.0) < self.error_margin

    def test_punctuation_ending(self):
        text = "This ends with punctuation.\nThis does not\nAnother sentence!"
        metrics = compute_gopher_metrics(text, "en")
        assert abs(metrics["frac_lines_end_punctuation"] - 2.0 / 3.0) < self.error_margin

    def test_alphabetic_fraction(self):
        text = "Hello World"
        metrics = compute_gopher_metrics(text, "en")
        # 10 alpha chars out of 11 total chars (including space)
        expected = 10.0 / 11.0
        assert abs(metrics["frac_alphabetic_chars"] - expected) < self.error_margin

    def test_numeric_text_low_alpha(self):
        text = "12345 67890 12345 67890"
        metrics = compute_gopher_metrics(text, "en")
        assert metrics["frac_alphabetic_chars"] == 0.0
        assert metrics["frac_words_with_alpha"] == 0.0

    def test_duplicate_sentences(self):
        text = "Hello world. Hello world. Unique sentence."
        metrics = compute_gopher_metrics(text, "en")
        assert metrics["frac_duplicate_sentences"] > 0.0

    def test_no_duplicate_sentences(self):
        text = "First sentence. Second sentence. Third sentence."
        metrics = compute_gopher_metrics(text, "en")
        assert metrics["frac_duplicate_sentences"] == 0.0

    def test_duplicate_paragraphs(self):
        text = "Paragraph one.\n\nParagraph two.\n\nParagraph one."
        metrics = compute_gopher_metrics(text, "en")
        assert metrics["frac_duplicate_paragraphs"] > 0.0

    def test_no_duplicate_paragraphs(self):
        text = "Paragraph one.\n\nParagraph two.\n\nParagraph three."
        metrics = compute_gopher_metrics(text, "en")
        assert metrics["frac_duplicate_paragraphs"] == 0.0

    def test_stop_words_french(self):
        text = "Le chat est sur le tapis. Il a une belle couleur."
        metrics = compute_gopher_metrics(text, "fr")
        assert metrics["has_stop_words"] == 1.0

    def test_no_stop_words_wrong_language(self):
        text = "The quick brown fox jumps over the lazy dog."
        metrics = compute_gopher_metrics(text, "fr")
        # English text checked against French stop words should fail
        assert metrics["has_stop_words"] == 0.0

    def test_unknown_language_stop_words(self):
        text = "Some random text here."
        metrics = compute_gopher_metrics(text, "xx")
        # Unknown language defaults to passing
        assert metrics["has_stop_words"] == 1.0
