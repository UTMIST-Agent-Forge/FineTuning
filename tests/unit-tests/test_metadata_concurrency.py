import unittest
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Dict, Any
from src.DataPreProcessing.extractMetaData import Metadata

class TestMetadataConcurrency(unittest.TestCase):
    def _one_parallel_round(self) -> tuple[Dict[str, Any], Dict[str, Any]]:
        meta = Metadata(add_word_count=True, add_sentence_count=True)
        barrier = threading.Barrier(2)

        r_long = {"text": "lorem ipsum dolor sit amet consectetur adipiscing elit"}
        r_short = {"text": "hi"}

        def run(record: Dict[str, Any]) -> Dict[str, Any]:
            barrier.wait()  # start both at once
            meta.process(record)
            return dict(meta.get_config())

        with ThreadPoolExecutor(max_workers=2) as ex:
            f1 = ex.submit(run, r_long)
            f2 = ex.submit(run, r_short)
            wait([f1, f2])

        return f1.result(), f2.result()

    def test_parallel_process_does_not_corrupt_config(self) -> None:
        """
        On a correct (stateless) implementation:
          - Configs from both threads are identical.
          - Config keys are only the static flags.
        On a stateful implementation that leaks last results:
          - This test will often fail within a few rounds.
        """
        rounds = 25
        for _ in range(rounds):
            cfg1, cfg2 = self._one_parallel_round()
            # Assert only static keys are present
            self.assertEqual(set(cfg1.keys()), {"add_word_count", "add_sentence_count"})
            self.assertEqual(set(cfg2.keys()), {"add_word_count", "add_sentence_count"})
            # And configs are identical
            self.assertEqual(cfg1, cfg2)

    def test_parallel_rounds_expose_no_state_leak(self) -> None:
        """
        Verify that under repeated parallel runs there are NO leaks:
        - Both threads return identical configs.
        - Configs contain only the two static flags.
        - Config does not fluctuate across rounds.
        """
        rounds = 50
        diffs = 0
        seen = set()

        for _ in range(rounds):
            cfg1, cfg2 = self._one_parallel_round()

            # Only static keys
            self.assertEqual(set(cfg1.keys()), {"add_word_count", "add_sentence_count"})
            self.assertEqual(set(cfg2.keys()), {"add_word_count", "add_sentence_count"})

            # No per-record fields sneaking in
            self.assertNotIn("word_count", cfg1)
            self.assertNotIn("sentence_count", cfg1)
            self.assertNotIn("word_count", cfg2)
            self.assertNotIn("sentence_count", cfg2)

            # Threads agree
            if cfg1 != cfg2:
                diffs += 1

            # Track stability across rounds
            seen.add(tuple(sorted(cfg1.items())))

        self.assertEqual(diffs, 0, "Configs differed between threads in at least one round")
        self.assertEqual(len(seen), 1, "Config fluctuated across rounds; expected a single stable config")






