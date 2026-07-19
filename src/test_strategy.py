"""Deterministic checks for the v2 quality-first entry conditions."""

import unittest

from src.strategy import MTFAnalyzer, StrategyEngine, _efficiency_ratio


class EfficiencyRatioTests(unittest.TestCase):
    def test_perfectly_straight_move_scores_one(self):
        self.assertAlmostEqual(_efficiency_ratio([100, 101, 102, 103]), 1.0)

    def test_pure_noise_scores_near_zero(self):
        # Ends where it started: zero net progress over a nonzero path.
        self.assertAlmostEqual(_efficiency_ratio([100, 101, 100, 101, 100]), 0.0)

    def test_net_directional_move_with_a_counter_tick_still_scores_well(self):
        # 4 down-ticks and 1 larger up counter-tick, net still strongly down.
        # A pure tick-counting filter (needing 70% of ticks to agree) would
        # reject this 4-of-5 = 80%... but a noisier, more realistic mix
        # would fail tick-counting even though ER still reflects real
        # net progress.
        sample = [100, 99.9, 99.8, 99.9, 99.7, 99.6]
        er = _efficiency_ratio(sample)
        self.assertGreater(er, 0.5)


class StrategyTests(unittest.TestCase):
    def test_requires_pullback_then_two_continuation_ticks(self):
        strategy = StrategyEngine()
        strategy.update_mtf_bias("UP", 3)
        for price in range(100, 108):
            self.assertIsNone(strategy.process_tick(float(price)))
        self.assertIsNone(strategy.process_tick(106.5))  # one-tick pullback
        self.assertIsNone(strategy.process_tick(107.5))  # continuation one
        self.assertEqual(strategy.process_tick(108.5), "BUY")  # continuation two

    def test_burst_path_signals_faster_than_classic_window(self):
        # Only 6 ticks total - too few for the classic 8-tick minimum - but a
        # strong, efficient burst should still be recognised via the burst path.
        strategy = StrategyEngine()
        strategy.update_mtf_bias("UP", 3)
        for price in [100, 100.5, 101.0, 101.5, 102.0]:
            strategy.process_tick(price)
        self.assertEqual(strategy.get_state()["trend_kind"], "burst")
        self.assertIsNone(strategy.process_tick(101.8))  # one-tick pullback
        self.assertIsNone(strategy.process_tick(102.2))  # continuation one
        self.assertEqual(strategy.process_tick(102.6), "BUY")  # continuation two

    def test_rejects_setup_below_mtf_minimum_agreement(self):
        # Default MTF_MIN_AGREEMENT is 2-of-3; only 1 timeframe agreeing
        # should still be rejected.
        strategy = StrategyEngine()
        strategy.update_mtf_bias("UP", 1)
        for price in range(100, 108):
            strategy.process_tick(float(price))
        strategy.process_tick(106.5)
        strategy.process_tick(107.5)
        self.assertIsNone(strategy.process_tick(108.5))

    def test_accepts_setup_with_majority_mtf_agreement(self):
        # 2-of-3 is now sufficient by default (was unanimous 3-of-3 before).
        strategy = StrategyEngine()
        strategy.update_mtf_bias("UP", 2)
        for price in range(100, 108):
            strategy.process_tick(float(price))
        strategy.process_tick(106.5)
        strategy.process_tick(107.5)
        self.assertEqual(strategy.process_tick(108.5), "BUY")

    def test_sensitivity_is_configurable_per_instance(self):
        # A stricter instance (e.g. "Conservative" preset) should reject a
        # setup that a looser instance accepts.
        strict = StrategyEngine(velocity_threshold=0.95, burst_threshold=0.98, mtf_min_agreement=3)
        strict.update_mtf_bias("UP", 3)
        noisy_up_ticks = [100, 100.4, 100.3, 100.7, 100.6, 101.0, 100.9, 101.3]
        for price in noisy_up_ticks:
            strict.process_tick(price)
        self.assertIsNone(strict.get_state()["trend_direction"])


class MTFAnalyzerTests(unittest.TestCase):
    def test_default_accepts_two_of_three_majority(self):
        analyzer = MTFAnalyzer()  # default min_agreement=2
        candles = {
            "5m": [{"close": 1}, {"close": 2}, {"close": 3}],
            "15m": [{"close": 1}, {"close": 2}, {"close": 3}],
            "30m": [{"close": 3}, {"close": 2}, {"close": 1}],  # dissenting
        }
        self.assertEqual(analyzer.analyze_with_strength(candles), ("UP", 2))

    def test_unanimous_still_works(self):
        analyzer = MTFAnalyzer()
        all_up = {key: [{"close": 1}, {"close": 2}, {"close": 3}] for key in ("5m", "15m", "30m")}
        self.assertEqual(analyzer.analyze_with_strength(all_up), ("UP", 3))

    def test_strict_instance_requires_unanimous(self):
        analyzer = MTFAnalyzer(min_agreement=3)
        candles = {
            "5m": [{"close": 1}, {"close": 2}, {"close": 3}],
            "15m": [{"close": 1}, {"close": 2}, {"close": 3}],
            "30m": [{"close": 3}, {"close": 2}, {"close": 1}],
        }
        self.assertEqual(analyzer.analyze_with_strength(candles), (None, 2))


if __name__ == "__main__":
    unittest.main()
