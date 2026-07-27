# MomentumMaster

MomentumMaster is a Streamlit trading terminal for Deriv’s Volatility 10 (1s) Index using a tick-momentum and pullback strategy.

This build uses Deriv’s current Personal Access Token flow:

1. List active Options accounts.
2. Request a short-lived account-specific WebSocket URL.
3. Stream ticks.
4. Request a proposal.
5. Buy the proposal.
6. Monitor contract settlement.

## Risk notice

This software can submit demo or real-money orders.

It does not guarantee correct signals, fills, uptime, or profit.

Use a demo account first. You are responsible for credentials, configuration, trading decisions, and losses.

## Important risk configuration

This build intentionally does **not** add maximum or minimum drawdown limits.

Martingale risk beyond the existing martingale configuration is left to the operator.

However, the martingale step semantics are now explicit:

```text
DEFAULT_MAX_MARTINGALE_STEPS = total stake levels, including the initial stake.