# MomentumMaster

MomentumMaster is a Streamlit trading terminal for Deriv’s Volatility 10 (1s) Index.

It uses a tick-momentum and pullback strategy with:

- Deriv PAT authentication
- account-specific OTP WebSocket connection
- live tick streaming
- proposal prefetch
- validated proposal buy
- contract settlement monitoring
- ambiguous-trade protection
- thread-safe UI state
- background async trading engine

## Risk notice

This software can submit demo or real-money orders.

It does not guarantee correct signals, fills, uptime, or profit.

Use a demo account first.

You are responsible for credentials, configuration, trading decisions, and losses.

## Project files

```text
requirements.txt
config.py
dashboard.py
README.md
src/logger.py
src/state_manager.py
src/api_client.py
src/strategy.py
src/trading_engine.py