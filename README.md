# MomentumMaster

MomentumMaster is a personal Streamlit dashboard for a momentum-and-pullback strategy on Deriv’s **Volatility 10 (1s) Index**. This repaired build uses Deriv’s current Personal Access Token flow: it retrieves the selected Options account, requests a short-lived account-specific WebSocket URL, streams market data, obtains a price proposal, purchases that exact proposal, and monitors the resulting contract.[1] [2]

> **Risk notice:** This software can submit demo or real-money orders. It does not guarantee correct signals, fills, uptime, or profit. Begin with a demo account and a small stake. You remain responsible for credentials, configuration, trading decisions, and losses.

## What was repaired

The production blocker was the proposal request. Current Deriv schemas require `underlying_symbol`; the previous bot sent the obsolete `symbol` property, which the live API rejects. The repaired client and engine also address related failures that could otherwise deadlock requests, reuse the wrong quote, lose track of a filled order, or misreport an unresolved contract.[3] [4]

| Area | Repaired behavior |
| --- | --- |
| PAT and account connection | Uses `Deriv-App-ID` plus bearer PAT for account discovery and the account-specific OTP WebSocket URL; inactive accounts are rejected. |
| Proposal requests | Sends `underlying_symbol`, validates the proposal envelope, and retains the dashboard’s selected barrier. |
| WebSocket correlation | Correlates responses by `req_id` without awaiting tick callbacks inside the listener, preventing callback-to-request deadlocks. |
| Quote prefetch | Keeps proposals per engine instance and reuses one only when direction, contract type, stake, barrier, connection, and age match. |
| Buy requests | Buys a validated proposal ID at its ask price and never blindly retries an ambiguous buy. |
| Missing buy receipt | Reconnects if needed, checks the current portfolio schema, adopts only one uniquely matching untracked fill, and otherwise stops in `UNKNOWN` state. |
| Contract monitoring | Polls `proposal_open_contract`, reconnects safely, uses Deriv’s settlement fields, and does not classify an unconfirmed outcome as a loss. |
| Stop and reconnect lifecycle | Prevents duplicate reconnects, preserves the active trade task across a reconnect, blocks a buy if Stop was pressed before submission, and finishes active-contract tracking before normal shutdown. |
| Failure reporting | A failure before purchase is `CANCELLED`; any ambiguous or post-purchase failure is `UNKNOWN`, stops further orders, and leaves Martingale unchanged. |

## Required Deriv credentials

Create a Deriv application and Personal Access Token with permissions sufficient to list the account and trade. Keep the PAT private; do not place it in source control, a URL, chat, screenshots, or logs. Deriv documents the REST headers and account-specific Options WebSocket flow in its current API overview and WebSocket guide.[1] [2]

For Streamlit Cloud, open **App settings → Secrets** and add:

```toml
DERIV_APP_ID = "YOUR_PAT_APP_ID"
DERIV_API_TOKEN = "YOUR_DERIV_PAT"
```

For local use, copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml` and replace only the placeholders. The real `secrets.toml`, `.env`, logs, and Python caches are excluded by `.gitignore`.

## Installation and startup

Use Python 3.11 or newer. From the project directory, create an isolated environment, install the declared dependencies, and launch the dashboard:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
streamlit run dashboard.py
```

For Streamlit Community Cloud, keep the repository and deployed app private, select `dashboard.py` as the entry point, add the two secrets, and reboot the app. The sidebar should then list the active Deriv Options accounts returned for that PAT.

## Execution safety

The selected Deriv account type determines what the engine is allowed to do. There is no separate simulated mode inside the application; a selected demo account still sends real API requests, but the resulting contract uses demo funds.

| Selected account and confirmation | Engine behavior |
| --- | --- |
| Demo account | Sends proposal and buy requests with demo funds after a qualifying signal. |
| Real account without exact `LIVE` confirmation | Streams and evaluates signals, but blocks proposal and buy execution. |
| Real account with `LIVE` typed exactly | Permits real-money proposal and buy requests after a qualifying signal. |
| Inactive or unrecognized account | Fails closed and sends no order. |
| Stop pressed before buy submission | Cancels the local attempt before sending `buy`. |
| Stop pressed after a contract is confirmed | Sends no new order and continues monitoring that contract until settlement or the safety timeout. |

A lost or malformed buy response is inherently ambiguous because the request may have reached Deriv even if the receipt did not return. This build therefore **does not retry `buy` automatically**. It reconciles against the portfolio and stops if no unique fill can be proven.

## Strategy configuration

The dashboard provides Conservative, Balanced, and Aggressive sensitivity presets. The strategy combines a short momentum-quality window, a pullback, continuation confirmation, and 5-minute, 15-minute, and 30-minute directional context. The configured contract is a five-tick `ONETOUCH` on `1HZ10V`, using `+0.08` for upward signals and `-0.08` for downward signals by default. Deriv’s current public WebSocket was verified to price both exact payloads on 26 July 2026; this confirms request compatibility, not profitability or future API stability.

Martingale remains enabled by configuration and can increase losses quickly. An unresolved or ambiguous contract never advances Martingale in this build.

## Validation

Run the deterministic test suite without a PAT and without placing a trade:

```bash
python3 -m compileall -q .
python3 -m unittest -v test_strategy.py test_deriv_repairs.py
python3 test_latency.py
```

The included public compatibility probe can also be run with `python3 tools/public_deriv_probe.py`. It requests candles and price proposals only; it does not authenticate or send `buy`.

The delivered repair passed **24 deterministic unit/regression tests**, both latency checks, dependency validation, an official-schema comparison, a headless dashboard health check, and a live public proposal probe. No account-authenticated buy was submitted during repair validation. See `REPAIR_REPORT.md` for the exact scope and boundary.

## Operational checklist

Before enabling any order, confirm that the account shown in the hero panel is the intended account, the currency is correct, the stake and Martingale progression are acceptable, the barriers are valid, and the first run uses demo funds. If the dashboard reports `UNKNOWN` or an ambiguous buy, do not restart immediately; inspect the Deriv statement for the displayed time and contract details first.

## References

[1]: https://developers.deriv.com/docs/intro/api-overview/ "Deriv API Overview"
[2]: https://developers.deriv.com/docs/options/websocket/ "WebSockets — Options API"
[3]: https://developers.deriv.com/docs/trading/proposal/ "Price Proposal — WebSocket API"
[4]: https://developers.deriv.com/docs/trading/buy/ "Buy Contract — WebSocket API"
[5]: https://developers.deriv.com/docs/trading/proposal-open-contract/ "Open Contract Status — WebSocket API"
