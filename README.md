# MomentumMaster

Personal Streamlit trading dashboard using Deriv's current Personal Access
Token (PAT) flow. It does not use OAuth, an iframe, a redirect URL, or a
browser login button.

## Streamlit Cloud setup

1. Keep the GitHub repository and Streamlit app private.
2. In Streamlit Cloud, set `dashboard.py` as the entry point.
3. Open **App settings → Secrets** and add both values:

   ```toml
   DERIV_APP_ID = "YOUR_PAT_APP_ID"
   DERIV_API_TOKEN = "YOUR_PAT_SECRET"
   ```

4. Reboot the app. Its sidebar should show your active Deriv Options accounts
   and their balances. Select one account before starting.

The PAT must have `trade` and `account_manage` scopes (plus `read` when that
scope is available). Never commit, paste, or share the PAT. It is a secret and
is only displayed once by Deriv.

## Execution modes and safety controls

The selected **Deriv account type** controls whether API-backed orders can be
sent; there is no ambiguous Practice/Live radio gate.

| Selected account or control | Engine behavior | Trade-history result |
| --- | --- | --- |
| `DEMO` account | Sends a real Deriv proposal request followed by a real buy request against demo funds as soon as a qualifying signal occurs. No additional confirmation is required. | `DEMO API ORDER`, with a real Deriv contract ID after buy confirmation and then `OPEN`, `WON`, `LOST`, or `UNKNOWN`. |
| `REAL` account without exact `LIVE` confirmation | Streams ticks and evaluates signals, but fails closed before any proposal or buy request. | `SAFETY BLOCKED`, including the reason that no order was sent. |
| `REAL` account with `LIVE` typed exactly | Sends real-money proposal and buy requests only after a qualifying signal. | `REAL API ORDER`, with a real contract ID after buy confirmation. |
| **Signal-only preview** checked | Records the strategy signal locally and deliberately skips both proposal and buy requests, regardless of account type. | `SIGNAL-ONLY PREVIEW` with `PREVIEW` status. |
| Unrecognised account type | Fails closed; no order request is sent. | `SAFETY BLOCKED`. |

The status banner and the `Detail` column surface proposal/buy rejections,
connection failures, and unresolved settlement monitoring instead of silently
dropping the attempt. Start with a demo account and a small stake.

## Strategy and risk controls

The quality-first entry requires a 70% directional tick trend, complete 5m /
15m / 30m alignment, one pullback, and two continuation ticks. Your stake and
Martingale controls remain configurable in the dashboard. These are execution
rules, not a promise of profit; validate them on a demo account before Live.

## Files to commit

Commit `dashboard.py`, `config.py`, `requirements.txt`, `src/`, `README.md`,
`STREAMLIT_CLOUD.md`, and `.streamlit/secrets.toml.example`. Do not commit
`.streamlit/secrets.toml`, `.env`, logs, `__pycache__`, or a token.
