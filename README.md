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

## Trading modes

- **Practice** receives real tick data and records strategy signals, but never
  sends a proposal or a buy request.
- **Live** can place real trades only after you select a `REAL` account and
  type `LIVE` exactly. Start with a demo account and a small stake.

## Strategy and risk controls

The quality-first entry requires a 70% directional tick trend, complete 5m /
15m / 30m alignment, one pullback, and two continuation ticks. Your stake and
Martingale controls remain configurable in the dashboard. These are execution
rules, not a promise of profit; validate them on a demo account before Live.

## Files to commit

Commit `dashboard.py`, `config.py`, `requirements.txt`, `src/`, `README.md`,
`STREAMLIT_CLOUD.md`, and `.streamlit/secrets.toml.example`. Do not commit
`.streamlit/secrets.toml`, `.env`, logs, `__pycache__`, or a token.
