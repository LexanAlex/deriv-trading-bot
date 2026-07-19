# MomentumMaster — personal Streamlit Cloud bot

This is a direct Streamlit Cloud application. It uses your existing registered Deriv API app:
the App ID and OAuth Redirect URL are all that are needed for this legacy
WebSocket OAuth flow.

## Deploy

1. Create a private GitHub repository and upload this folder's contents.
2. Create a Streamlit Cloud app with `dashboard.py` as the entry point.
3. Copy the app URL, for example:
   `https://deriv-trading-bot.streamlit.app/`
4. In your existing Deriv application, set **OAuth Redirect URL** to that
   exact URL, including the final slash if shown. Save the change.
5. In Streamlit Cloud, open **App settings → Secrets** and add:

   ```toml
   DERIV_APP_ID = "YOUR_REGISTERED_DERIV_APP_ID"
   ```

6. Reboot the Streamlit app, open it directly in a browser, expand the
   sidebar, and select **Login with Deriv**.

On a successful return, Deriv supplies `token1`, `acct1`, and `cur1`. The app
keeps them only for the active Streamlit session and removes them from the URL.

## Modes

* **Paper** is the default. It receives live market data and records a signal,
  but never sends a proposal or a buy request to Deriv.
* **Live** can place trades for the account you authorize. Select Live and type
  `LIVE` exactly before pressing Start.

The bot stops when the Streamlit session/browser is closed or restarted. Begin
with a demo account and a small stake.

## Files to commit

Commit `dashboard.py`, `config.py`, `requirements.txt`, `src/`, `README.md`,
`STREAMLIT_CLOUD.md`, and `.streamlit/secrets.toml.example`. Never commit a
real `.streamlit/secrets.toml`, `.env`, log file, or token.
