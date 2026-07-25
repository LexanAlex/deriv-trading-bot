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
