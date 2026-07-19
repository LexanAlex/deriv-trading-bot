# Deriv Volatility 10 (1s) Trading Bot

A sophisticated automated trading system designed for the Deriv Volatility 10 (1s) Index. It leverages a high-probability 1-3-1 tick pattern strategy, multi-timeframe trend confirmation, and Martingale money management. The system is built with Python, connects directly to the Deriv WebSocket API, and features a responsive Streamlit dashboard for real-time monitoring and control.

## Core Features
- **1-3-1 Tick Pattern Strategy:** Precise entry signals based on micro-retracements within established trends.
- **Trend & Velocity Filtering:** Only trades during strong, unidirectional momentum pushes (10-15 ticks).
- **Multi-Timeframe Confirmation:** Validates 1-tick signals against 5m, 15m, and 30m candle trends.
- **Martingale Money Management:** Configurable stake multiplier (3x) and step limits.
- **Risk Distribution:** Enforces a maximum of 1 trade per identified trend with automatic cool-down periods.
- **Streamlit Dashboard:** Live tick chart, performance metrics, and trade history.
- **Iframe Embeddable:** Easily deployable and embeddable into any website (e.g., Hostinger).

---

## 1. Prerequisites
- **Python 3.9+**
- A **Deriv API Token** with `Trading` and `Read` permissions. You can generate one at [app.deriv.com/account/api-token](https://app.deriv.com/account/api-token).

## 2. Installation

1. **Clone or copy the project files** to your server (e.g., your Hostinger VPS).
2. **Navigate to the project directory:**
   ```bash
   cd momentum_master
   ```
3. **Install the required Python packages:**
   ```bash
   pip install -r requirements.txt
   ```

## 3. Configuration (Optional)
Default settings are located in `config.py`. You can adjust core parameters such as:
- `TREND_WINDOW_MIN` and `TREND_WINDOW_MAX`
- `VELOCITY_THRESHOLD`
- `MAX_TRADES_PER_TREND`
- Default initial stakes and barrier offsets.

## 4. Running the Application

Start the Streamlit application using the following command:

```bash
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

The dashboard will be accessible at `http://<your-server-ip>:8501`.

## 5. Embedding into a Website (Hostinger)

To embed the dashboard into your Hostinger website page, use an `<iframe>` HTML tag.

1. Ensure your Streamlit server is running and accessible over the internet (check your firewall/security group settings to allow traffic on port 8501).
2. If you are using HTTPS on your website, you will need to set up a reverse proxy (like Nginx or Caddy) with an SSL certificate for your Streamlit app, because browsers block mixed content (HTTP iframe inside an HTTPS site).
3. Add the following HTML code to your webpage:

```html
<iframe 
    src="http://<your-server-ip>:8501" 
    width="100%" 
    height="900px" 
    style="border:none;" 
    allowfullscreen>
</iframe>
```

### Important Streamlit CORS Settings
If the iframe refuses to load due to cross-origin restrictions, you may need to disable CORS and XSRF protection when starting Streamlit:

```bash
streamlit run app.py --server.port 8501 --server.address 0.0.0.0 --server.enableCORS false --server.enableXsrfProtection false
```

## 6. Logging and Session Management
- **Logs:** All API interactions, pattern detections, and trade outcomes are logged to `logs/momentum_master.log`. The logger uses a rotating file handler to prevent unbounded file growth.
- **Session Management:** The `api_client.py` handles automatic WebSocket reconnections and background pinging to maintain the session. If the connection drops, it will attempt to reconnect up to 10 times.

## 7. Disclaimer
Trading synthetic indices involves significant risk. This software is provided for educational and experimental purposes. Always test your strategies on a **Demo Account** before risking real money.
