# Deployment Guide

This app is deployable as a read-only Streamlit dashboard using the committed
`cache/`, `data/`, and `geo_data/` files. API keys are only required when
refreshing source data locally.

## Streamlit Community Cloud

1. Push this repository to GitHub.
2. In Streamlit Community Cloud, create a new app from the repository.
3. Set the main file path to `app.py`.
4. Keep the Python version from `runtime.txt`.
5. Deploy.

No secrets are required for the dashboard if the cached data files are included.

## Optional Secrets For Data Updates

Only add these secrets if you plan to run data collection scripts in the deploy
environment:

```toml
BOK_API_KEY = "..."
DATA_GO_KR_KEY = "..."
MOLIT_API_KEY = "..."
KOSIS_API_KEY = "..."
SGIS_CONSUMER_KEY = "..."
SGIS_CONSUMER_SECRET = "..."
```

For normal public dashboard hosting, update data locally and commit refreshed
`cache/` and `data/` files instead.

## Local Production Check

```bash
python -m py_compile app.py
streamlit run app.py --server.headless true --server.port 8503
```

Open `http://localhost:8503` and confirm the top tabs are:

`Overview`, `수요공급분석`, `거래현황`, `매물찾기`, `적정값가상계산`.

## Files Required At Deploy Time

- `app.py`, `data_loader.py`, `analysis.py`, `tax_calculator.py`, `board.py`
- `requirements.txt`, `packages.txt`, `runtime.txt`
- `cache/` parquet and CSV files
- `data/` CSV files
- `geo_data/sigungu.geojson`

Do not commit `.env` or `.streamlit/secrets.toml`.
