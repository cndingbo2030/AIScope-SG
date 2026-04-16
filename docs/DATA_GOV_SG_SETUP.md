# data.gov.sg API key — GitHub Actions & local use

## 1. Create a repository secret (CI/CD)

1. Open the GitHub repository in the browser.
2. Go to **Settings** → **Secrets and variables** → **Actions**.
3. Click **New repository secret**.
4. Name: **`DATA_GOV_SG_API_KEY`** (exact spelling).
5. Value: paste your **Production** key from the [data.gov.sg developer dashboard](https://data.gov.sg/).
6. Save.

The workflow `.github/workflows/deploy-pages.yml` maps this secret to the environment variable `DATA_GOV_SG_API_KEY` for the fetch step so `pipeline/step1_fetch.py` can authenticate with the `x-api-key` header.

## 2. Local development

1. Copy `.env.example` to `.env`.
2. Set `DATA_GOV_SG_API_KEY=` in `.env`.
3. Run `python3 scripts/test_api_connection.py` to verify connectivity.
4. Run `python -m pipeline.step1_fetch` (or `python run_pipeline.py --fetch`) to write `data/raw/wages_fetched.json`.

If the key is missing or the API fails, step 1 logs a **warning** and writes from **`data/raw/wages_fallback.json`** so pipelines keep running.

## 3. Licence

Tabular open data on data.gov.sg is typically distributed under the **Singapore Open Data Licence**. See: [Open Data Licence](https://data.gov.sg/open-data-licence).
