# Storing in GCP (Optional)

## Overview

GCP integration is **optional**. The pipeline works fully offline with local storage. Enable GCP when you want:

- Cloud backup of processed data
- BigQuery for ad-hoc SQL against your financial history
- Shared access across machines
- Scheduled pipeline runs via Cloud Functions or Cloud Run

---

## Architecture

```
data/processed/*.parquet
        │
        ├──→ Google Cloud Storage (GCS)     — parquet archive
        │        bucket: mmex-finance-data/
        │        path:   processed/{table_name}/{YYYY-MM-DD}.parquet
        │
        └──→ BigQuery                       — queryable tables
                 dataset: personal_finance
                 tables:  transactions, monthly_aggregates,
                          expense_trends, cashflow_forecast
```

---

## Setup

### 1. GCP Project & Authentication

```bash
# Install the CLI
pip install google-cloud-storage google-cloud-bigquery --break-system-packages

# Authenticate (one-time)
gcloud auth application-default login

# Or use a service account key (CI/CD)
export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
```

### 2. Create Resources

```bash
# Set your project
export GCP_PROJECT="your-project-id"

# Create GCS bucket
gsutil mb -l europe-central2 gs://mmex-finance-data-${GCP_PROJECT}/

# Create BigQuery dataset
bq mk --dataset --location=EU ${GCP_PROJECT}:personal_finance
```

### 3. Pipeline Config

Add GCP settings to `configs/pipeline.yaml`:

```yaml
gcp:
  enabled: false  # Set to true to activate GCP uploads
  project_id: "your-project-id"
  gcs:
    bucket: "mmex-finance-data-your-project-id"
    prefix: "processed"
  bigquery:
    dataset: "personal_finance"
    location: "EU"
    write_disposition: "WRITE_TRUNCATE"  # or WRITE_APPEND
```

---

## Implementation: `src/storage/gcp_writer.py`

```python
"""
Write processed DataFrames to GCS (parquet) and BigQuery.

Only active when configs/pipeline.yaml has gcp.enabled: true.

Usage:
    from src.storage.gcp_writer import GCPWriter
    writer = GCPWriter(config)  # config from pipeline.yaml
    writer.upload_to_gcs(df, "monthly_aggregates")
    writer.upload_to_bigquery(df, "monthly_aggregates")
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)


class GCPWriter:
    def __init__(self, config: dict):
        """
        Parameters
        ----------
        config : dict
            The 'gcp' section of pipeline.yaml.
        """
        self.enabled = config.get("enabled", False)
        if not self.enabled:
            logger.info("GCP writer disabled (gcp.enabled: false)")
            return

        self.project_id = config["project_id"]
        self.bucket_name = config["gcs"]["bucket"]
        self.gcs_prefix = config["gcs"]["prefix"]
        self.bq_dataset = config["bigquery"]["dataset"]
        self.bq_location = config["bigquery"]["location"]
        self.write_disposition = config["bigquery"]["write_disposition"]

        # Import GCP libraries only when enabled
        from google.cloud import storage as gcs
        from google.cloud import bigquery as bq

        self.gcs_client = gcs.Client(project=self.project_id)
        self.bq_client = bq.Client(project=self.project_id, location=self.bq_location)
        logger.info(f"GCP writer initialised: project={self.project_id}")

    def upload_to_gcs(self, df: pd.DataFrame, table_name: str) -> str:
        """
        Upload DataFrame as parquet to GCS.

        Returns the GCS URI of the uploaded file.
        """
        if not self.enabled:
            logger.warning("GCP disabled. Skipping GCS upload.")
            return ""

        today = datetime.now().strftime("%Y-%m-%d")
        blob_path = f"{self.gcs_prefix}/{table_name}/{today}.parquet"

        # Write to temp file first
        tmp_path = f"/tmp/{table_name}_{today}.parquet"
        df.to_parquet(tmp_path, index=False)

        bucket = self.gcs_client.bucket(self.bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(tmp_path)

        gcs_uri = f"gs://{self.bucket_name}/{blob_path}"
        logger.info(f"Uploaded {len(df)} rows to {gcs_uri}")

        # Clean up temp file
        Path(tmp_path).unlink(missing_ok=True)
        return gcs_uri

    def upload_to_bigquery(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Upload DataFrame to BigQuery table.

        Table is created if it doesn't exist. Schema inferred from DataFrame.
        """
        if not self.enabled:
            logger.warning("GCP disabled. Skipping BigQuery upload.")
            return

        from google.cloud.bigquery import LoadJobConfig, WriteDisposition

        table_id = f"{self.project_id}.{self.bq_dataset}.{table_name}"

        disposition_map = {
            "WRITE_TRUNCATE": WriteDisposition.WRITE_TRUNCATE,
            "WRITE_APPEND": WriteDisposition.WRITE_APPEND,
        }

        job_config = LoadJobConfig(
            write_disposition=disposition_map.get(
                self.write_disposition, WriteDisposition.WRITE_TRUNCATE
            ),
            autodetect=True,
        )

        job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion

        logger.info(f"Uploaded {len(df)} rows to BigQuery: {table_id}")
```

---

## BigQuery Table Design

| Table | Source Step | Key Columns | Grain |
|-------|-----------|-------------|-------|
| `transactions` | Step 1 | date, account, category, amount | 1 row per transaction |
| `monthly_aggregates` | Step 2 | month, income, expenditure, net_cashflow, savings_rate | 1 row per month |
| `expense_trends` | Step 3 | category, month, spend, share_pct, trend_slope, trend_pvalue | 1 row per category-month |
| `cashflow_forecast` | Step 4 | month, point_estimate, ci80_lower, ci80_upper, ci95_lower, ci95_upper | 1 row per forecast month |
| `networth_trajectory` | Step 5 | month, net_worth, mom_change, growth_rate_6m | 1 row per month |

---

## Security Notes

- **Never commit GCP credentials to git.** The `.gitignore` already excludes `*.json`.
- Use **service account keys** only for CI/CD. For local development, `gcloud auth application-default login` is safer.
- The BigQuery dataset should have **restricted access** — this is personal financial data.
- Consider enabling **BigQuery column-level security** if sharing the dataset.
- GCS bucket should have **uniform bucket-level access** and no public access.

---

## Cost Estimate

For a typical personal finance dataset (< 100k transactions, < 10 MB):

| Resource | Estimated Monthly Cost |
|----------|----------------------|
| GCS storage | < $0.01 |
| BigQuery storage | < $0.01 |
| BigQuery queries | < $0.01 (first 1 TB/month free) |
| **Total** | **~$0.00** |

GCP's free tier comfortably covers personal finance workloads.
