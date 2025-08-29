-- BigQuery setup script
-- Run these commands in your BigQuery environment to set up the necessary objects

-- Create dataset (replace PROJECT_ID with your actual project ID)
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.bi_assistant_staging`
OPTIONS(
  description="Staging schema for BI Assistant raw data",
  location="US"
);

CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.bi_assistant_marts`
OPTIONS(
  description="Marts schema for BI Assistant business-ready data",
  location="US"
);

CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.bi_assistant_seeds`
OPTIONS(
  description="Seeds schema for BI Assistant reference data",
  location="US"
);

-- Note: BigQuery permissions are managed through IAM
-- Ensure your service account has the following roles:
-- - BigQuery Data Editor (on the datasets)
-- - BigQuery Job User (on the project)
-- - BigQuery User (on the project)

-- Example IAM commands (run via gcloud CLI):
-- gcloud projects add-iam-policy-binding PROJECT_ID \
--   --member="serviceAccount:your-service-account@PROJECT_ID.iam.gserviceaccount.com" \
--   --role="roles/bigquery.dataEditor"

-- gcloud projects add-iam-policy-binding PROJECT_ID \
--   --member="serviceAccount:your-service-account@PROJECT_ID.iam.gserviceaccount.com" \
--   --role="roles/bigquery.jobUser"