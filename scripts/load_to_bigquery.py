#!/usr/bin/env python3
"""Load sample data to BigQuery."""

import os
import sys
from pathlib import Path
import pandas as pd

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analytics.runners.bigquery_runner import BigQueryRunner
from app.config import Config


def main():
    """Load sample data to BigQuery."""
    print("🔍 Loading data to BigQuery...")
    
    # Validate configuration
    if not Config.BQ_PROJECT_ID or not Config.BQ_DATASET:
        print("❌ Missing required BigQuery configuration:")
        print("   Please set BQ_PROJECT_ID and BQ_DATASET in your .env file")
        sys.exit(1)
    
    try:
        # Initialize BigQuery runner
        runner = BigQueryRunner()
        
        print(f"🏢 Project: {Config.BQ_PROJECT_ID}")
        print(f"📁 Dataset: {Config.BQ_DATASET}")
        
        # Create dataset if it doesn't exist
        print("🔧 Creating dataset...")
        runner.create_dataset(Config.BQ_DATASET)
        
        # Also create staging and marts datasets
        staging_dataset = f"{Config.BQ_DATASET}_staging"
        marts_dataset = f"{Config.BQ_DATASET}_marts"
        
        runner.create_dataset(staging_dataset)
        runner.create_dataset(marts_dataset)
        
        print(f"   ✅ Datasets ready: {Config.BQ_DATASET}, {staging_dataset}, {marts_dataset}")
        
        # Load seed data
        seeds_dir = project_root / "dbt" / "seeds"
        seed_files = [
            ("hr_employees.csv", "hr_employees"),
            ("hr_regions.csv", "hr_regions"),
            ("hr_attrition_events.csv", "hr_attrition_events")
        ]
        
        print("📄 Loading seed data...")
        for csv_file, table_name in seed_files:
            csv_path = seeds_dir / csv_file
            if csv_path.exists():
                print(f"   Loading {csv_file}...")
                
                # Read CSV
                df = pd.read_csv(csv_path)
                
                # Convert date columns
                if 'hire_date' in df.columns:
                    df['hire_date'] = pd.to_datetime(df['hire_date'])
                if 'birth_date' in df.columns:
                    df['birth_date'] = pd.to_datetime(df['birth_date'])
                if 'termination_date' in df.columns:
                    df['termination_date'] = pd.to_datetime(df['termination_date'])
                
                # Load to BigQuery
                runner.load_dataframe_to_table(df, table_name, Config.BQ_DATASET)
                print(f"   ✅ Loaded {len(df)} rows to {Config.BQ_DATASET}.{table_name}")
            else:
                print(f"   ⚠️  Warning: {csv_file} not found")
        
        # Setup dbt profile for BigQuery
        print("🔄 Setting up dbt profile for BigQuery...")
        
        dbt_profiles_dir = Path.home() / ".dbt"
        dbt_profiles_dir.mkdir(exist_ok=True)
        
        # Generate profiles.yml content
        profiles_content = f"""
bi_assistant:
  target: bigquery
  outputs:
    bigquery:
      type: bigquery
      method: service-account
      project: {Config.BQ_PROJECT_ID}
      dataset: {Config.BQ_DATASET}
      keyfile: {Config.GOOGLE_APPLICATION_CREDENTIALS or 'path/to/keyfile.json'}
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
"""
        
        profiles_file = dbt_profiles_dir / "profiles.yml"
        
        if profiles_file.exists():
            response = input(f"⚠️  {profiles_file} already exists. Overwrite? (y/N): ")
            if response.lower() == 'y':
                with open(profiles_file, 'w') as f:
                    f.write(profiles_content)
                print(f"✅ Updated dbt profile at {profiles_file}")
        else:
            with open(profiles_file, 'w') as f:
                f.write(profiles_content)
            print(f"✅ Created dbt profile at {profiles_file}")
        
        # Try to run dbt
        try:
            import subprocess
            
            os.chdir(project_root)
            
            result = subprocess.run(['dbt', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                print("📦 Running dbt transformations...")
                
                dbt_commands = [
                    ['dbt', 'deps'],
                    ['dbt', 'seed', '--target', 'bigquery'],
                    ['dbt', 'run', '--target', 'bigquery'],
                    ['dbt', 'test', '--target', 'bigquery']
                ]
                
                for cmd in dbt_commands:
                    print(f"   Running: {' '.join(cmd)}")
                    result = subprocess.run(cmd, cwd='dbt', capture_output=True, text=True)
                    if result.returncode == 0:
                        print(f"   ✅ {cmd[1]} completed")
                    else:
                        print(f"   ⚠️  {cmd[1]} had issues:")
                        if result.stderr:
                            print(f"      {result.stderr}")
            else:
                print("⚠️  dbt not found. Install dbt-bigquery and run manually:")
                print("   cd dbt && dbt run --target bigquery")
                
        except Exception as e:
            print(f"⚠️  Could not run dbt: {e}")
        
        # Verify data
        print("🔍 Verifying data load...")
        
        for _, table_name in seed_files:
            try:
                df, _ = runner.execute_query(f"SELECT COUNT(*) as count FROM `{Config.BQ_PROJECT_ID}.{Config.BQ_DATASET}.{table_name}`")
                count = df.iloc[0]['count']
                print(f"   ✅ {Config.BQ_DATASET}.{table_name}: {count} rows")
            except Exception as e:
                print(f"   ❌ {Config.BQ_DATASET}.{table_name}: {e}")
        
        # Show recent job info
        print("\n📊 Recent BigQuery Jobs:")
        jobs = runner.get_job_history(limit=3)
        for job in jobs:
            if job.get('bytes_processed'):
                bytes_mb = job['bytes_processed'] / (1024 * 1024)
                print(f"   Job {job['job_id']}: {bytes_mb:.2f} MB processed")
        
        print("\n🎉 BigQuery setup completed!")
        print("\nNext steps:")
        print("1. Update your .env file with WAREHOUSE=BIGQUERY")
        print("2. Ensure service account credentials are properly configured")
        print("3. Run the Streamlit app to test the connection")
        print("4. Consider setting up BigQuery scheduled queries for regular data updates")
        
    except Exception as e:
        print(f"❌ BigQuery setup failed: {e}")
        sys.exit(1)
    
    finally:
        runner.close()


if __name__ == "__main__":
    main()