#!/usr/bin/env python3
"""Load sample data to Snowflake warehouse."""

import os
import sys
from pathlib import Path
import pandas as pd

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analytics.runners.snowflake_runner import SnowflakeRunner
from app.config import Config


def main():
    """Load sample data to Snowflake."""
    print("❄️  Loading data to Snowflake...")
    
    # Validate configuration
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE'
    ]
    
    missing_vars = [var for var in required_vars if not getattr(Config, var)]
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please configure Snowflake connection settings in your .env file")
        sys.exit(1)
    
    try:
        # Initialize Snowflake runner
        runner = SnowflakeRunner()
        
        print(f"📊 Connected to Snowflake account: {Config.SNOWFLAKE_ACCOUNT}")
        print(f"🏢 Database: {Config.SNOWFLAKE_DATABASE}")
        print(f"📁 Schema: {Config.SNOWFLAKE_SCHEMA}")
        
        # Create schemas if they don't exist
        print("🔧 Creating schemas...")
        schemas = ['STAGING', 'MARTS', 'SEEDS']
        for schema in schemas:
            runner.create_schema(schema)
            print(f"   ✅ Schema {schema} ready")
        
        # Load seed data
        seeds_dir = project_root / "dbt" / "seeds"
        seed_files = [
            ("hr_employees.csv", "HR_EMPLOYEES", "SEEDS"),
            ("hr_regions.csv", "HR_REGIONS", "SEEDS"),
            ("hr_attrition_events.csv", "HR_ATTRITION_EVENTS", "SEEDS")
        ]
        
        print("📄 Loading seed data...")
        for csv_file, table_name, schema in seed_files:
            csv_path = seeds_dir / csv_file
            if csv_path.exists():
                print(f"   Loading {csv_file}...")
                
                # Read CSV
                df = pd.read_csv(csv_path)
                
                # Load to Snowflake
                runner.load_dataframe_to_table(df, table_name, schema)
                print(f"   ✅ Loaded {len(df)} rows to {schema}.{table_name}")
            else:
                print(f"   ⚠️  Warning: {csv_file} not found")
        
        # Run dbt if available
        print("🔄 Setting up dbt profile for Snowflake...")
        
        # Create dbt profiles directory if it doesn't exist
        dbt_profiles_dir = Path.home() / ".dbt"
        dbt_profiles_dir.mkdir(exist_ok=True)
        
        # Generate profiles.yml content
        profiles_content = f"""
bi_assistant:
  target: snowflake
  outputs:
    snowflake:
      type: snowflake
      account: {Config.SNOWFLAKE_ACCOUNT}
      user: {Config.SNOWFLAKE_USER}
      password: {Config.SNOWFLAKE_PASSWORD}
      role: {Config.SNOWFLAKE_ROLE or 'PUBLIC'}
      database: {Config.SNOWFLAKE_DATABASE}
      warehouse: {Config.SNOWFLAKE_WAREHOUSE}
      schema: {Config.SNOWFLAKE_SCHEMA or 'PUBLIC'}
      threads: 4
      keepalives_idle: 30
"""
        
        profiles_file = dbt_profiles_dir / "profiles.yml"
        
        # Ask user before overwriting existing profiles
        if profiles_file.exists():
            response = input(f"⚠️  {profiles_file} already exists. Overwrite? (y/N): ")
            if response.lower() != 'y':
                print("Skipping dbt profile creation")
            else:
                with open(profiles_file, 'w') as f:
                    f.write(profiles_content)
                print(f"✅ Created dbt profile at {profiles_file}")
        else:
            with open(profiles_file, 'w') as f:
                f.write(profiles_content)
            print(f"✅ Created dbt profile at {profiles_file}")
        
        # Try to run dbt
        try:
            import subprocess
            
            os.chdir(project_root)
            
            # Check if dbt is available
            result = subprocess.run(['dbt', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                print("📦 Running dbt transformations...")
                
                dbt_commands = [
                    ['dbt', 'deps'],
                    ['dbt', 'seed', '--target', 'snowflake'],
                    ['dbt', 'run', '--target', 'snowflake'],
                    ['dbt', 'test', '--target', 'snowflake']
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
                print("⚠️  dbt not found. Install dbt-snowflake and run manually:")
                print("   cd dbt && dbt run --target snowflake")
                
        except Exception as e:
            print(f"⚠️  Could not run dbt: {e}")
        
        # Verify data
        print("🔍 Verifying data load...")
        
        for _, table_name, schema in seed_files:
            try:
                df, _ = runner.execute_query(f"SELECT COUNT(*) as count FROM {schema}.{table_name}")
                count = df.iloc[0]['count']
                print(f"   ✅ {schema}.{table_name}: {count} rows")
            except Exception as e:
                print(f"   ❌ {schema}.{table_name}: {e}")
        
        # Show warehouse usage
        print("\n💰 Warehouse Usage:")
        usage = runner.get_warehouse_usage()
        if 'credits_used_7d' in usage:
            print(f"   Credits used (7 days): {usage['credits_used_7d']:.4f}")
        
        print("\n🎉 Snowflake setup completed!")
        print("\nNext steps:")
        print("1. Update your .env file with WAREHOUSE=SNOWFLAKE")
        print("2. Run the Streamlit app to test the connection")
        print("3. Configure additional Snowflake users/roles as needed")
        
    except Exception as e:
        print(f"❌ Snowflake setup failed: {e}")
        sys.exit(1)
    
    finally:
        runner.close()


if __name__ == "__main__":
    main()