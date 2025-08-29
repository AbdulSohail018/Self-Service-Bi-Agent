#!/usr/bin/env python3
"""Bootstrap DuckDB database with sample data for local development."""

import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analytics.runners.duckdb_runner import DuckDBRunner
from app.config import Config


def main():
    """Bootstrap DuckDB database with sample data."""
    print("🚀 Bootstrapping DuckDB database...")
    
    # Ensure data directory exists
    db_path = Path(Config.DUCKDB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Initialize DuckDB runner
        runner = DuckDBRunner(str(db_path))
        
        print(f"📁 Database location: {db_path}")
        
        # Test connection
        if not runner.test_connection():
            raise Exception("Failed to connect to DuckDB")
        
        print("✅ DuckDB connection established")
        
        # Execute setup script
        setup_script_path = project_root / "sql" / "ddl" / "duckdb.sql"
        if setup_script_path.exists():
            print("🔧 Running setup script...")
            runner.execute_script(str(setup_script_path))
            print("✅ Setup script completed")
        
        # Create schemas
        print("📊 Creating schemas...")
        runner.create_schema("staging")
        runner.create_schema("marts")
        runner.create_schema("seeds")
        print("✅ Schemas created")
        
        # Load seed data
        seeds_dir = project_root / "dbt" / "seeds"
        seed_files = [
            ("hr_employees.csv", "seeds.hr_employees"),
            ("hr_regions.csv", "seeds.hr_regions"),
            ("hr_attrition_events.csv", "seeds.hr_attrition_events")
        ]
        
        print("📄 Loading seed data...")
        for csv_file, table_name in seed_files:
            csv_path = seeds_dir / csv_file
            if csv_path.exists():
                # Extract schema and table name
                schema, table = table_name.split('.')
                runner.load_csv_to_table(str(csv_path), table, schema)
                print(f"   ✅ Loaded {csv_file} → {table_name}")
            else:
                print(f"   ⚠️  Warning: {csv_file} not found")
        
        # Run dbt transformations if dbt is available
        print("🔄 Attempting to run dbt transformations...")
        try:
            import subprocess
            
            # Change to project directory
            os.chdir(project_root)
            
            # Check if dbt is available
            result = subprocess.run(['dbt', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                print("📦 dbt found, running transformations...")
                
                # Run dbt commands
                dbt_commands = [
                    ['dbt', 'deps'],  # Install dependencies
                    ['dbt', 'seed'],  # Load seeds (redundant but ensures consistency)
                    ['dbt', 'run'],   # Run models
                    ['dbt', 'test']   # Run tests
                ]
                
                for cmd in dbt_commands:
                    print(f"   Running: {' '.join(cmd)}")
                    result = subprocess.run(cmd, cwd='dbt', capture_output=True, text=True)
                    if result.returncode == 0:
                        print(f"   ✅ {cmd[1]} completed successfully")
                    else:
                        print(f"   ⚠️  {cmd[1]} completed with warnings:")
                        if result.stderr:
                            print(f"      {result.stderr}")
                
            else:
                print("⚠️  dbt not found, skipping model transformations")
                print("   Install dbt and run 'dbt run' in the dbt/ directory to create mart tables")
                
        except Exception as e:
            print(f"⚠️  Could not run dbt transformations: {e}")
            print("   You can manually run dbt commands in the dbt/ directory")
        
        # Verify data loading
        print("🔍 Verifying data loading...")
        
        # Check seed tables
        for _, table_name in seed_files:
            try:
                df, _ = runner.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
                count = df.iloc[0]['count']
                print(f"   ✅ {table_name}: {count} rows")
            except Exception as e:
                print(f"   ❌ {table_name}: Error - {e}")
        
        # Get database stats
        stats = runner.get_stats()
        print(f"\n📊 Database Statistics:")
        print(f"   Size: {stats.get('database_size_mb', 'Unknown')} MB")
        print(f"   Tables: {stats.get('table_count', 'Unknown')}")
        print(f"   Status: {stats.get('connection_status', 'Unknown')}")
        
        # Show available tables
        print("\n📋 Available Tables:")
        schema_info = runner.get_schema_info()
        for table_name in sorted(schema_info.keys()):
            column_count = len(schema_info[table_name])
            print(f"   {table_name} ({column_count} columns)")
        
        print("\n🎉 Bootstrap completed successfully!")
        print("\nNext steps:")
        print("1. Copy .env.example to .env and configure as needed")
        print("2. Run 'streamlit run app/streamlit_app.py' to start the application")
        print("3. Run 'python eval/evaluator.py' to test the NL→SQL functionality")
        
    except Exception as e:
        print(f"❌ Bootstrap failed: {e}")
        sys.exit(1)
    
    finally:
        runner.close()


if __name__ == "__main__":
    main()