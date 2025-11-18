import cx_Oracle
import pandas as pd
import os
from pathlib import Path

class DatabaseManager:
    def __init__(self, username='system', password='jm64108034', 
                 host='localhost', port=1521, service_name='xe'):
        self.connection_string = f"{username}/{password}@{host}:{port}/{service_name}"
        self.connection = None
    
    def connect(self):
        """Connect to Oracle database"""
        try:
            self.connection = cx_Oracle.connect(self.connection_string)
            print("✅ Connected to Oracle Database")
            return True
        except cx_Oracle.Error as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def execute_sql_file(self, file_path):
        """Execute SQL file from VS Code"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read()
            
            # Split by semicolons but ignore those in strings
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            cursor = self.connection.cursor()
            for statement in statements:
                if statement.upper().startswith('SELECT'):
                    # For SELECT statements, fetch and display results
                    cursor.execute(statement)
                    results = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(results, columns=columns)
                    print(f"📊 Results for: {statement[:50]}...")
                    print(df)
                else:
                    # For DML/DDL statements, execute and commit
                    cursor.execute(statement)
                    print(f"✅ Executed: {statement[:50]}...")
            
            self.connection.commit()
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error executing {file_path}: {e}")
            self.connection.rollback()
            return False
    
    def run_all_sql_scripts(self, scripts_folder='../database'):
        """Run all SQL scripts in order"""
        scripts = ['schema.sql', 'hotel_insertion.sql', 'processed_reviews.sql', 'ratings_insertion.sql']
        
        for script in scripts:
            script_path = os.path.join(scripts_folder, script)
            if os.path.exists(script_path):
                print(f"🚀 Running {script}...")
                self.execute_sql_file(script_path)
            else:
                print(f"⚠️ Script not found: {script_path}")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("✅ Database connection closed")

# VS Code integration function
def setup_database_from_vscode():
    """Run this from VS Code to set up the entire database"""
    db_manager = DatabaseManager()
    
    if db_manager.connect():
        try:
            db_manager.run_all_sql_scripts()
            print("🎉 Database setup complete!")
        finally:
            db_manager.close()
    else:
        print("❌ Database setup failed!")

if __name__ == "__main__":
    setup_database_from_vscode()