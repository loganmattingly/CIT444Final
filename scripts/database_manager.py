import os
from typing import List

import oracledb
import pandas as pd

class DatabaseManager:
    """Handle Oracle connections and run project SQL scripts."""

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        service_name: str | None = None,
    ):
        self.username = username or os.getenv("ORACLE_USER", "system")
        self.password = password or os.getenv("ORACLE_PASSWORD", "adminpw")
        self.host = host or os.getenv("ORACLE_HOST", "localhost")
        self.port = int(port or os.getenv("ORACLE_PORT", 1521))
        self.service_name = service_name or os.getenv("ORACLE_SERVICE", "XEPDB1")
        self.connection = None
    
    def connect(self):
        """Connect to Oracle database"""
        try:
            dsn = oracledb.makedsn(
                self.host,
                self.port,
                service_name=self.service_name,
            )
            self.connection = oracledb.connect(
                user=self.username,
                password=self.password,
                dsn=dsn,
            )
            print("✅ Connected to Oracle Database")
            return True
        except oracledb.Error as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def _prepare_statements(self, sql_content: str) -> List[str]:
        """Strip comments, ignore SQL*Plus commands, split SQL/PLSQL statements."""

        sqlplus_prefixes = (
            "SET ",
            "SPOOL",
            "PROMPT",
            "SHOW ",
            "WHENEVER",
            "ACCEPT",
            "EXIT",
        )

        statements: List[str] = []
        buffer: List[str] = []
        in_block_comment = False
        in_plsql_block = False

        def flush_buffer():
            nonlocal buffer, in_plsql_block
            stmt = "\n".join(buffer).strip()
            if stmt.endswith(';') and not in_plsql_block:
                stmt = stmt[:-1].strip()
            if stmt:
                statements.append(stmt)
            buffer = []
            in_plsql_block = False

        for raw_line in sql_content.splitlines():
            stripped = raw_line.strip()

            if in_block_comment:
                if "*/" in stripped:
                    in_block_comment = False
                continue

            if stripped.startswith("/*"):
                if not stripped.endswith("*/"):
                    in_block_comment = True
                continue

            if not stripped or stripped.startswith("--"):
                continue

            upper = stripped.upper()
            if upper.startswith(sqlplus_prefixes):
                continue

            if upper == "/":
                if buffer:
                    flush_buffer()
                continue

            buffer.append(stripped)

            if not in_plsql_block:
                first_token = upper.split()[0]
                if first_token in {"DECLARE", "BEGIN"}:
                    in_plsql_block = True

            if not in_plsql_block and stripped.endswith(';'):
                flush_buffer()

        if buffer:
            flush_buffer()

        return statements

    def execute_sql_file(self, file_path):
        """Execute SQL file from VS Code"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read()

            statements = self._prepare_statements(sql_content)

            cursor = self.connection.cursor()
            for statement in statements:
                try:
                    cursor.execute(statement)
                    if cursor.description:
                        results = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description]
                        df = pd.DataFrame(results, columns=columns)
                        print(f"📊 Results for: {statement[:60]}...")
                        print(df)
                    else:
                        print(f"✅ Executed: {statement[:60]}...")
                except Exception as stmt_err:
                    print(f"❌ Statement failed: {statement[:120]}...")
                    raise stmt_err

            self.connection.commit()
            cursor.close()
            return True

        except Exception as e:
            print(f"❌ Error executing {file_path}: {e}")
            self.connection.rollback()
            return False
    
    def run_all_sql_scripts(self, scripts_folder='database'):
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