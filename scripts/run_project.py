"""
CIT444 Final Project - VS Code Integrated Runner
Run this file to execute the entire project pipeline
"""

import subprocess
import sys
from pathlib import Path

class ProjectRunner:
    def __init__(self):
        self.scripts_dir = Path(__file__).parent
        self.project_root = self.scripts_dir.parent
        
    def run_step(self, step_name, script_path, args=None):
        """Run a project step with nice formatting"""
        print(f"\n{'='*60}")
        print(f"🚀 {step_name}")
        print(f"{'='*60}")
        
        try:
            if args:
                result = subprocess.run([sys.executable, script_path] + args, 
                                      cwd=self.scripts_dir, capture_output=True, text=True)
            else:
                result = subprocess.run([sys.executable, script_path], 
                                      cwd=self.scripts_dir, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✅ {step_name} completed successfully!")
                print(result.stdout)
                return True
            else:
                print(f"❌ {step_name} failed!")
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                return False
                
        except Exception as e:
            print(f"💥 Error running {step_name}: {e}")
            return False
    
    def run_complete_pipeline(self):
        """Run the complete project pipeline"""
        print("🎯 CIT444 Hotel Analysis Project - Complete Pipeline")
        print("📁 Running from VS Code Integrated Environment")
        
        steps = [
            ("Data Processing", "data_processor.py", None),
            ("Sentiment Analysis", "sentiment_analyzer.py", None),
            ("Word Frequency Analysis", "word_dictionary.py", None),
            ("Database Setup", "database_manager.py", None)
        ]
        
        for step_name, script, args in steps:
            success = self.run_step(step_name, script, args)
            if not success:
                print(f"⏹️ Pipeline stopped at {step_name}")
                return False
        
        print(f"\n{'🎉'*20}")
        print("ALL STEPS COMPLETED SUCCESSFULLY!")
        print("Next: Run the JavaFX GUI from the GUI folder")
        print(f"{'🎉'*20}")
        return True
    
    def create_sample_project(self):
        """Create a sample project for testing"""
        print("🧪 Creating sample project structure...")
        return self.run_step("Create Sample Data", "data_processor.py", ["--sample"])

if __name__ == "__main__":
    runner = ProjectRunner()
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "sample":
            runner.create_sample_project()
        elif sys.argv[1] == "db":
            runner.run_step("Database Setup", "database_manager.py")
        else:
            runner.run_complete_pipeline()
    else:
        runner.run_complete_pipeline()