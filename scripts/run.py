"""
Simple runner script - Use this to run everything
"""
import os
import sys
from pathlib import Path

def run_script(script_name, description):
    """Run a Python script with nice output"""
    print(f"\n{'='*50}")
    print(f"🚀 {description}")
    print(f"{'='*50}")
    
    script_path = Path(__file__).parent / script_name
    
    if not script_path.exists():
        print(f"❌ Script not found: {script_path}")
        return False
    
    try:
        # Use system Python to run the script
        exit_code = os.system(f'python "{script_path}"')
        if exit_code == 0:
            print(f"✅ {description} completed successfully!")
            return True
        else:
            print(f"❌ {description} failed with exit code: {exit_code}")
            return False
    except Exception as e:
        print(f"💥 Error running {script_name}: {e}")
        return False

def main():
    """Main runner function"""
    print("🎯 CIT444 Project Runner")
    print("Running from current directory:", Path(__file__).parent)
    
    # Check if we're in the right directory
    if not Path("data_processor.py").exists():
        print("❌ Please run this from the scripts directory!")
        return
    
    steps = [
        ("setup_project.py", "Project Setup"),
        ("data_processor.py", "Data Processing"), 
        ("sentiment_analyzer.py", "Sentiment Analysis"),
        ("word_dictionary.py", "Word Frequency Analysis")
    ]
    
    for script, description in steps:
        if not run_script(script, description):
            print(f"\n⏹️ Stopping pipeline due to error in {description}")
            break
    else:
        print(f"\n{'🎉'*20}")
        print("ALL STEPS COMPLETED SUCCESSFULLY!")
        print(f"{'🎉'*20}")

if __name__ == "__main__":
    main()