import subprocess
import sys
import os

def setup_environment():
    """Set up Python virtual environment and install dependencies"""
    
    # Create virtual environment
    subprocess.run([sys.executable, "-m", "venv", "venv"])
    
    # Determine the correct pip path
    if os.name == 'nt':  # Windows
        pip_path = "venv/Scripts/pip.exe"
        python_path = "venv/Scripts/python.exe"
    else:  # Linux/Mac
        pip_path = "venv/bin/pip"
        python_path = "venv/bin/python"
    
    # Install requirements
    requirements = [
        "pandas>=1.3.0",
        "nltk>=3.6.0", 
        "transformers>=4.0.0",
        "torch>=1.9.0",
        "tqdm>=4.60.0",
        "numpy>=1.21.0",
        "sqlalchemy>=1.4.0",
        "cx_oracle>=8.3.0"
    ]
    
    for package in requirements:
        subprocess.run([pip_path, "install", package])
    
    print("✅ Virtual environment setup complete!")
    print(f"✅ Python interpreter: {python_path}")
    return python_path

if __name__ == "__main__":
    setup_environment()