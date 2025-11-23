"""
CIT444 Project Setup - Fixed Paths
Run this once to set up the environment
"""
import os
import subprocess
import sys
import venv
from pathlib import Path

def setup_virtual_environment():
    """Create and set up Python virtual environment in scripts/venv"""
    print("🔧 Setting up Python virtual environment...")
    
    scripts_dir = Path(__file__).parent
    venv_path = scripts_dir / "venv"
    
    if venv_path.exists():
        print("✅ Virtual environment already exists")
        return get_python_path()
    
    try:
        venv.create(venv_path, with_pip=True)
        print("✅ Virtual environment created successfully")
        return get_python_path()
        
    except Exception as e:
        print(f"❌ Error creating virtual environment: {e}")
        return None

def get_python_path():
    """Get the correct Python path for the current OS"""
    scripts_dir = Path(__file__).parent
    if os.name == 'nt':  # Windows
        python_path = scripts_dir / "venv/Scripts/python.exe"
    else:  # Linux/Mac
        python_path = scripts_dir / "venv/bin/python"
    
    if python_path.exists():
        return str(python_path)
    else:
        print(f"❌ Python not found at expected path: {python_path}")
        return None

def install_requirements(python_path):
    """Install required packages"""
    print("📦 Installing required packages...")
    
    requirements = [
        "pandas>=1.3.0",
        "nltk>=3.6.0", 
        "transformers>=4.0.0",
        "torch>=1.9.0",
        "tqdm>=4.60.0",
        "numpy>=1.21.0"
    ]
    
    try:
        for package in requirements:
            print(f"  Installing {package}...")
            result = subprocess.run([
                python_path, "-m", "pip", "install", package
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"    ✅ {package} installed")
            else:
                print(f"    ⚠️  {package} had issues: {result.stderr[:100]}...")
        
        print("✅ Package installation completed")
        return True
        
    except Exception as e:
        print(f"❌ Error installing packages: {e}")
        return False

def create_project_structure():
    """Create the necessary project folder structure in the ROOT directory"""
    print("📁 Creating project structure...")
    
    project_root = Path(__file__).parent.parent
    
    folders = [
        project_root / "raw_data",
        project_root / "processed_data",
        project_root / "database",
        project_root / "gui/CIT444FinalGUI/src/Application"
    ]
    
    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)
        print(f"  ✅ Created {folder.relative_to(project_root)}")
    
    print("✅ Project structure created")

def main():
    """Main setup function"""
    print("🎯 CIT444 Hotel Analysis Project Setup")
    print("=" * 50)
    
    # We're in scripts directory, setup everything relative to project root
    scripts_dir = Path(__file__).parent
    project_root = scripts_dir.parent
    
    print(f"Project root: {project_root}")
    print(f"Scripts directory: {scripts_dir}")
    
    # Step 1: Create virtual environment in scripts/venv
    python_path = setup_virtual_environment()
    if not python_path:
        print("❌ Setup failed: Could not create virtual environment")
        return False
    
    # Step 2: Install requirements
    if not install_requirements(python_path):
        print("❌ Setup failed: Could not install packages")
        return False
    
    # Step 3: Create project structure in ROOT
    create_project_structure()
    
    print("\n" + "=" * 50)
    print("🎉 SETUP COMPLETED SUCCESSFULLY!")
    print("=" * 50)
    print("\n📁 Your project structure:")
    print("CIT444_Final_Project/")
    print("├── raw_data/                 ← PUT YOUR DATA HERE")
    print("│   ├── city_name/")
    print("│   │   └── hotel_folder/")
    print("│   │       └── reviews      ← Files with no extension")
    print("├── scripts/                  ← Python scripts")
    print("├── processed_data/           ← Generated files")
    print("├── database/                 ← SQL scripts") 
    print("└── gui/                      ← JavaFX application")
    print("\n🚀 Next steps:")
    print("1. Add your hotel data to the 'raw_data' folder")
    print("2. Run: python scripts/data_processor.py")
    print("3. Run: python scripts/sentiment_analyzer.py")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)