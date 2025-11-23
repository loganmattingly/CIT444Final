FROM python:3.11-slim

# Keep Python from writing .pyc files and bufferless stdout
ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1 \
	PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

# System dependencies for pandas/torch/cx_Oracle
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		build-essential \
		libaio1t64 \
		git \
	&& rm -rf /var/lib/apt/lists/*

# Install Python dependencies listed in scripts/setup_environment.py
RUN pip install --no-cache-dir \
	pandas>=1.3.0 \
	nltk>=3.6.0 \
	transformers>=4.0.0 \
	torch>=1.9.0 \
	tqdm>=4.60.0 \
	numpy>=1.21.0 \
	sqlalchemy>=1.4.0 \
	cx_Oracle>=8.3.0

# Copy project code into the image
COPY . .

# Pre-download the NLTK assets used by main_workflow
RUN python -m nltk.downloader punkt stopwords wordnet

# Default command runs the orchestrator script; override for single steps
# CMD ["python", "scripts/run_project.py"]
CMD ["python", "scripts/database_manager.py"]
