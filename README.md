# clinical-trials-exploration

Exploring public clinical trials dataset as a part of the DE tech challenge.

# Clinical Trial Pipeline - Setup

This repository contains a clinical-trial data pipeline and UI. Below are recommended steps to set up local development and production (runtime) environments.

## Files
- `requirements.txt` — production / runtime dependencies (minimal).
- `requirements-dev.txt` — development dependencies (includes `-r requirements.txt`).

## Quick start (recommended: use a virtual environment)

1. From project root:
\`\`\`bash
# create a backup of the old requirements if you haven't already
cp requirements.txt requirements.txt.bak
\`\`\`

2. Create & activate a virtualenv (Linux/macOS):
\`\`\`bash
python3 -m venv .venv
source .venv/bin/activate
\`\`\`

3. Install runtime deps (for deploying or running the app):
\`\`\`bash
pip install -r requirements.txt
\`\`\`

4. Install development deps (for development, linting, tests):
\`\`\`bash
pip install -r requirements-dev.txt
\`\`\`

## Running locally
- Streamlit app (if present):
\`\`\`bash
streamlit run path/to/your_app.py
\`\`\`

## Notes & recommendations
- \`great_expectations\` is in \`requirements-dev.txt\` because it's often used in development/CI to validate data. If you need it in production pipelines, move it to \`requirements.txt\`.
- For reproducible deployments, consider using a constraints file or \`pip-compile\` to lock transitive dependency versions.
- Keep dev tools (black, isort, pre-commit, flake8) in \`requirements-dev.txt\` or prefer a \`pyproject.toml\` + dev extras if you migrate to Poetry or pip-tools later.

## TO DO
- Finish implementing configs, for logging and for environment variable handling
- Change logging statements throughout the codebase to use the logging module (moving logs to a file would be a good start)
- Add .env.example setup instructions
- Add pre-commit setup
- Add instructions for setting up and running the PostgreSQL database (locally and via Docker)
- Add instructions for initializing the database schema
- Add unit tests
- Improve error handling and validation
- Create data ingestion and processing scripts
- Create Streamlit app for data exploration and visualization