# VisionData 6Sem2025 ETL

## Project Description

VisionData 6Sem2025 ETL is an Extract, Transform, Load (ETL) automation project designed for data integration and logging. The project uses Python, Playwright, scheduling, and Elasticsearch to automate data flows, logging, and monitoring for VisionData's 6th semester 2025 activities. All code comments and log messages are standardized in English for internationalization and maintainability. Logging is handled via JSON format for both console and file outputs, and all docstrings follow English documentation standards.

## Technologies Used

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Schedule](https://img.shields.io/badge/Schedule-00A3E0?style=for-the-badge&logo=google-calendar&logoColor=white)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-005571?style=for-the-badge&logo=elasticsearch&logoColor=white)
![Pytest](https://img.shields.io/badge/pytest-0A0A0A?style=for-the-badge&logo=pytest&logoColor=white)
![Aspectlib](https://img.shields.io/badge/Aspectlib-FF5733?style=for-the-badge&logo=python&logoColor=white)
![ODBC](https://img.shields.io/badge/ODBC-008080?style=for-the-badge&logo=microsoft-access&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

## Project Structure

```bash
VisionData-6Sem2025ETL/
├── README.md                       # Project overview and usage instructions
├── requirements.txt                # List of Python dependencies for pip install
├── setup.py                        # setuptools script to build/install the package (wheel/sdist)
├── sonar-project.properties        # SonarCloud/SonarQube configuration (project key, sources, coverage path)
├── Dockerfile                      # Docker container configuration
├── docker-compose.yml              # Docker Compose orchestration file
├── production.env                  # Environment variables for production
├── .env.example                    # Environment variables template for development/local
└── src/                            # Source code root (package directory)
    ├── config/                     # Configuration loaders and env handling (YAML/JSON, env vars)
    ├── entities/                   # Domain models, dataclasses and schemas
    ├── process/                    # ETL orchestration and processing pipelines
    ├── services/                   # Service implementations (API clients, DB access, integrations)
    ├── test/                       # Tests (pytest) — test discovery path used by CI
    │   └── test_hello.py           # Simple smoke test to verify pytest/CI setup (e.g., assert True)
    └── utils/                      # Reusable helper functions and utilities
```

## Environment Configuration

Install dependencies:

```bash
pip install -r requirements.txt
```

Python version required: >=3.11

## Environment Variables
### Environment Files

- For production, use a file named **production.env** in the project root with your environment variables.
- For development/local, use the file **.env.example** as a template to create your own `.env` file.

**Important:** Do not put real or sensitive values in .env.example. The production.env file should be used only in production and contain your actual configuration.

#### Environment variable keys:

```dotenv
# ETL
AUTOMATION_NAME

# LOGGER PARAMS
LOGGER_LEVEL
LOGGER_FILE
LOGGER_OUTPUT

# ELASTICSEARCH PARAMS
ELASTICSEARCH_URL
ELASTICSEARCH_LOG_INDEX
ELASTICSEARCH_INDEX
ELASTICSEARCH_USER
ELASTICSEARCH_PASSWORD

# DATABASE PARAMS
CLIENT_DB_NAME
DW_DB_NAME
DB_HOST
DB_USER
DB_PASSWORD
DB_PORT

# SCHEDULING
SCHEDULE_TIME
```
### About .env Files

The project uses environment configuration files:

- **.env**: Main environment file for production/development. Copy from `.env.example` and fill with your values.
- **.env.example**: Template file showing all available environment variables with descriptions.

Configure your environment variables by copying the example file:

```bash
cp .env.example .env
# Edit .env with your actual values
```

Required environment variables:

- `APPLICATION_NAME`: Name of the application
- `LOGGER_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `LOGGER_FILE`: Output file for logs
- `LOGGER_OUTPUT`: Output type (FILE, CONSOLE, or both)
- `ELASTICSEARCH_URL`: Elasticsearch server URL
- `ELASTICSEARCH_INDEX`: Index name for Elasticsearch
- `ELASTICSEARCH_USER`: Elasticsearch username
- `ELASTICSEARCH_PASSWORD`: Elasticsearch password
- `DB_NAME`: Database name
- `DB_HOST`: Database host
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password
- `DB_PORT`: Database port (default: 5432)
- `SCHEDULE_TIME`: ETL execution schedule time

## How to Run

### Local Development

1. Configure your `.env` file:

   ```bash
   cp .env.example .env
   # Edit .env with your values
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Run the main script:

   ```bash
   python src/main.py
   ```

### Docker

#### Prerequisites

- Docker installed on your system
- Docker Compose installed

#### Running with Docker

1. **Configure environment variables:**

   ```bash
   cp .env.example .env
   # Edit .env with your actual database and Elasticsearch credentials
   ```

2. **Build and start the container:**

   ```bash
   docker-compose up -d
   ```

3. **View logs:**

   ```bash
   docker-compose logs -f visiondata-etl
   ```

4. **Run ETL manually:**

   ```bash
   docker-compose exec visiondata-etl python -m src.main
   ```

5. **Stop the container:**

   ```bash
   docker-compose down
   ```

#### Docker Commands Reference

```bash
# Build only (without starting)
docker-compose build

# Start in foreground (see logs directly)
docker-compose up

# Start in background
docker-compose up -d

# View real-time logs
docker-compose logs -f visiondata-etl

# Execute commands inside the running container
docker-compose exec visiondata-etl <command>

# Stop and remove containers
docker-compose down

# Stop, remove containers and volumes
docker-compose down -v

# Restart the service
docker-compose restart visiondata-etl
```

#### Docker Environment

The Docker setup includes:

- **Containerized ETL application** with all dependencies
- **Volume persistence** for logs (`/app/logs`)
- **Environment variable support** from `.env` file
- **Network isolation** for security
- **Automatic restart policy** (`unless-stopped`)

The container will:

1. Install all Python dependencies from `requirements.txt`
2. Set up the application environment
3. Run the ETL process once and exit
4. Restart automatically if it crashes (due to restart policy)

For continuous operation, you can:

- Use external schedulers (cron, Kubernetes CronJob)
- Modify `src/main.py` to include scheduling logic
- Use Docker restart policies for periodic execution

## Testing

### Run all pre-commit hooks and tests

```bash
pre-commit run --all-files
```

### Run specific tools

```bash
# Run Black formatter
pre-commit run black --all-files

# Run pytest tests
pytest src/test -q --maxfail=1 --disable-warnings

# Generate coverage report
pytest --cov=src --cov-report=xml src/test
```

## Observations

- All code comments and log messages are in English for internationalization and maintainability.
- Pre-commit hooks are configured to enforce commit message patterns and code style.
- Logging is handled via file and/or console as configured in environment variables, using JSON format for consistency.
- ETL scheduling is managed via the `schedule` library and can be customized using the `SCHEDULE_TIME` variable.
- Elasticsearch integration is used for storing, retrieving data, logging, and monitoring.
- Database parameters must be set in the `.env` file for full functionality.
- All docstrings and documentation follow English standards for easier collaboration.
- Docker support provides containerized deployment with environment isolation and easy scaling.
