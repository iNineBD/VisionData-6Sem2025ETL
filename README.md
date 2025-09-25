
# VisionData 6Sem2025 ETL

## Project Description
VisionData 6Sem2025 ETL is an Extract, Transform, Load (ETL) automation project designed for data integration and logging. The project uses Python, Playwright, scheduling, and Elasticsearch to automate data flows, logging, and monitoring for VisionData's 6th semester 2025 activities. All code comments and log messages are standardized in English for internationalization and maintainability. Logging is handled via JSON format for both console and file outputs, and all docstrings follow English documentation standards.

## Technologies Used
<p align="left">
	<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
	<img src="https://img.shields.io/badge/Schedule-00A3E0?style=for-the-badge&logo=google-calendar&logoColor=white" alt="Schedule" />
	<img src="https://img.shields.io/badge/Elasticsearch-005571?style=for-the-badge&logo=elasticsearch&logoColor=white" alt="Elasticsearch" />
	<img src="https://img.shields.io/badge/pytest-0A0A0A?style=for-the-badge&logo=pytest&logoColor=white" alt="Pytest" />
	<img src="https://img.shields.io/badge/Aspectlib-FF5733?style=for-the-badge&logo=python&logoColor=white" alt="Aspectlib" />
	<img src="https://img.shields.io/badge/ODBC-008080?style=for-the-badge&logo=microsoft-access&logoColor=white" alt="ODBC" />
</p>

src/
	config/
		aop_logging.py
		db_connector.py
		dotenv_loader.py
		elastic_handler.py
		logger.py
	entities/
	process/
		etl_processor.py
		scheduler.py
	utils/
## Project Structure

```
vision_data_logger_output/
src/
	config/
		aop_logging.py
		db_connector.py
		dotenv_loader.py
		elastic_client.py
		logger.py
	entities/
	process/
		dw_etl_processor.py
		elastic_etl_processor.py
		scheduler.py
	services/
		extract_dw_service.py
		extract_elastic_service.py
		load_dw_service.py
		transform_dw_service.py
		transforme_elastic_service.py
	test/
		(test_hello.py)
	utils/
requirements.txt
development.env
production.env
setup.py
.pre-commit-config.yaml
.gitignore
README.md
```

## Environment Configuration

Install dependencies:

```bash
pip install -r requirements.txt
```

Python version required: >=3.11

## Environment Variables

### About .env Files

The project uses two environment configuration files:

- **development.env**: Used for local development and testing. Set parameters and credentials for a development environment, enable debug logging, and flexible output options. All log messages and comments are in English.
- **production.env**: Used for deployment in production. Contains the settings and credentials for the live environment, with stricter logging and output configurations for security and reliability. All log messages and comments are in English.


## How to Run

1. Configure your `.env` file (choose `development.env` or `production.env`).
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the main script:
   ```bash
   python src/main.py
   ```

## Observations

- All code comments and log messages are now in English for internationalization and maintainability.
- Pre-commit hooks are configured to enforce commit message patterns and code style.
- Logging is handled via file and/or console as configured in the environment variables, using JSON format for consistency.
- ETL scheduling is managed via the `schedule` library and can be customized using the `SCHEDULE_TIME` variable.
- Elasticsearch integration is used as a database for storing and retrieving data, as well as for logging and monitoring.
- Database parameters must be set in the `.env` file for full functionality.
- All docstrings and documentation follow English standards for easier onboarding and collaboration.
