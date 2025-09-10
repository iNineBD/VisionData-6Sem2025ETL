
# VisionData 6Sem2025 ETL

## Project Description
VisionData 6Sem2025 ETL is an Extract, Transform, Load (ETL) automation project designed for data integration and logging. It leverages Python, Playwright, scheduling, and Elasticsearch to automate data flows, logging, and monitoring for VisionData's 6th semester 2025 activities.

## Technologies Used
<p align="left">
	<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
	<img src="https://img.shields.io/badge/Schedule-00A3E0?style=for-the-badge&logo=google-calendar&logoColor=white" alt="Schedule" />
	<img src="https://img.shields.io/badge/Elasticsearch-005571?style=for-the-badge&logo=elasticsearch&logoColor=white" alt="Elasticsearch" />
	<img src="https://img.shields.io/badge/Polars-FFDD00?style=for-the-badge&logo=polars&logoColor=black" alt="Polars" />
	<img src="https://img.shields.io/badge/pytest-0A0A0A?style=for-the-badge&logo=pytest&logoColor=white" alt="Pytest" />
</p>

## Project Structure

```
vision_data_logger_output/
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

- **development.env**: Used for local development and testing. It allows you to set parameters and credentials for a development environment, enabling debug logging and flexible output options.
- **production.env**: Used for deployment in production. It contains the settings and credentials for the live environment, with stricter logging and output configurations for security and reliability.


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

- Pre-commit hooks are configured to enforce commit message patterns and code style.
- Logging is handled via file and/or console as configured in the environment variables.
- ETL scheduling is managed via the `schedule` library and can be customized using the `SCHEDULE_TIME` variable.
- Elasticsearch integration is available for logging and monitoring.
- Database parameters must be set in the `.env` file for full functionality.
