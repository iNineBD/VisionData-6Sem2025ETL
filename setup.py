from setuptools import setup, find_packages

setup(
    name="VisionData6Sem2025ETL",
    version="0.1.0",
    description="ETL project for VisionData 6th semester 2025",
    author="Inine",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=[],
)
