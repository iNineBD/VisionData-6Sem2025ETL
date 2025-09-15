from setuptools import find_packages, setup


def read_requirements():
    try:
        with open("requirements.txt", "r") as f:
            return [
                line.strip() for line in f if line.strip() and not line.startswith("#")
            ]
    except FileNotFoundError:
        return []


setup(
    name="VisionData6Sem2025ETL",
    version="0.1.0",
    description="ETL project for VisionData 6th semester 2025",
    author="Inine",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=read_requirements(),
)
