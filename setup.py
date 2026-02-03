from setuptools import setup, find_packages

setup(
    name="sg-air-quality-etl",      # pip install name (can have hyphens)
    version="0.1",
    packages=find_packages(),       # finds sg_air_quality and subpackages
    install_requires=[
        "pandas",
        "python-dotenv",
        "requests",
        "google-cloud-bigquery",
        "pyarrow",
        "pandas_gbq"
    ],
)

