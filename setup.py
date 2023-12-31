from setuptools import find_packages, setup

setup(
    name="ohtk_etl",
    packages=find_packages(exclude=["ohtk_etl_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "psycopg2",
        "pandas",
        "pyarrow",
        "sqlalchemy",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
