from setuptools import find_packages, setup

setup(
    name="fda_etl_project",
    packages=find_packages(exclude=["fda_etl_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
