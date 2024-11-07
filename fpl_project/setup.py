from setuptools import find_packages, setup

setup(
    name="fpl_project",
    packages=find_packages(exclude=["fpl_project_tests"]),
    install_requires=["dagster", "dagster-cloud"],
    extras_require={"dev": ["dagster-webserver", "pytest", "pandas"]},
)
