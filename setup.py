from setuptools import setup, find_packages

setup(
    name='spark_column_analyzer',
    version='0.2.2',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        'pyspark',
    ],
    include_package_data=True,
    zip_safe=False,
)
