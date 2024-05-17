from setuptools import setup, find_packages

setup(
    name='spark-column-analyzer',  # Replace 'your_package_name' with the desired package name
    version='0.1',  # Update the version number accordingly
    packages=find_packages(),  # Automatically find packages in the current directory
    author='iMich Talebzadeh',  # Replace 'Your Name' with your name
    author_email='mich.talebzadeh@gmail.com',  # Replace 'your_email@example.com' with your email
    description='A package for analyzing PySpark DataFrame columns',  # Update the description
    long_description=open('README.md').read(),  # Load README.md as the long description
    long_description_content_type='text/markdown',  # Set the long description content type
    url='https://github.com/your_username/your_package_name',  # Update the URL to your package repository
    install_requires=[
        'pyspark',  # Add any dependencies required by your package
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)

