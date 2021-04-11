from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'CSV Ingestor Package'
LONG_DESCRIPTION = 'Async ingestion of CSV file to a database'

# Setting up
setup(
        name="file_ingestor", 
        version=VERSION,
        author="Rishabh Gulati",
        author_email="risgulati@agmail.com",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=["asyncio","aiofiles","aiocsv","random","time","csv","datetime","mysql-connector-python"],
        keywords=['python', 'package'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Assignment Evaluators",
            "Programming Language :: Python :: 3.8",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)