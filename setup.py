from setuptools import setup, find_packages

setup(
    name="lakehouse-accidents-us",
    version="1.0.0",
    description="Architecture Lakehouse pour l'analyse des accidents US",
    author="BDFS3 Team",
    author_email="team@bdfs3.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.4.0",
        "py4j>=0.10.9.7",
        "pymysql>=1.0.3",
        "sqlalchemy>=2.0.15",
        "hive-thrift-py>=0.7.0",
        "fastapi>=0.100.0",
        "uvicorn>=0.22.0",
        "pydantic>=2.0.0",
        "scikit-learn>=1.3.0",
        "mlflow>=2.4.0",
        "numpy>=1.24.0",
        "pandas>=2.0.0",
        "python-dotenv>=1.0.0",
        "pyyaml>=6.0",
        "structlog>=23.1.0",
        "click>=8.1.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.11.0",
            "black>=23.3.0",
            "flake8>=6.0.0",
            "mypy>=1.4.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "feeder=applications.feeder.feeder_app:main",
            "preprocessor=applications.preprocessor.preprocessor_app:main",
            "datamart=applications.datamart.datamart_app:main",
            "ml-training=applications.ml_training.ml_training_app:main",
            "api-server=api.main:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)