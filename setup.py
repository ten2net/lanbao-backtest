from setuptools import setup, find_packages

setup(
    name="lanbao-backtest",
    version="0.1.0",
    description="揽宝量化回测引擎与模拟盘基础设施",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "pandas",
        "numpy",
    ],
)
