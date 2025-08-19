from setuptools import setup, find_packages
import os

# Read the contents of your README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read the contents of your requirements file
with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = f.read().splitlines()

setup(
    name="foxglove2zmq",
    version="0.1.0",
    author="Hamza El-Kebir",
    author_email="ha.elkebir@gmail.com",
    description="A relay for forwarding Foxglove WebSocket messages to a ZMQ server.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/helkebir/foxglove2zmq",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: System :: Networking",
        "Framework :: AsyncIO",
    ],
    python_requires='>=3.7',
    entry_points={
        'console_scripts': [
            'foxglove2zmq=foxglove2zmq.cli:main',
        ],
    },
)
