from setuptools import setup, find_packages

# Read requirements from requirements.txt file
with open('requirements.txt', 'r', encoding='utf-16') as f:
    requirements = f.read().splitlines()

setup(
    name='Irdai and afmi',
    version='1.0',
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'irdai_bot_runner = src.main:main',
        ],
    }
)
