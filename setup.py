from setuptools import setup, find_packages
from setuptools.command.install import install as _install

# Custom install command to run 'playwright install'
# class InstallCommand(_install):
#     def run(self):
#         _install.run(self)
#         # Run 'playwright install' after installation
#         import subprocess
#         subprocess.run(['playwright', 'install'], check=True)

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
            'amfi_irdai_bot_runner = src.main:main',  # Adjust the module and function name accordingly
        ],
    },
    cmdclass={
        'install': InstallCommand,
    }
)
