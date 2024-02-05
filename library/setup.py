from setuptools import find_packages, setup

# read the contents of your README file
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='keydb_load_balancer',
    packages=find_packages(),
    version='1.0.0',
    description='Physcis project. Load balancer',
    author='Ainhoa Azqueta-Alzúaz',
    author_email='aazqueta@fi.upm.es',
    license='MIT',
    install_requires=['prometheus_api_client', 'keydb', 'prometheus_client'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
    python_requires='>=3.7'
    download_url='https://github.com/physics-faas/keydb-load-balancer/archive/refs/tags/v.1.0.0.tar.gz',
    long_description=long_description,
    long_description_content_type='text/markdown'
)