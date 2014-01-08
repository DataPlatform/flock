from setuptools import setup
from pip.req import parse_requirements


# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements('requirements.txt')

# reqs is a list of requirement
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='Flock',
    version='0.1.0',
    author='Stefan Fox',
    author_email='Stefan.Fox@cfpb.gov',
    packages=['flock',],
    scripts=[],
    url='https://github.com/DataPlatform/flock/wiki/',
    license='',
    description='Useful DB-related stuff.',
    long_description=open('README.md').read(),
    install_requires=reqs,
)
