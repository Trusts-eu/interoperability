from setuptools import setup

setup(
    name='interoperability',
    version='0.1',
    description='A collection of connectors to third party data repositories.',
    url='http://github.com/storborg/funniest',
    author='Stefan Gindl',
    author_email='stefan.gindl@researchstudio.at',
    license='MIT',
    packages=['interoperability'],
    install_requires=[
        'requests==2.27.1',
    ],
    zip_safe=False,
)
