from setuptools import setup

setup(
    name='I3ToSQLite',
    version='0.1.0',    
    description='An unoffical I3-file to SQLite database converter.',
    url='https://github.com/RasmusOrsoe/I3ToSQLite',
    author='Rasmus F. Ørsøe',
    author_email='RasmusOrsoe@gmail.com',
    license='MIT',
    packages=['I3ToSQLite'],
    install_requires=['sqlalchemy',
                      'pandas',
                      'numpy'],

    classifiers=[
        'Development Status :: 1 - VERSION 0.1',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',  
        'Operating System :: POSIX :: Linux',        
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)