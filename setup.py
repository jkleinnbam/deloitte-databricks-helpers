import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='deloitte_databricks_helpers',       # name of the package
    version='0.0.2',                          # release version
    author='BEADAnalytics',                   # org/author
    description=\
        '''
        deloitte_databricks_helpers

        Functions the BEAD Analytics team uses often to help with common data processing.
        #TODO Add a better description
        #All functions must have docstrings
         
        ''',
    long_description=long_description,      # long description read from the the readme file
    long_description_content_type='text/markdown',
    classifiers=[                           # information to filter the project on PyPi website
                        'Programming Language :: Python :: 3',
                        'License :: OSI Approved :: MIT License',
                        'Operating System :: OS Independent',
                        'Natural Language :: English',
                        'Programming Language :: Python :: 3.7',
                        ],                                      
    python_requires='>=3.7',                # minimum version requirement of the package
    #py_modules=['functions', 'schemas'],      # name of the python package     
    package_dir={'':'src'},                 # directory of the source code of the package
    packages=setuptools.find_packages(where="src"), # list of all python modules to be installed
    install_requires=[           # package dependencies
                        'pandas>=1.1.0,<2.1.0',  
                        'numpy<1.25,>=1.16.0',
                        'openpyxl',
                    ]
    )
