from setuptools import find_packages, setup
from typing import List

HYPEN_E_DOT = '-e .'
def get_requirements(file_path) ->List[str]:

    '''
    this function return the list of requirements
    '''
    requirements=[]
    with open(file_path) as file_obj:
        requirements = file_obj.readlines()
        [req.replace("\n", "") for req in requirements]

        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)
            
    return requirements

setup(
name='cementStrengthPrediction',
version= '1.0.0',
author='Amin',
author_email='sharifmia236@gmail.com',
packages=find_packages(),
install_requires = get_requirements('requirements.txt')
)