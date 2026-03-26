import papermill as pm
import logging
import sys

pm.execute_notebook(
    '/home/sandbox/personal-repos/DA-3564/DA-3564.ipynb', 
    '/home/sandbox/personal-repos/DA-3564/standalone/DA-3564_output.ipynb',
    parameters=dict()
)