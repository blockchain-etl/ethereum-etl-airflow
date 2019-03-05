from __future__ import print_function

import logging

from build_load_dag import build_load_dag
from build_load_dag_redshift import build_load_dag_redshift
from variables import read_load_dag_vars
from variables import read_load_dag_redshift_vars
from variables import read_var

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# Default is gcp
cloud_provider = read_var('cloud_provider', var_prefix=None, required=False, cloud_provider='gcp')

if cloud_provider == 'gcp':
    DAG = build_load_dag(
        dag_id='ethereum_load_dag',
        chain='ethereum',
        **read_load_dag_vars(
            var_prefix='ethereum_',
            schedule_interval='30 1 * * *'
        )
    )
elif cloud_provider == 'aws':
    DAG = build_load_dag_redshift(
        dag_id='ethereum_load_dag',
        chain='ethereum',
        **read_load_dag_redshift_vars(
            var_prefix='ethereum_',
            schedule_interval='30 1 * * *'
        )
    )
else:
    raise ValueError('You must set a valid cloud_provider Airflow variable (gcp,aws)')
