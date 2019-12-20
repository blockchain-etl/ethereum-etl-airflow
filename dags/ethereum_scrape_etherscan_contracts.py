from ethereumetl_airflow.build_scrape_etherscan_contracts_dag import build_scrape_etherscan_contracts_dag

from ethereumetl_airflow.variables import read_scrape_etherscan_contracts_dag_vars

# When searching for DAGs, Airflow will only consider files where the string "airflow" and "DAG" both appear in the
# contents of the .py file.
DAG = build_scrape_etherscan_contracts_dag(
    dag_id='scrape_etherscan_contracts_dag',
    **read_scrape_etherscan_contracts_dag_vars(
        var_prefix='ethereum_',
        scrape_schedule_interval='0 13 * * *',
        scrape_start_date='2015-07-30',
    )
)
