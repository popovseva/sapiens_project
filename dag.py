# automation of loading data and creating a data mart in Greenplum
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

DB_CONN = "gp_std6_116" # airflow connection name
DB_SCHEMA = 'std6_116' # greenplum schema name
DB_PROC_LOAD = 'f_full_load' # function in greenplum
FULL_LOAD_TABLES = ['stores', 'promos', 'promo_types']
FULL_LOAD_FILES = {'stores': 'stores', 'promos': 'promos', 'promo_types': 'promo_types'}
MD_TABLE_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_PROC_LOAD}(p_table_to_name := %(tab_name)s, p_file_name := %(file_name)s, p_ip := %(ip_address)s)"

DB_PROC_LOAD_PART = 'f_load_delta_partitions' # function in greenplum
PK = {'coupons': 'date', 'traffic': 'date', 'bills_head': 'calday', 'bills_item': 'calday'} # partition keys
D_START = '2021-01-01'
D_END = '2021-02-28'

PART_LOAD_TABLES = ['coupons', 'traffic', 'bills_head', 'bills_item']
PART_LOAD_FILES = {'coupons': 'coupons', 'traffic': 'traffic', 'bills_head': 'bills_head', 'bills_item': 'bills_item'}
PART_LOAD_PROTOCOLS = {'coupons': 'GPFDIST', 'traffic': 'PXF', 'bills_head': 'PXF', 'bills_item': 'PXF'}

# define connection parameters for PXF and GPFDIST
CONNECTION_PARAMS = {
    'PXF': {'ip_address': 'your_postgres_ip', 'username': 'your_postgres_user', 'password': 'your_postgres_password'},
    'GPFDIST': {'ip_address': 'your_gpfdist_ip'}
}

MD_LOAD_PART_TABLE_QUERY_PXF = f"""select {DB_SCHEMA}.{DB_PROC_LOAD_PART}(
    p_table_to_name := %(tab_name)s, 
    p_source := %(source_name)s, 
    p_partition_key := %(part_key)s, 
    p_start_date := '{D_START}', 
    p_end_date := '{D_END}', 
    ext_protocol := %(p_ext_protocol)s, 
    p_ip := %(ip_address)s, 
    p_user := %(username)s, 
    p_pass := %(password)s
)"""

MD_LOAD_PART_TABLE_QUERY_GPFDIST = f"""select {DB_SCHEMA}.{DB_PROC_LOAD_PART}(
    p_table_to_name := %(tab_name)s, 
    p_source := %(source_name)s, 
    p_partition_key := %(part_key)s, 
    p_start_date := '{D_START}', 
    p_end_date := '{D_END}', 
    ext_protocol := %(p_ext_protocol)s, 
    p_ip := %(ip_address)s
)"""

DB_PROC_MART = 'f_load_mart_proj'
DT_START = '2021-01-01'
DT_END = '2021-02-28'
MD_MART = f"select {DB_SCHEMA}.{DB_PROC_MART}('{DT_START}', '{DT_END}')"

default_args = {
    'depends_on_past': False,
    'owner': 'std6_116',
    'start_date': datetime(2024, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "std6_116_project",
    max_active_runs=4,
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    task_start = DummyOperator(task_id="start")

    with TaskGroup('full_insert') as task_full_insert_tables:
        for table in FULL_LOAD_TABLES:
            task = PostgresOperator(
                task_id=f'load_table_{table}',
                postgres_conn_id=DB_CONN,
                sql=MD_TABLE_LOAD_QUERY,
                parameters={'tab_name': f'{DB_SCHEMA}.{table}', 'file_name': f'{FULL_LOAD_FILES[table]}', 'ip_address': CONNECTION_PARAMS['GPFDIST']['ip_address']}
            )

    with TaskGroup('part_load') as task_part_load_tables:
        for table in PART_LOAD_TABLES:
            protocol = PART_LOAD_PROTOCOLS[table]
            if protocol == 'PXF':
                task = PostgresOperator(
                    task_id=f'part_table_{table}',
                    postgres_conn_id=DB_CONN,
                    sql=MD_LOAD_PART_TABLE_QUERY_PXF,
                    parameters={
                        'tab_name': f'{DB_SCHEMA}.{table}',
                        'source_name': f'{PART_LOAD_FILES[table]}',
                        'part_key': f'{PK[table]}',
                        'p_ext_protocol': protocol,
                        'ip_address': CONNECTION_PARAMS[protocol]['ip_address'],
                        'username': CONNECTION_PARAMS[protocol]['username'],
                        'password': CONNECTION_PARAMS[protocol]['password']
                    }
                )
            else:  # GPFDIST
                task = PostgresOperator(
                    task_id=f'part_table_{table}',
                    postgres_conn_id=DB_CONN,
                    sql=MD_LOAD_PART_TABLE_QUERY_GPFDIST,
                    parameters={
                        'tab_name': f'{DB_SCHEMA}.{table}',
                        'source_name': f'{PART_LOAD_FILES[table]}',
                        'part_key': f'{PK[table]}',
                        'p_ext_protocol': protocol,
                        'ip_address': CONNECTION_PARAMS[protocol]['ip_address']
                    }
                )

    task_mart = PostgresOperator(
        task_id='load_mart',
        postgres_conn_id=DB_CONN,
        sql=MD_MART
    )

    task_end = DummyOperator(task_id="end")

    task_start >> task_full_insert_tables >> task_part_load_tables >> task_mart >> task_end