from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Ravi',
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id='etl_demo',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False
)

create_raw_ddl = BashOperator(
    task_id='create_raw_ddl',
    bash_command='beeline -u jdbc:hive2://10.128.0.14:10000 root kskks -f /root/airflow/dags/raw_ddl.hql',
    dag=dag
)

bucket_to_hdfs = BashOperator(
    task_id='bucket_to_hdfs',
    bash_command='sudo -u hdfs hdfs dfs -chmod 777 /warehouse/tablespace/external/hive/;sudo -u hdfs hdfs dfs -chown -R root /warehouse/tablespace/external/hive/;hdfs dfs -put -f /root/airflow/dags/OnlineRetail.csv /warehouse/tablespace/external/hive/raw.db/retail/',
    dag=dag
)

Repaire_Raw_Table = BashOperator(
    task_id='Repaire_Raw_Table',
    bash_command='beeline -u jdbc:hive2://10.128.0.14:10000 root kskks -f /root/airflow/dags/raw_msck.hql ',
    dag=dag
)

create_transform_ddl = BashOperator(
    task_id='create_transform_ddl',
    bash_command='beeline -u jdbc:hive2://10.128.0.14:10000 root kskks -f /root/airflow/dags/transform_ddl.hql ',
    dag=dag
)

load_transform = BashOperator(
    task_id='load_transform',
    bash_command='beeline -u jdbc:hive2://10.128.0.14:10000 root kskks -f /root/airflow/dags/load_transform.hql ',
    dag=dag
)

publish_ddl = BashOperator(
    task_id='publish_ddl',
    bash_command='beeline -u jdbc:hive2://10.128.0.14:10000 root kskks -f /root/airflow/dags/publish_ddl.hql ',
    dag=dag
)


load_publish = BashOperator(
    task_id='load_publish',
    bash_command='beeline -u jdbc:hive2://10.128.0.14:10000 root kskks -f /root/airflow/dags/load_publish.hql ',
    dag=dag
)


create_raw_ddl >> bucket_to_hdfs >> Repaire_Raw_Table >> create_transform_ddl >> load_transform >> publish_ddl >> load_publish

if __name__ == "__main__":
    dag.cli()
