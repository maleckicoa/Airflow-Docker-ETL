# script to fill up the vkb staging and testers table

#---Imports
from airflow import DAG
import psycopg2 #PostgreSQL database adapter for python
from airflow.hooks.postgres_hook import PostgresHook #Interaction with the Postgres connection
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from psycopg2.extras import execute_values
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import os

#---Define default arguments
default_args = {
    'owner': 'Team Analytics',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['toolsadmin@yas.life'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dev_db = "sputnikdb.ciosr15rstrz.eu-central-1.rds.amazonaws.com"
prod_db = "apollodb.ciosr15rstrz.eu-central-1.rds.amazonaws.com"

#---Get Environment
host = os.getenv("HOSTNAME")
env = "local" # if dev or prod are not found then default to local
if host.find("dev")>0 :
    db = dev_db
elif host.find("prod")>0 :
    db = prod_db
else :
    db = "localhost"

#---Set path to dags (this is needed for calling other scripts later)
dag_path = "/opt/airflow/dags/etl-processes/Analytics-Hub-ETL-Processes/Airflow_DAGs/"


#---Define the Dag and arguments
with DAG('clone_and_stage_app_DBs',
            max_active_runs=1,
            schedule_interval='@daily',
            catchup=False,
            default_args = default_args) as dag:

#---Transport Tasks
#---VKB
    vkb_backup = BashOperator(
        task_id="vkb_backing_up",
        bash_command="pg_dump -h vkb2-prod-db.ciosr15rstrz.eu-central-1.rds.amazonaws.com -p 5432 --no-owner --schema=public -U vkb2admin vkb2_prod_db > /usr/local/airflow/tmpdata/db_backup_VKBLiveDB.sql",
        dag=dag)

    vkb_clean_public_schema = PostgresOperator(
        task_id='vkb_cleaning_public_schema',
        sql="DROP SCHEMA if exists public cascade;",
        autocommit = True,
        dag=dag)

    vkb_clean_prod_schema = PostgresOperator(
        task_id='vkb_cleaning_prod_schema',
        sql="DROP SCHEMA if exists vkb_prod cascade;",
        autocommit = True,
        dag=dag)

    vkb_create_public_schema = PostgresOperator(
        task_id='vkb_creating_public_schema',
        sql="CREATE SCHEMA if not exists public;",
        autocommit = True,
        dag=dag)

    vkb_restore = BashOperator(
        task_id="vkb_restoring_backup",
        bash_command="psql -h " + db + " -d analytics -p 5432 -U awsanalyticsadmin -f /usr/local/airflow/tmpdata/db_backup_VKBLiveDB.sql",
        dag=dag)

    vkb_rename = PostgresOperator(
        task_id='vkb_renaming_public_schema',
        sql="ALTER SCHEMA public rename to vkb_prod;",
        autocommit = True,
        dag=dag)

    vkb_loglastuser = PostgresOperator(
        task_id='vkb_last_registered_user',
        sql="select max(u.registration_date) from vkb_prod.users u;",
        autocommit = True,
        dag=dag)

    vkb_staging = PostgresOperator(
        task_id='vkb_execute_staging_script',
        sql='scripts/vkb/vkb_staging_after_load.sql',
        autocommit = True
        )

    vkb_insert_testers = PostgresOperator(
        task_id='vkb_insert_tester_data',
        sql='scripts/vkb/vkb_insert_tester_data.sql',
        autocommit = True,
        dag=dag
        )

    vkb_remove_file = BashOperator(
        task_id="vkb_removing_temp_data",
        bash_command="rm /usr/local/airflow/tmpdata/db_backup_VKBLiveDB.sql",
        dag=dag)

#---UKV
    ukv_backup = BashOperator(
        task_id="ukv_backing_up",
        bash_command="pg_dump -h ukv2-prod-db.ciosr15rstrz.eu-central-1.rds.amazonaws.com -p 5432 --no-owner --schema=public -U ukv2admin ukv2_prod_db > /usr/local/airflow/tmpdata/db_backup_UKV_liveDB.sql",
        dag=dag)

    ukv_clean_public_schema = PostgresOperator(
        task_id='ukv_cleaning_public_schema',
        sql="DROP SCHEMA if exists public cascade;",
        autocommit = True,
        dag=dag)

    ukv_clean_prod_schema = PostgresOperator(
        task_id='ukv_cleaning_prod_schema',
        sql="DROP SCHEMA if exists ukv_prod cascade;",
        autocommit = True,
        dag=dag)

    ukv_create_public_schema = PostgresOperator(
        task_id='ukv_creating_public_schema',
        sql="CREATE SCHEMA if not exists public;",
        autocommit = True,
        dag=dag)

    ukv_restore = BashOperator(
        task_id="ukv_restoring_backup",
        bash_command="psql -h " + db + " -d analytics -p 5432 -U awsanalyticsadmin -f /usr/local/airflow/tmpdata/db_backup_UKV_liveDB.sql",
        dag=dag)

    ukv_rename = PostgresOperator(
        task_id='ukv_renaming_public_schema',
        sql="ALTER SCHEMA public rename to ukv_prod;",
        autocommit = True,
        dag=dag)

    ukv_loglastuser = PostgresOperator(
        task_id='ukv_last_registered_user',
        sql="select max(u.registration_date) from ukv_prod.users u;",
        autocommit = True,
        dag=dag)

    ukv_staging = PostgresOperator(
        task_id='ukv_execute_staging_script',
        sql='scripts/ukv/ukv_staging_after_load.sql',
        autocommit = True
        )

    ukv_insert_testers = PostgresOperator(
        task_id='ukv_insert_tester_data',
        sql='scripts/ukv/ukv_insert_tester_data.sql',
        autocommit = True,
        dag=dag
        )

    ukv_remove_file = BashOperator(
        task_id="ukv_removing_temp_data",
        bash_command="rm /usr/local/airflow/tmpdata/db_backup_UKV_liveDB.sql",
        dag=dag)

#---YAS
    yas_backup = BashOperator(
        task_id="yas_backing_up",
        bash_command="pg_dump -h yas-live-mig.ciosr15rstrz.eu-central-1.rds.amazonaws.com -p 5432 --no-owner --schema=public -U yasawsadmin YasLiveDB > /usr/local/airflow/tmpdata/db_backup_yas_liveDB.sql",
        dag=dag)

    yas_clean_public_schema = PostgresOperator(
        task_id='yas_cleaning_public_schema',
        sql="DROP SCHEMA if exists public cascade;",
        autocommit = True,
        dag=dag)

    yas_clean_prod_schema = PostgresOperator(
        task_id='yas_cleaning_prod_schema',
        sql="DROP SCHEMA if exists yas_prod cascade;",
        autocommit = True,
        dag=dag)

    yas_create_public_schema = PostgresOperator(
        task_id='yas_creating_public_schema',
        sql="CREATE SCHEMA if not exists public;",
        autocommit = True,
        dag=dag)

    yas_restore = BashOperator(
        task_id="yas_restoring_backup",
        bash_command="psql -h " + db + " -d analytics -p 5432 -U awsanalyticsadmin -f /usr/local/airflow/tmpdata/db_backup_yas_liveDB.sql",
        dag=dag)

    yas_rename = PostgresOperator(
        task_id='yas_renaming_public_schema',
        sql="ALTER SCHEMA public rename to yas_prod;",
        autocommit = True,
        dag=dag)

    yas_loglastuser = PostgresOperator(
        task_id='yas_last_registered_user',
        sql="select max(u.registration_date) from yas_prod.users u;",
        autocommit = True,
        dag=dag)

    yas_staging = PostgresOperator(
        task_id='yas_execute_staging_script',
        sql='scripts/yas/yas_staging_after_load.sql',
        autocommit = True
        )

    yas_insert_testers = PostgresOperator(
        task_id='yas_insert_tester_data',
        sql='scripts/yas/yas_insert_tester_data.sql',
        autocommit = True,
        dag=dag
        )

    yas_remove_file = BashOperator(
        task_id='yas_removing_temp_data',
        bash_command='rm /usr/local/airflow/tmpdata/db_backup_yas_liveDB.sql',
        dag=dag)

    # convert activity minutes to integer values
    yas_activity_int_conversion = BashOperator(
        task_id='yas_calculate_activity_minutes',
        bash_command='Rscript ' + dag_path + 'scripts/yas/Activities_to_int_minutes.R',
        dag=dag)

    # calculate the days between sessions
    yas_days_betw_sessions_calculation = BashOperator(
        task_id='yas_calculate_days_between_sessions',
        bash_command='Rscript ' + dag_path + 'scripts/yas/Days_between_sessions.R',
        dag=dag)

    # calculate the total points for each user in a separate table
    yas_totalpoints_calculation = PostgresOperator(
        task_id='yas_calculate_total_user_points',
        sql='scripts/yas/yas_total_user_points.sql',
        autocommit = True,
        dag=dag)

#---bmwbkk
    bmwbkk_backup = BashOperator(
        task_id="bmwbkk_backing_up",
        bash_command="pg_dump -h bmwbkk-prod-db.ciosr15rstrz.eu-central-1.rds.amazonaws.com -p 5432 --no-owner --schema=public -U bmwprodadmin bmwproddb > /usr/local/airflow/tmpdata/db_backup_bmwbkk_liveDB.sql",
        dag=dag)

    bmwbkk_clean_public_schema = PostgresOperator(
        task_id='bmwbkk_cleaning_public_schema',
        sql="DROP SCHEMA if exists public cascade;",
        autocommit = True,
        dag=dag)

    bmwbkk_clean_prod_schema = PostgresOperator(
        task_id='bmwbkk_cleaning_prod_schema',
        sql="DROP SCHEMA if exists bmwbkk_prod cascade;",
        autocommit = True,
        dag=dag)

    bmwbkk_create_public_schema = PostgresOperator(
        task_id='bmwbkk_creating_public_schema',
        sql="CREATE SCHEMA if not exists public;",
        autocommit = True,
        dag=dag)

    bmwbkk_restore = BashOperator(
        task_id="bmwbkk_restoring_backup",
        bash_command="psql -h " + db + " -d analytics -p 5432 -U awsanalyticsadmin -f /usr/local/airflow/tmpdata/db_backup_bmwbkk_liveDB.sql",
        dag=dag)

    bmwbkk_rename = PostgresOperator(
        task_id='bmwbkk_renaming_public_schema',
        sql="ALTER SCHEMA public rename to bmwbkk_prod;",
        autocommit = True,
        dag=dag)

    bmwbkk_loglastuser = PostgresOperator(
        task_id='bmwbkk_last_registered_user',
        sql="select max(u.registration_date) from bmwbkk_prod.users u;",
        autocommit = True,
        dag=dag)

    bmwbkk_staging = PostgresOperator(
        task_id='bmwbkk_execute_staging_script',
        sql='scripts/bmwbkk/bmwbkk_staging_after_load.sql',
        autocommit = True
        )

    bmwbkk_insert_testers = PostgresOperator(
        task_id='bmwbkk_insert_tester_data',
        sql='scripts/bmwbkk/bmwbkk_insert_tester_data.sql',
        autocommit = True,
        dag=dag
        )

    bmwbkk_remove_file = BashOperator(
        task_id="bmwbkk_removing_temp_data",
        bash_command="rm /usr/local/airflow/tmpdata/db_backup_bmwbkk_liveDB.sql",
        dag=dag)

#---mhplus
    mhplus_backup = BashOperator(
        task_id="mhplus_backing_up",
        bash_command="pg_dump -h mhplus-prod-db-mig-res.ciosr15rstrz.eu-central-1.rds.amazonaws.com -p 5432 --no-owner --schema=public -U mhplusadmin mhplusdb > /usr/local/airflow/tmpdata/db_backup_mhplus_liveDB.sql",
        dag=dag)

    mhplus_clean_public_schema = PostgresOperator(
        task_id='mhplus_cleaning_public_schema',
        sql="DROP SCHEMA if exists public cascade;",
        autocommit = True,
        dag=dag)

    mhplus_clean_prod_schema = PostgresOperator(
        task_id='mhplus_cleaning_prod_schema',
        sql="DROP SCHEMA if exists mhplus_prod cascade;",
        autocommit = True,
        dag=dag)

    mhplus_create_public_schema = PostgresOperator(
        task_id='mhplus_creating_public_schema',
        sql="CREATE SCHEMA if not exists public;",
        autocommit = True,
        dag=dag)

    mhplus_restore = BashOperator(
        task_id="mhplus_restoring_backup",
        bash_command="psql -h " + db + " -d analytics -p 5432 -U awsanalyticsadmin -f /usr/local/airflow/tmpdata/db_backup_mhplus_liveDB.sql",
        dag=dag)

    mhplus_rename = PostgresOperator(
        task_id='mhplus_renaming_public_schema',
        sql="ALTER SCHEMA public rename to mhplus_prod;",
        autocommit = True,
        dag=dag)

    mhplus_loglastuser = PostgresOperator(
        task_id='mhplus_last_registered_user',
        sql="select max(u.registration_date) from mhplus_prod.users u;",
        autocommit = True,
        dag=dag)

    mhplus_staging = PostgresOperator(
        task_id='mhplus_execute_staging_script',
        sql='scripts/mhplus/mhplus_staging_after_load.sql',
        autocommit = True
        )

    mhplus_insert_testers = PostgresOperator(
        task_id='mhplus_insert_tester_data',
        sql='scripts/mhplus/mhplus_insert_tester_data.sql',
        autocommit = True,
        dag=dag
        )

    mhplus_remove_file = BashOperator(
        task_id="mhplus_removing_temp_data",
        bash_command="rm /usr/local/airflow/tmpdata/db_backup_mhplus_liveDB.sql",
        dag=dag)

#---proactive (LV1871)
    proactive_backup = BashOperator(
        task_id="proactive_backing_up",
        bash_command="pg_dump -h proactive-db.ciosr15rstrz.eu-central-1.rds.amazonaws.com -p 5432 --no-owner --schema=public -U proactive proactivedb > /usr/local/airflow/tmpdata/db_backup_proactive_liveDB.sql",
        dag=dag)

    proactive_clean_public_schema = PostgresOperator(
        task_id='proactive_cleaning_public_schema',
        sql="DROP SCHEMA if exists public cascade;",
        autocommit = True,
        dag=dag)

    proactive_clean_prod_schema = PostgresOperator(
        task_id='proactive_cleaning_prod_schema',
        sql="DROP SCHEMA if exists proactive_prod cascade;",
        autocommit = True,
        dag=dag)

    proactive_create_public_schema = PostgresOperator(
        task_id='proactive_creating_public_schema',
        sql="CREATE SCHEMA if not exists public;",
        autocommit = True,
        dag=dag)

    proactive_restore = BashOperator(
        task_id="proactive_restoring_backup",
        bash_command="psql -h " + db + " -d analytics -p 5432 -U awsanalyticsadmin -f /usr/local/airflow/tmpdata/db_backup_proactive_liveDB.sql",
        dag=dag)

    proactive_rename = PostgresOperator(
        task_id='proactive_renaming_public_schema',
        sql="ALTER SCHEMA public rename to proactive_prod;",
        autocommit = True,
        dag=dag)

    proactive_loglastuser = PostgresOperator(
        task_id='proactive_last_registered_user',
        sql="select max(u.registration_date) from proactive_prod.users u;",
        autocommit = True,
        dag=dag)

    proactive_staging = PostgresOperator(
        task_id='proactive_execute_staging_script',
        sql='scripts/proactive/proactive_staging_after_load.sql',
        autocommit = True
        )

    proactive_insert_testers = PostgresOperator(
        task_id='proactive_insert_tester_data',
        sql='scripts/proactive/proactive_insert_tester_data.sql',
        autocommit = True,
        dag=dag
        )

    proactive_remove_file = BashOperator(
        task_id="proactive_removing_temp_data",
        bash_command="rm /usr/local/airflow/tmpdata/db_backup_proactive_liveDB.sql",
        dag=dag)

#---rwebkk
    rwebkk_backup = BashOperator(
        task_id="rwebkk_backing_up",
        bash_command="pg_dump -h rwe-prod.ciosr15rstrz.eu-central-1.rds.amazonaws.com -p 5432 --no-owner --schema=public -U rweprodadmin rweproddb > /usr/local/airflow/tmpdata/db_backup_rwebkk_liveDB.sql",
        dag=dag)

    rwebkk_clean_public_schema = PostgresOperator(
        task_id='rwebkk_cleaning_public_schema',
        sql="DROP SCHEMA if exists public cascade;",
        autocommit = True,
        dag=dag)

    rwebkk_clean_prod_schema = PostgresOperator(
        task_id='rwebkk_cleaning_prod_schema',
        sql="DROP SCHEMA if exists rwebkk_prod cascade;",
        autocommit = True,
        dag=dag)

    rwebkk_create_public_schema = PostgresOperator(
        task_id='rwebkk_creating_public_schema',
        sql="CREATE SCHEMA if not exists public;",
        autocommit = True,
        dag=dag)

    rwebkk_restore = BashOperator(
        task_id="rwebkk_restoring_backup",
        bash_command="psql -h " + db + " -d analytics -p 5432 -U awsanalyticsadmin -f /usr/local/airflow/tmpdata/db_backup_rwebkk_liveDB.sql",
        dag=dag)

    rwebkk_rename = PostgresOperator(
        task_id='rwebkk_renaming_public_schema',
        sql="ALTER SCHEMA public rename to rwebkk_prod;",
        autocommit = True,
        dag=dag)

    rwebkk_loglastuser = PostgresOperator(
        task_id='rwebkk_last_registered_user',
        sql="select max(u.registration_date) from rwebkk_prod.users u;",
        autocommit = True,
        dag=dag)

    rwebkk_staging = PostgresOperator(
        task_id='rwebkk_execute_staging_script',
        sql='scripts/rwebkk/rwebkk_staging_after_load.sql',
        autocommit = True
        )

    rwebkk_insert_testers = PostgresOperator(
        task_id='rwebkk_insert_tester_data',
        sql='scripts/rwebkk/rwebkk_insert_tester_data.sql',
        autocommit = True,
        dag=dag
        )

    rwebkk_remove_file = BashOperator(
        task_id="rwebkk_removing_temp_data",
        bash_command="rm /usr/local/airflow/tmpdata/db_backup_rwebkk_liveDB.sql",
        dag=dag)

#---big
    big_backup = BashOperator(
        task_id="big_backing_up",
        bash_command="pg_dump -h bigproddb.ciosr15rstrz.eu-central-1.rds.amazonaws.com -p 5432 --no-owner --schema=public -U bigprodadmin bigproddb > /usr/local/airflow/tmpdata/db_backup_big_liveDB.sql",
        dag=dag)

    big_clean_public_schema = PostgresOperator(
        task_id='big_cleaning_public_schema',
        sql="DROP SCHEMA if exists public cascade;",
        autocommit = True,
        dag=dag)

    big_clean_prod_schema = PostgresOperator(
        task_id='big_cleaning_prod_schema',
        sql="DROP SCHEMA if exists big_prod cascade;",
        autocommit = True,
        dag=dag)

    big_create_public_schema = PostgresOperator(
        task_id='big_creating_public_schema',
        sql="CREATE SCHEMA if not exists public;",
        autocommit = True,
        dag=dag)

    big_restore = BashOperator(
        task_id="big_restoring_backup",
        bash_command="psql -h " + db + " -d analytics -p 5432 -U awsanalyticsadmin -f /usr/local/airflow/tmpdata/db_backup_big_liveDB.sql",
        dag=dag)

    big_rename = PostgresOperator(
        task_id='big_renaming_public_schema',
        sql="ALTER SCHEMA public rename to big_prod;",
        autocommit = True,
        dag=dag)

    big_loglastuser = PostgresOperator(
        task_id='big_last_registered_user',
        sql="select max(u.registration_date) from big_prod.users u;",
        autocommit = True,
        dag=dag)

    big_staging = PostgresOperator(
        task_id='big_execute_staging_script',
        sql='scripts/big/big_staging_after_load.sql',
        autocommit = True
        )

    big_insert_testers = PostgresOperator(
        task_id='big_insert_tester_data',
        sql='scripts/big/big_insert_tester_data.sql',
        autocommit = True,
        dag=dag
        )

    big_remove_file = BashOperator(
        task_id="big_removing_temp_data",
        bash_command="rm /usr/local/airflow/tmpdata/db_backup_big_liveDB.sql",
        dag=dag)

#---Transport Tasks End

    transportcomplete = DummyOperator(task_id="transport_complete",
                                      dag = dag,
                                      trigger_rule = 'all_success')

#---Create Pseudonyms
    pseudonym_creation = PostgresOperator(
        task_id='calculate_pseudonyms',
        sql='scripts/create_pseudonym_mapping.sql',
        trigger_rule='all_success',
        dag=dag)

#---Permissions on Prod Schemas
    # this should be executed after all the _prod-schemas are present
    permissions_refresh = PostgresOperator(
        task_id='refresh_permissions_prod_schemas',
        sql='scripts/refresh_permissions.sql',
        dag=dag)

#---Reporting Tasks

    reporting_daily_metrics = BashOperator(
        task_id='calculate_daily_metrics',
        bash_command='Rscript "' + dag_path + 'scripts/reporting/Analytics Reporting ETL - Daily Metrics.R"',
        dag=dag)

    reporting_steps_metrics = BashOperator(
        task_id='calculate_steps_metrics',
        bash_command='Rscript "' + dag_path + 'scripts/reporting/Analytics Reporting ETL - User Steps Metrics.R"',
        dag=dag)

    reporting_user_metrics = BashOperator(
        task_id='calculate_user_metrics',
        bash_command='Rscript "' + dag_path + 'scripts/reporting/Analytics Reporting ETL - User Metrics.R"',
        dag=dag)

    reporting_weekly_metrics = BashOperator(
        task_id='calculate_weekly_metrics',
        bash_command='Rscript "' + dag_path + 'scripts/reporting/Analytics Reporting ETL - Weekly Metrics.R"',
        dag=dag)

    reporting_monthly_metrics = BashOperator(
        task_id='calculate_monthly_metrics',
        bash_command='Rscript "' + dag_path + 'scripts/reporting/Analytics Reporting ETL - Monthly Metrics.R"',
        dag=dag)

    reporting_challenges = BashOperator(
        task_id='calculate_challenges',
        bash_command='Rscript "' + dag_path + 'scripts/reporting/Analytics Reporting ETL - Challenges.R"',
        dag=dag)

    reporting_challenge_participants = BashOperator(
        task_id='calculate_challenge_participants',
        bash_command='Rscript "' + dag_path + 'scripts/reporting/Analytics Reporting ETL - Challenge Participants.R"',
        dag=dag)

    reporting_redemptions = BashOperator(
        task_id='calculate_redemptions',
        bash_command='Rscript "' + dag_path + 'scripts/reporting/Analytics Reporting ETL - Redemptions.R"',
        dag=dag)

    reporting_user_groups = BashOperator(
        task_id='calculate_user_groups',
        bash_command='Rscript "' + dag_path + 'scripts/reporting/Analytics Reporting ETL - User Groups.R"',
        dag=dag)

    reportingcomplete = DummyOperator(task_id="reporting_complete",
                                      dag = dag)

#---Testing Consistency

    testing_consistency = BashOperator(
        task_id='testing_consistency',
        bash_command='Rscript "' + dag_path + 'scripts/testing_consistency/run_tests.R"',
        dag=dag)

    all_done = DummyOperator(task_id="all_done",
                             dag = dag)

#---Control Flow

# VKB
    vkb_backup >> vkb_clean_public_schema
    vkb_clean_public_schema >> vkb_clean_prod_schema
    vkb_clean_prod_schema >> vkb_create_public_schema
    vkb_create_public_schema >> vkb_restore
    vkb_restore >> vkb_rename
    vkb_rename >> vkb_loglastuser
    vkb_loglastuser >> vkb_remove_file
    vkb_remove_file >> permissions_refresh
    permissions_refresh >> vkb_staging
    vkb_staging >> vkb_insert_testers
    vkb_insert_testers >> transportcomplete

# UKV
    ukv_backup >> ukv_clean_public_schema
    ukv_clean_public_schema >> ukv_clean_prod_schema
    ukv_clean_prod_schema >> ukv_create_public_schema
    ukv_create_public_schema >> ukv_restore
    ukv_restore >> ukv_rename
    ukv_rename >> ukv_loglastuser
    ukv_loglastuser >> ukv_remove_file
    ukv_remove_file >> permissions_refresh
    permissions_refresh >> ukv_staging
    ukv_staging >> ukv_insert_testers
    ukv_insert_testers >> transportcomplete

# yas
    yas_backup >> yas_clean_public_schema
    yas_clean_public_schema >> yas_clean_prod_schema
    yas_clean_prod_schema >> yas_create_public_schema
    yas_create_public_schema >> yas_restore
    yas_restore >> yas_rename
    yas_rename >> yas_loglastuser
    yas_loglastuser >> yas_remove_file
    yas_remove_file >> permissions_refresh
    permissions_refresh >> yas_staging
    yas_staging >> yas_insert_testers
    yas_insert_testers >> yas_activity_int_conversion
    yas_activity_int_conversion >> yas_days_betw_sessions_calculation
    yas_days_betw_sessions_calculation >> yas_totalpoints_calculation
    yas_totalpoints_calculation >> transportcomplete

# bmwbkk
    bmwbkk_backup >> bmwbkk_clean_public_schema
    bmwbkk_clean_public_schema >> bmwbkk_clean_prod_schema
    bmwbkk_clean_prod_schema >> bmwbkk_create_public_schema
    bmwbkk_create_public_schema >> bmwbkk_restore
    bmwbkk_restore >> bmwbkk_rename
    bmwbkk_rename >> bmwbkk_loglastuser
    bmwbkk_loglastuser >> bmwbkk_remove_file
    bmwbkk_remove_file >> permissions_refresh
    permissions_refresh >> bmwbkk_staging
    bmwbkk_staging >> bmwbkk_insert_testers
    bmwbkk_insert_testers >> transportcomplete # end of the app-specific tasks


# mhplus
    mhplus_backup >> mhplus_clean_public_schema
    mhplus_clean_public_schema >> mhplus_clean_prod_schema
    mhplus_clean_prod_schema >> mhplus_create_public_schema
    mhplus_create_public_schema >> mhplus_restore
    mhplus_restore >> mhplus_rename
    mhplus_rename >> mhplus_loglastuser
    mhplus_loglastuser >> mhplus_remove_file
    mhplus_remove_file >> permissions_refresh
    permissions_refresh >> mhplus_staging
    mhplus_staging >> mhplus_insert_testers
    mhplus_insert_testers >> transportcomplete # end of the app-specific tasks


# proactive
    proactive_backup >> proactive_clean_public_schema
    proactive_clean_public_schema >> proactive_clean_prod_schema
    proactive_clean_prod_schema >> proactive_create_public_schema
    proactive_create_public_schema >> proactive_restore
    proactive_restore >> proactive_rename
    proactive_rename >> proactive_loglastuser
    proactive_loglastuser >> proactive_remove_file
    proactive_remove_file >> permissions_refresh
    permissions_refresh >> proactive_staging
    proactive_staging >> proactive_insert_testers
    proactive_insert_testers >> transportcomplete # end of the app-specific tasks


# rwebkk
    rwebkk_backup >> rwebkk_clean_public_schema
    rwebkk_clean_public_schema >> rwebkk_clean_prod_schema
    rwebkk_clean_prod_schema >> rwebkk_create_public_schema
    rwebkk_create_public_schema >> rwebkk_restore
    rwebkk_restore >> rwebkk_rename
    rwebkk_rename >> rwebkk_loglastuser
    rwebkk_loglastuser >> rwebkk_remove_file
    rwebkk_remove_file >> permissions_refresh
    permissions_refresh >> rwebkk_staging
    rwebkk_staging >> rwebkk_insert_testers
    rwebkk_insert_testers >> transportcomplete # end of the app-specific tasks

# big
    big_backup >> big_clean_public_schema
    big_clean_public_schema >> big_clean_prod_schema
    big_clean_prod_schema >> big_create_public_schema
    big_create_public_schema >> big_restore
    big_restore >> big_rename
    big_rename >> big_loglastuser
    big_loglastuser >> big_remove_file
    big_remove_file >> permissions_refresh
    permissions_refresh >> big_staging
    big_staging >> big_insert_testers
    big_insert_testers >> transportcomplete # end of the app-specific tasks

#---preventing parallel writing to schema
ukv_backup << vkb_remove_file # ukv tasks cannot start before vkb_loglastuser is done and schema public is renamed
yas_backup << ukv_remove_file
bmwbkk_backup << yas_remove_file
mhplus_backup << bmwbkk_remove_file
proactive_backup << mhplus_remove_file
rwebkk_backup << proactive_remove_file
big_backup << rwebkk_remove_file

# reporting
transportcomplete >> pseudonym_creation
pseudonym_creation >> reporting_daily_metrics
reporting_daily_metrics >> reporting_steps_metrics
reporting_steps_metrics >> reporting_user_metrics
reporting_user_metrics >> reporting_weekly_metrics
reporting_weekly_metrics >> reporting_monthly_metrics
reporting_monthly_metrics >> reporting_challenges
reporting_challenges >> reporting_challenge_participants
reporting_challenge_participants >> reporting_redemptions
reporting_redemptions >> reporting_user_groups
reporting_user_groups >> reportingcomplete

# testing
reportingcomplete >> testing_consistency
testing_consistency >> all_done
