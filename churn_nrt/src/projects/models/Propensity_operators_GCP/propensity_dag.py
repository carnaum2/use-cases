from airflow.contrib.operators import dataproc_operator
from airflow.operators import (BashOperator, DummyOperator)
from airflow.utils.trigger_rule import TriggerRule
from typing import Type
from airflow import DAG
import calendar
import datetime
import string
import random




PROJECT = "vf-es-ca-nonlive"
BUCKET = "vf-es-ca-nonlive-dev"
userIAM = "vf-es-ca-dev-dp-ds-sa"


def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(stringLength))


def get_dag_configs(project, bucket):

    composer_params: dict = {
        "project_id": project,
        "sa_run_dp_job": "{}@{}.iam.gserviceaccount.com".format(userIAM,PROJECT),
        "job_name": "propensity_model",
        "job_desc": "Train the propensity model for each operator and make the predictions",
        "region": "europe-west1",
        #"max_idle": "15m",
        "zone": "europe-west1-b",
        "subnet_url": "projects/{}/regions/europe-west1/subnetworks/dev-restricted-zone".format(project),
        "dp_machine_type": "n1-highmem-16",
        "dp_tags": "allow-internal-dataproc-dev,allow-ssh-from-management-zone,allow-ssh-from-net-to-bastion",
        "dp_properties": "core:fs.gs.implicit.dir.repair.enable=false,core:fs.gs.status.parallel.enable=true",
        "dp_num_workers": "8",
        "schedule": "@once",
        "optional_components": "ANACONDA,JUPYTER",
        "metadata": "\'PIP_PACKAGES=numpy pandas python-dateutil\'",
        "initialization-actions": "gs://goog-dataproc-initialization-actions-europe-west1/python/pip-install.sh",
    }

    base_dag_config: dict = {
        "model": "gs://{}/models/propensity_operators/model.py".format(bucket),
        "predictions": "gs://{}/models/propensity_operators/predictions.py".format(bucket),
    }

    operator_config_list: list = [
        {"name": "masmovil"},
        {"name": "movistar"},
        {"name": "orange"},
        {"name": "others"}
    ]

    return composer_params, base_dag_config, operator_config_list





# Arguments

train_date = "20190914"
test_date = "20190914"
mode = "production"


composer_params, base_dag_config, operator_config_list = get_dag_configs(PROJECT, BUCKET)

dag = DAG(
    composer_params["job_name"],
    description=composer_params["job_desc"],
    start_date=datetime.datetime(2000, 1, 1),
    schedule_interval=composer_params["schedule"],
    catchup=False
)



operator_job_dict: dict = {}

for operator_dict in operator_config_list:
    operator: str = operator_dict["name"]
    cluster_name: str = "model-training-{}".format(operator).replace("_", "-")
    random_str = randomString()


    cluster_create_str = """gcloud beta dataproc clusters create {} --region {} --zone {} --subnet {} --tags {} --project {} \
        --service-account {} --master-machine-type {} --num-workers {} --worker-machine-type {} --properties {} \
        --optional-components={} --metadata {} --initialization-actions {} --enable-component-gateway --no-address""".format(
        cluster_name, composer_params["region"], composer_params["zone"], composer_params["subnet_url"],
        composer_params["dp_tags"], composer_params["project_id"], composer_params["sa_run_dp_job"],
        composer_params["dp_machine_type"], operator_dict.get("dp_workers ", composer_params["dp_num_workers"]),
        composer_params["dp_machine_type"], composer_params["dp_properties"], composer_params["optional_components"],
        composer_params["metadata"], composer_params["initialization-actions"]
    )



    create_dataproc = BashOperator(
        task_id=f"create_dataproc_{operator_dict['name']}",
        bash_command=cluster_create_str,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag
    )


    run_job_str = """ gcloud dataproc jobs submit pyspark {} \
                 --id={} \
                 --cluster={} \
                 --bucket={} \
                 --region={} \
                 -- -train_date {}  -test_date {} -mode {} -operator {}
                """.format(base_dag_config["model"],
                           "job_{}_{}".format(operator, random_str),
                           cluster_name,
                           BUCKET,
                           composer_params["region"],
                           train_date,
                           test_date,
                           mode,
                           operator)
    run_job = BashOperator(
        task_id="run_job_{}".format(operator),
        bash_command=run_job_str,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag
    )

    delete_dataproc = dataproc_operator.DataprocClusterDeleteOperator(
        task_id=f"delete-cluster-{cluster_name}",
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_DONE,
        project_id=composer_params["project_id"],
        region=composer_params["region"],
        dag=dag,
    )

    delete_dataproc.set_upstream(run_job)
    run_job.set_upstream(create_dataproc)


    operator_job_dict[operator_dict["name"]] = run_job

# Prediction job:
# Prediction Parameters:
cluster_name: str = "predictions-propensity"
module_arguments: list = [
    "-test_date {}".format(test_date),
    "-mode {}".format(mode)
]

cluster_create_str = """gcloud beta dataproc clusters create {} --region {} --zone {} --subnet {} --tags {} --project {} \
    --service-account {} --master-machine-type {} --num-workers {} --worker-machine-type {} --properties {} \
    --optional-components={} --metadata {} --initialization-actions {} --enable-component-gateway --no-address""".format(
    cluster_name, composer_params["region"], composer_params["zone"], composer_params["subnet_url"],
    composer_params["dp_tags"], composer_params["project_id"], composer_params["sa_run_dp_job"],
    composer_params["dp_machine_type"], operator_dict.get("dp_workers ", composer_params["dp_num_workers"]),
    composer_params["dp_machine_type"], composer_params["dp_properties"], composer_params["optional_components"],
    composer_params["metadata"], composer_params["initialization-actions"]
)

create_prediction_dataproc = BashOperator(
    task_id=f"create_dataproc_predictions_propensity",
    bash_command=cluster_create_str,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

run_prediction_job_str = """ gcloud dataproc jobs submit pyspark {} \
                 --cluster={} \
                 --bucket={} \
                 --region={} \
                 -- -test_date {} -mode {}""".format(base_dag_config["predictions"],
                           cluster_name,
                           BUCKET,
                           composer_params["region"],
                           test_date,
                           mode
                           )

run_prediction_job = BashOperator(
    task_id="run_prediction_job",
    bash_command=run_prediction_job_str,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)


delete_prediction_dataproc = dataproc_operator.DataprocClusterDeleteOperator(
    task_id=f"delete-cluster-{cluster_name}",
    cluster_name=cluster_name,
    trigger_rule=TriggerRule.ALL_DONE,
    project_id=composer_params["project_id"],
    region=composer_params["region"],
    dag=dag,
)

delete_prediction_dataproc.set_upstream(run_prediction_job)
run_prediction_job.set_upstream(create_prediction_dataproc)



for operator_key in operator_job_dict.keys():
    create_prediction_dataproc.set_upstream(operator_job_dict[operator_key])
