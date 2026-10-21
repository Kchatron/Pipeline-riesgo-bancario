"""Reproducción histórica manual: un mes independiente desde initial por DagRun."""

from datetime import datetime, timezone

from airflow.sdk import DAG, Param

from src.orquestacion.operador_historico import HistoricoOperator, PATRON_PERIODO


with DAG(
    dag_id="historico_mensual",
    schedule=None,
    start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "periodo": Param(
            type="string", pattern="^" + PATRON_PERIODO + "$", minLength=7, maxLength=7,
            description="Mes obligatorio entre 2023-01 y 2025-12 (YYYY-MM).",
        ),
    },
) as dag:
    ingest = HistoricoOperator(task_id="ingest", operacion="ingest", retries=0,
                               do_xcom_push=True, multiple_outputs=False)
    verify_ingest = HistoricoOperator(task_id="verify_ingest", operacion="verify",
                                     origen_task_id="ingest", retries=0,
                                     do_xcom_push=True, multiple_outputs=False)
    rebuild = HistoricoOperator(task_id="rebuild", operacion="rebuild",
                               origen_task_id="verify_ingest", retries=0,
                               do_xcom_push=True, multiple_outputs=False)
    verify_rebuild = HistoricoOperator(task_id="verify_rebuild", operacion="verify",
                                      origen_task_id="rebuild", retries=0,
                                      do_xcom_push=True, multiple_outputs=False)
    publish = HistoricoOperator(task_id="publish", operacion="publish",
                               origen_task_id="verify_rebuild", retries=0,
                               do_xcom_push=True, multiple_outputs=False)
    verify_version = HistoricoOperator(task_id="verify_version", operacion="verify",
                                      origen_task_id="publish", retries=0,
                                      do_xcom_push=True, multiple_outputs=False)

    ingest >> verify_ingest >> rebuild >> verify_rebuild >> publish >> verify_version
