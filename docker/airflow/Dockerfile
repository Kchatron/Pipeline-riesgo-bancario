# Variante Linux amd64 comprobada del tag oficial 3.3.2-python3.14.
FROM apache/airflow:3.3.2-python3.14@sha256:361bf4e851dffa0bbf7dc5dbe6a33da8718a4cf987326a8380ba435106dbb922

USER root
RUN python -m venv /opt/pipeline-venv
COPY docker/airflow/pipeline.lock /opt/pipeline.lock
RUN /opt/pipeline-venv/bin/python -m pip install --no-cache-dir \
      --require-hashes --only-binary=:all: -r /opt/pipeline.lock \
    && /opt/pipeline-venv/bin/python -m pip check

WORKDIR /opt/project
# Airflow importa el operador desde el código incorporado, sin activar el venv.
ENV PYTHONPATH=/opt/project \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
    PYTHONDONTWRITEBYTECODE=1
COPY src/ src/
COPY config/referencias_historico.json config/referencias_historico.json
COPY data/ data/
COPY tests/ tests/
COPY tests_airflow/ tests_airflow/
COPY dags/historico_mensual.py /opt/airflow/dags/historico_mensual.py
COPY requirements.txt pyproject.toml ./
COPY docker/airflow/validate.py /opt/project/docker/airflow/validate.py
COPY docker/airflow/pipeline-python /opt/pipeline-python
# Inmutabilidad respecto al usuario de ejecución (no una garantía frente a root).
RUN mkdir /opt/project/data/raw/SBS \
    && chmod 0555 /opt/pipeline-python \
    && chown root:root /opt/airflow/dags \
    && chmod 0555 /opt/airflow/dags \
    && chmod 0444 /opt/airflow/dags/historico_mensual.py \
    && find /opt/project /opt/pipeline-venv -type d -exec chmod 0555 {} + \
    && find /opt/project -type f -exec chmod 0444 {} +

USER airflow
RUN python -m pip check
# El golden master corre con el intérprete real del pipeline durante la construcción.
RUN /opt/pipeline-python docker/airflow/validate.py --store /tmp/build-golden-store
# Pruebas del SDK en el Python de Airflow, con CLI simulada y sin DagRun.
RUN python -m unittest discover -s tests_airflow -v
# No se altera ENTRYPOINT ni PATH de la imagen oficial de Airflow.
