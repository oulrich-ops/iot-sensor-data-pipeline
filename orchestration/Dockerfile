FROM apache/airflow:3.0.0

USER root

# Copie les répertoires locaux dans l'image
COPY ./airflow /opt/airflow
COPY ./extractions-loading /opt/airflow/extractions-loading
COPY ./ftp_storage /opt/airflow/ftp_storage
COPY ./ftp_archive /opt/airflow/ftp_archive
COPY ./ftp-server /ftp-server

# Fixe les permissions pour l'utilisateur airflow (uid 50000)
RUN chown -R 50000:0 /opt/airflow 

# Revenir à l'utilisateur airflow
USER 50000:0

# Installer les dépendances Python avant de démarrer Airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt