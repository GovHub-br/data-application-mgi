ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION} AS airflow-base

USER root

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    PIP_DEFAULT_TIMEOUT=300

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    libgdal-dev \
    libgtk2.0-dev \
    libpq-dev \
    unzip \
    unixodbc-dev \
    vim \
  && sed -i "s,^\(MinProtocol[ ]*=\).*,\1TLSv1.0,g" /etc/ssl/openssl.cnf \
  && sed -i "s,^\(CipherString[ ]*=\).*,\1DEFAULT@SECLEVEL=1,g" /etc/ssl/openssl.cnf \
  && curl -fsSLo /usr/local/share/ca-certificates/ACcompactado.zip http://acraiz.icpbrasil.gov.br/credenciadas/CertificadosAC-ICP-Brasil/ACcompactado.zip \
  && unzip -o /usr/local/share/ca-certificates/ACcompactado.zip -d /usr/local/share/ca-certificates/ \
  && rm -f /usr/local/share/ca-certificates/ACcompactado.zip \
  && update-ca-certificates \
  && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
  && sed -i 's/^# pt_BR.UTF-8 UTF-8$/pt_BR.UTF-8 UTF-8/g' /etc/locale.gen \
  && locale-gen en_US.UTF-8 pt_BR.UTF-8 \
  && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /var/cache/apt/* \
    /usr/share/doc \
    /usr/share/doc-base \
    /usr/share/man

USER airflow
WORKDIR ${AIRFLOW_HOME}


FROM airflow-base AS airflow-prod

COPY requirements.txt ./requirements.txt

RUN python -m pip install --no-cache-dir -r requirements.txt \
  && rm -f requirements.txt

ENV ENVIRONMENT=prod


FROM airflow-base AS airflow-dev

COPY requirements.txt ./requirements.txt

RUN python -m pip install --no-cache-dir -r requirements.txt \
  && rm -f requirements.txt

ENV ENVIRONMENT=dev
