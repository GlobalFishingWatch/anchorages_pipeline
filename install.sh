#!/bin/bash

echo "Installing dags"
cp ./airflow/port_events_dag.py /dags/port_events_dag.py
cp ./airflow/port_visits_dag.py /dags/port_visits_dag.py
echo "Installing post_install.sh"
cp ./airflow/post_install.sh /dags/post_install.sh
