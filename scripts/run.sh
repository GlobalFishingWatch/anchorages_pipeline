#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

display_usage() {
  echo "Available Commands"
  echo "  port_events             run port events dataflow"
  echo "  port_visits             run port visits dataflow"
  echo "  anchorages              run anchorages dataflow"
  echo "  name_anchorages         run name anchorages dataflow"
  echo "  generate_voyages        generate voyages"
  echo "  deletes_raw_port_visits deletes the raw port visits table"
}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi


case $1 in

  port_events)
    python -m pipe_anchorages.port_events "${@:2}"
    ;;

  port_visits)
    python -m pipe_anchorages.port_visits "${@:2}"
    ;;

  anchorages)
    python -m pipe_anchorages.anchorages "${@:2}"
    ;;

  name_anchorages)
    python -m pipe_anchorages.name_anchorages "${@:2}"
    ;;

  generate_voyages)
    ${THIS_SCRIPT_DIR}/generate_voyages.sh "${@:2}"
    ;;

  deletes_raw_port_visits)
    python -m pipe_anchorages.deletes_raw_port_visits "${@:2}"
    ;;

  *)
    display_usage
    exit 0
    ;;
esac
