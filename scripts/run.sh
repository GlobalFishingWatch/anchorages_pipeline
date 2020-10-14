#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

display_usage() {
  echo "Available Commands"
  echo "  port_events             Run port events dataflow"
  echo "  port_visits             Run port visits dataflow"
  echo "  anchorages              Run anchorages dataflow"
  echo "  name_anchorages         Run name anchorages dataflow"
  echo "  frozen_dependencies     Get the frozen pip dependencies"
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

  frozen_dependencies)
    cat ./frozen_dependencies.txt
    ;;

  *)
    display_usage
    exit 0
    ;;
esac
