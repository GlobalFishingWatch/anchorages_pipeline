#! /usr/bin/env python
import sys
import logging

logging.basicConfig(level=logging.INFO)

def run_generate_confidence_voyages(args):
    from pipe_anchorages.confidence_voyages import run as run_confidence_voyages
    run_confidence_voyages(args)

def run_thin_port_messages(args):
    from pipe_anchorages.thin_port_messages import run as run_thin_port_messages
    run_thin_port_messages(args)

def run_port_visits(args):
    from pipe_anchorages.port_visits import run as run_port_visits
    run_port_visits(args)

def run_anchorages(args):
    from pipe_anchorages.anchorages import run as run_anchorages
    run_anchorages(args)

def run_name_anchorages(args):
    from pipe_anchorages.name_anchorages import run as run_name_anchorages
    run_name_anchorages(args)

def run_voyages(args):
    from pipe_anchorages.voyages.__main__ import run as run_voyages
    run_voyages(args)


SUBCOMMANDS = {
    "thin_port_messages": run_thin_port_messages,
    "port_visits": run_port_visits,
    "anchorages": run_anchorages,
    "name_anchorages": run_name_anchorages,
    "generate_confidence_voyages": run_generate_confidence_voyages,
    "voyages": run_voyages,
}

if __name__ == "__main__":
    logging.info("Running %s", sys.argv)

    if len(sys.argv) < 2:
        logging.info("No subcommand specified. Run pipeline [SUBCOMMAND], where subcommand is one of %s", SUBCOMMANDS.keys())
        exit(1)

    subcommand = sys.argv[1]
    subcommand_args = sys.argv[2:]

    SUBCOMMANDS[subcommand](subcommand_args)
