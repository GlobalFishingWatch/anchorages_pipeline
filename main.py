#! /usr/bin/env python
import sys
import logging

logging.basicConfig(level=logging.INFO)

def run_script(bash_script_command):
    import subprocess
    resp = subprocess.run(bash_script_command, check=True, stdout=sys.stdout, stderr=sys.stderr, universal_newlines=True)
    print(resp.stdout, resp.stderr)
    if (resp.returncode != 0):
        raise Exception(f'The script returns non zero result <{bash_script_command}> is {resp}')

def run_generate_confidence_voyages(args):
    run_script(['./scripts/generate_confidence_voyages.sh']+args)

def run_generate_voyages(args):
    run_script(['./scripts/generate_voyages.sh']+args)

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


SUBCOMMANDS = {
    "thin_port_messages": run_thin_port_messages,
    "port_visits": run_port_visits,
    "anchorages": run_anchorages,
    "name_anchorages": run_name_anchorages,
    "generate_confidence_voyages": run_generate_confidence_voyages,
    "generate_voyages": run_generate_voyages
}

if __name__ == "__main__":
    logging.info("Running %s", sys.argv)

    if len(sys.argv) < 2:
        logging.info("No subcommand specified. Run pipeline [SUBCOMMAND], where subcommand is one of %s", SUBCOMMANDS.keys())
        exit(1)

    subcommand = sys.argv[1]
    subcommand_args = sys.argv[2:]

    SUBCOMMANDS[subcommand](subcommand_args)
