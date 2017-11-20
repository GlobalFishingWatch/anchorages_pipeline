
import logging
import sys
import os
import subprocess


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info('starting port_events')
    try:
        from anchorages import port_events
        args = []
        # Special case fast_test
        args = [('--fast-test' if (x == 'fast_test=true') else x) for x in args]
        #
        # Turn dataflow style [`--x=y`] args into python style [`--x`, 'y'] args
        for x in sys.argv[1:]:
            args.extend(x.split('=', 1))
        # Change DataflowPipelineRunner to DataflowRunner
        args = [('DataflowRunner' if (x == 'DataflowPipelineRunner') else x) for x in args]
        this_dir = os.path.abspath(os.path.dirname(__file__))
        result = subprocess.call(["docker-compose", "run",  "port_events"] + args, cwd=this_dir)
    except StandardError, err:
        logging.exception('Exception in run()')
        raise
    sys.exit(result)