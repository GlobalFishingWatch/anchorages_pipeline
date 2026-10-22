import logging

# Silence Beam's noisy Prism process shutdown warning.
logging.getLogger("apache_beam.utils.subprocess_server").setLevel(logging.ERROR)
