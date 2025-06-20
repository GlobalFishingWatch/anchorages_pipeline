import logging

# monkey patch to suppress the annoying warning you get when you import apache_beam
#
# No handlers could be found for logger "oauth2client.contrib.multistore_file"
#
# This warning is harmless, but annooying when you are using beam from a command line app
# see: https://issues.apache.org/jira/browse/BEAM-1183


# This just creates a null handler for that logger so there is no output
logger = logging.getLogger("oauth2client.contrib.multistore_file")
handler = logging.NullHandler()
logger.addHandler(handler)
