#!/opt/csw/bin/python3

import os
import subprocess
import sys
from FileHandler import FileHandler
from lib import my_env

# Initialize Environment
projectname = "vea_od"
modulename = my_env.get_modulename(__file__)
config = my_env.get_inifile(projectname, __file__)
my_log = my_env.init_loghandler(config, modulename)
my_log.info('Start Application')
scriptdir = sys.executable
# Get FileHandler Object
fh = FileHandler(config)
# Check for proxyserver
# Proxy can be enabled now.
try:
    http_proxy = config['Main']['proxy']
except KeyError:  # http_proxy not defined, continue
    pass
else:
    os.environ['http_proxy'] = http_proxy
    my_log.info("Set proxy to %s", http_proxy)
fh.process_input_directory()
# Load dcat_ap profile for Open Data if flag is set to create dcat_ap
scandir = config['Main']['scandir']
dcat_ap_flag = os.path.join(scandir, "dcat_ap_create")
if os.path.isfile(dcat_ap_flag):
    os.remove(dcat_ap_flag)
    scriptname = 'Dcat_ap_Create.py'
    cmdline = scriptdir + " " + scriptname
    my_log.info("CmdLine: {c}".format(c=cmdline))
    subprocess.call(cmdline)
my_log.info("End Application")
