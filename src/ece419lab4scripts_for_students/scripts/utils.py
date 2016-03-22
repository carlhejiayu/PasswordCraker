####################################
#       Author: Jieyu Lin (Eric)   #
####################################
import subprocess
import os
import re
import datetime
import time
from random import randrange
import struct

class CommandRunner(object):

    def generate_ssh_command (self, host, username, command):
        username = username
        result = 'ssh -o "StrictHostKeyChecking no" -o UserKnownHostsFile=/dev/null %s@%s "%s"' % (username, host, command)
        return result

    def run_command(self,command):
        print command
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        return p

    def run_shell_command(self, cmd, desc=None):
        if desc:
            print desc
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, preexec_fn=os.setsid)
        return p

    def run_shell_command_return(self, cmd, desc=None):
        if desc:
            print desc
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, preexec_fn=os.setsid)
        out,err = p.communicate()
        return (out, err)

    def run_ssh_command(self, host, username, cmd, desc=None):
        full_cmd = self.generate_ssh_command(host, username, cmd)
        return self.run_shell_command(full_cmd, desc)

    def run_ssh_command_return(self, host, username, cmd, desc=None):
        full_cmd = self.generate_ssh_command(host, username, cmd)
        return self.run_shell_command_return(full_cmd, desc)