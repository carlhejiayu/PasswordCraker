####################################
#       Author: Jieyu Lin (Eric)   #
####################################
import os
import re
import sys
import getpass
import time
import signal
import traceback
from utils import CommandRunner


# Initial global and constant variable
GENERAL_ERROR = "Error: "
SEPARATOR = "-------------------------------------------------------------"
FILES = ['start_jobtracker.sh',
         'start_worker.sh',
         'start_fileserver.sh',
         'submit_job.sh',
         'check_job_status.sh']
ZOOKEEPER_PORT = 8267
FAIL_PASSWORD_NO_FOUND = "password no found"
running_processes = []

# Ctrl + C handler for killing all the running processes
def signal_handler(signal, frame):
        print('You pressed Ctrl+C! Cleaning all running processes')
        stop_all_running_processes()
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


# Function for kill all the running processes, needed for program exit
def stop_all_running_processes():
    for p in running_processes:
        print("Killing process: " + str(p.pid))
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except Exception, e:
            print(e)


# Check if a specific file exist, raise an exception if it doesn't exist
def check_file_exist(path, filename):
    cm = CommandRunner()
    file_exist, err = cm.run_shell_command_return('ls -l ' + os.path.join(path, filename),
                                                 "checking if " + filename + " exist")
    if not file_exist:
        raise Exception(filename + " does not exist!")

# Check if a process is running, raise an exception if it is not running
# The process param should be a python subprocess object
def check_process_running(process, name):
    process_terminated = process.poll()
    if process_terminated:
        raise Exception("Process for " + name +" with pid " + str(process.pid) + " is not running after starting!")
    else:
        running_processes.append(process)
        print("Process for " + name + " with pid " + str(process.pid) + " is running properly")


# Exerciser 0: check the basic setup before running the test
def exerciser0(run_path):
    print("")
    print(SEPARATOR)
    print("This is the initial check for presence of files")
    print(SEPARATOR)
    for f in FILES:
        check_file_exist(run_path, f)

# Exerciser 1 and 2:
# Exerciser 1 check to see if all the components can be properly started and all the processes are running
# Exerciser 2 submit a job and wait for the result to come back and check the client formats
def exerciser1and2(machines):

    # Exerciser 1
    print("")
    print(SEPARATOR)
    print("Exerciser 1: This is a basic exercise that checks if your programs starts properly and run with no failure")
    print(SEPARATOR)
    if len(machines)<1:
        raise Exception(GENERAL_ERROR + "At least one execution machine should be provided for exerciser 1")
    exe_machine = machines[0]
    cm = CommandRunner()
    p_zk = cm.run_shell_command('~/myzk/bin/zkServer.sh start')
    p_fs = cm.run_shell_command('./start_fileserver.sh ' + exe_machine + ' ' + str(ZOOKEEPER_PORT))
    p_jb = cm.run_shell_command('./start_jobtracker.sh ' + exe_machine + ' ' + str(ZOOKEEPER_PORT))
    p_w1 = cm.run_shell_command('./start_worker.sh ' + exe_machine + ' ' + str(ZOOKEEPER_PORT))
    p_w2 = cm.run_shell_command('./start_worker.sh ' + exe_machine + ' ' + str(ZOOKEEPER_PORT))

    print("File server running on process number: " + str(p_fs.pid))
    print("Job tracker running on process number: " + str(p_jb.pid))
    print("Worker 1 running on process number: " + str(p_w1.pid))
    print("Worker 2 running on process number: " + str(p_w2.pid))

    p_zk.wait()
    print("Zookeeper started running on port: " + str(ZOOKEEPER_PORT))

    time.sleep(2)
    check_process_running(p_fs, "File Server")
    check_process_running(p_jb, "Job Tracker")
    check_process_running(p_w1, "Worker 1")
    check_process_running(p_w2, "Worker 2")
    print("Exerciser 1: PASS")

    # Exerciser 2
    print("")
    print(SEPARATOR)
    print("Exerciser 2: This exerciser submits a job to your cluster and check the result")
    print(SEPARATOR)

    in_progress_pattern = re.compile('In progress')
    success_pattern = re.compile('Password found:.*')
    failure_pattern = re.compile('Failed:.*')
    hashcode = '755f85c2723bb39381c7379a604160d8'
    password = 'good'
    print("Submitting job to find password for hashcode: " + hashcode)
    submit_result, submit_error = cm.run_shell_command_return('./submit_job.sh ' + exe_machine + ' ' +
                                                              str(ZOOKEEPER_PORT) + ' ' + hashcode)

    print("submit result: " + submit_result)
    print("Getting status of the submitted job...")
    while (True):
        status_result, status_error = cm.run_shell_command_return('./check_job_status.sh ' + exe_machine + ' ' +
                                                                  str(ZOOKEEPER_PORT) + ' ' + hashcode)
        print("Current Status is: " + status_result)
        if in_progress_pattern.match(status_result):
            print('Job running in progress')
        elif success_pattern.match(status_result):
            password_found = status_result.split(':')
            if len(password_found)<2:
                raise('Incorrect format for message when password found. Expecting: "Password found: {password here}" '
                      'without the braces')
            else:
                password_found = password_found[1].strip(' \t\n')
            if password_found != password:
                raise Exception('Incorrect password found: The password you found is ' + password_found +
                                ', the expected password is ' + password)
            else:
                print("successfully found password")
            break
        elif failure_pattern.match(status_result):
            reason = status_result.split(':')
            if len(reason < 2):
                raise Exception('Incorrect format for message when failed to find password. Expecting: "Failed: ' + FAIL_PASSWORD_NO_FOUND + '"')
            else:
                reason = reason[1].strip()
            if reason == FAIL_PASSWORD_NO_FOUND:
                raise Exception("Failed to find password: hash code" + hashcode + " is corresponding to password " + password)
            break
        else:
            raise Exception("Message with incorrect format printed by client driver. Please refer to the handout for format.")
        time.sleep(1)
    print("End of exerciser 2")



if __name__ == '__main__':
    print("Starting exerciser script for ECE419 Lab 4")
    if len(sys.argv) < 2:
        print("Execution machines not provided. Please specify execution machine e.g. ug149,ug150 ")
        sys.exit(-1)

    curr_path = os.getcwd()
    machines = str(sys.argv[1]).split(",")

    try:
        exerciser0(curr_path)
        exerciser1and2(machines)
        print("Congratulations! You have passed all the exercisers!")
    except Exception, e:
        print("Exerciser running failed!")
        print str(e)
        traceback.print_exc()
        stop_all_running_processes()


    print("End of exerciser script")