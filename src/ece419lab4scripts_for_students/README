Job Tracker: 
start_jobtracker.sh <zookeeper host> <zookeeper port>

Worker:
start_worker.sh <zookeeper host> <zookeeper port>

FileServer:
start_fileserver.sh <zookeeper host> <zookeeper port>

Client 
submit_job.sh <zookeeper host> <zookeeper port> <password hash>
— this allows a user to submit a job request. It prints the job id to the console, the student can defined their own student ID

check_job_status.sh <zookeeper host> <zookeeper port> <job id>
— this allows a user to check the job status. This prints the status: Processing, Finished, Failed, Password Not Found, etc… we need to define these for them

get_job_result.sh <zookeeper host> <zookeeper port> <job id>
— this allows a user to get the result, i.e. the cracked password. this prints the result to the console

(optional) get_job_list.sh <zookeeper host> <zookeeper port>
— this allows a user to see a list of all the submitted jobs, and their status. May be too time consuming for student, so we don’t have to force them to do this.

Also we probably need to discuss the format of the student output and ask them to use the same format.
