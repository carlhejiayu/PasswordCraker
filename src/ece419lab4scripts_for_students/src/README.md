Client
======
To start the Client run: 
    java ClientDriver <IP Address of ZK>:<Port>

FileServer
==========
To start the FileServer run: 
    java Fileserver <args>

JobTracker
==========
To start the JobTracker run: 
    java JobTracker <args>

Worker
==========
To start the Worker run: 
    java Worker <IP Address of ZK>:<Port>


Notes
=====
When a worker starts, it registers itself with ZK.

When a FileServer starts, it tries to register itself as primary,
if this fails, this is probably because a primary exists, and it
becomes secondary.




