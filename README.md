# CCBR cluster scripts

Updated PBS scripts for CCBR's cluster.

# Python compatibility

Tested on Python 2.7 and Python 3.x

# Requirements

None

# Examples

submit _\<command\>_ with all default settings:
```sh
$ python submitjob.py <command>
```

submit _\<command\>_ with defined wall-time, memory and cpu (fractions supported):
```sh
$ python submitjob.py -w 24.5 -m 2.5 -c 2 <command>
```

submit each line in _batch_file.txt_ as a separate command
```sh
$ python submitjob.py -f batch_file.txt
```

submit _\<command\>_ and receive an email when job is aborted or finished
```sh
$ python submitjob.py -E m.usaj@utoronto.ca <command>
```

# Fun facts

 - submitjob.py keeps a log of all submited commands in _~/.pbs_log_ along with a timestamp and jobid. Makes it easier to re-submit jobs if they fail.
