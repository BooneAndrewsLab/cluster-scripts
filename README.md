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

submit each line in _batch_file.txt_ as a separate command:
```sh
$ python submitjob.py -f batch_file.txt
```

submit _\<command\>_ and receive an email when job is aborted or finished:
```sh
$ python submitjob.py -E m.usaj@utoronto.ca <command>
```

generate a list of all failed jobs in the past 31days:
```bash
# -f -> print failed jobs; -l 64d -> limit printed jobs to last 64d(ays); `-o cmd` -> print commands
$ python jobstatus.py details -f -l 64d -o cmd > failed_this_month.txt
# You can then use this file to re-submit failed jobs (possibly with adjusted resource requirements)
$ python submitjob.py -f failed_this_month.txt
```

delete all queued jobs:
```bash
$ python jobstatus.py details -q -o jobid
28833598 28831572 28816963 28816962
$ qdel 28833598 28831572 28816963 28816962

# or in one line:
$ python jobstatus.py details -q -o jobid | xargs qdel
```

clean up pbs-output and pbs_log, keep only jobs that are at most 2 weeks old:
```bash
$ python jobstatus.py archive 2w
```

# Fun facts

 - submitjob.py keeps a log of all submited commands in _~/.pbs_log_ along with a timestamp and jobid. Makes it easier to re-submit jobs if they fail.
