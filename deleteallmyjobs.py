#!/usr/bin/env python
import os
import re
import sys
from subprocess import PIPE, Popen

USER = os.getenv("USER")


def main():
    if os.getuid() == 0:
        print("This script can not run by root!")
        exit(1)

    proc = Popen('qstat -u %s' % USER, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True,
                 universal_newlines=True)
    qstat, err = proc.communicate()
    if err:
        raise Exception("Can't run qstat: %s" % err)

    jobs = re.findall(r'^(\d+)[.]', qstat, flags=re.M)

    if not query_yes_no("Are you really sure you want to delete all your jobs (%d)?" % len(jobs), default="no"):
        print("No jobs were deleted.")
        return

    print("Deleting jobs: %s" % ' '.join(jobs))

    proc = Popen('qdel %s' % ' '.join(jobs), shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True,
                 universal_newlines=True)
    qdel, err = proc.communicate()
    if err:
        raise Exception("Can't run qdel: %s" % err)


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    Code adapted from: https://stackoverflow.com/a/3041990
    """

    input_func = input
    if '__builtin__' in sys.modules:  # if we're using python2, fallback to raw_input
        input_func = sys.modules['__builtin__'].raw_input

    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input_func().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


if __name__ == '__main__':
    main()
