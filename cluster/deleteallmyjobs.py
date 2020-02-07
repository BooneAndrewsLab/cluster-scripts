#!/usr/bin/env python
import os
import re

from cluster.config import USER
from cluster.tools import query_yes_no, run_cmd


def main():
    if os.getuid() == 0:
        print("This script can not run by root!")
        exit(1)

    # noinspection PyCompatibility
    import argparse
    parser = argparse.ArgumentParser(
        description='Deletes all queued and running jobs.')
    _ = parser.parse_args()

    qstat = run_cmd('/usr/bin/qstat -u %s' % USER)
    jobs = re.findall(r'^(\d+)[.]', qstat, flags=re.M)

    if not query_yes_no("Are you really sure you want to delete all your jobs (%d)?" % len(jobs), default="no"):
        print("No jobs were deleted.")
        return

    print("Deleting jobs: %s" % ' '.join(jobs))

    _ = run_cmd('qdel %s' % ' '.join(jobs))


if __name__ == '__main__':
    main()
