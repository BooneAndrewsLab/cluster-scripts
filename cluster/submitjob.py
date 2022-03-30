#!/usr/bin/env python
import os
import re
import sys

from cluster.common import list_node_names
from cluster.config import HOME
from cluster.submit import sanitize_cmd, submit_jobs
from cluster.tools import environment_exists, batch


def main():
    # noinspection PyCompatibility
    import argparse

    class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter):
        """
        We'd like to have the REMAINDER argument formatted in a nicer way and the epilogue is already formatted
        """

        # noinspection PyProtectedMember
        def _format_args(self, action, default_metavar):
            if action.nargs == argparse.REMAINDER:
                get_metavar = self._metavar_formatter(action, default_metavar)
                return '%s' % get_metavar(1)
            return super(CustomHelpFormatter, self)._format_args(action, default_metavar)

    parser = argparse.ArgumentParser(
        description='Submit a job to queue.',
        formatter_class=CustomHelpFormatter,
        epilog="""Job STDERR is merged with STDOUT and redirected to %s/pbs-output/.
Any job exceeding the run time and memory limits will be killed automatically.

All options following your command are considered a property of your command, please state the
submitjob options like walltime, cpu and memory before the command ie:

EXAMPLE #1: submitjob -w 12 -m 5 my_command.py
\t- walltime is 12 hours
\t- memory is 5GB

EXAMPLE #2: submitjob my_command.py -w 12 -m 5
\t- walltime is 24 hours (default)
\t- memory is 2GB (default) 
\t- -w and -m options are ignored by submitjob and used by my_command.py""" % (HOME,))

    parser.add_argument('command', nargs=argparse.REMAINDER, metavar='CMD OPTIONS INPUT',
                        help='The command to run on the cluster. Note that any output redirection or pipe symbols '
                             'must be escaped, i.e.   \\> or \\|')
    parser.add_argument('-w', '-walltime', '--walltime', type=float, default=24,
                        help='The expected run time in hours, default 24.')
    parser.add_argument('-m', '-mem', '--mem', type=float, default=2,
                        help='Max amount of memory to be used in Gb, default 2.')
    # noinspection PyTypeChecker
    parser.add_argument('-c', '-cpu', '--cpu', type=int, default=1,
                        help='Number of CPUs required on a single node, default 1.')
    parser.add_argument('-N', '-node', '--node', type=str, help='Request specific node by name. Ie: dc01')
    parser.add_argument('-e', '-conda-environment', '--conda-environment',
                        help='Activate this environment before running a job.')
    parser.add_argument('-conda-profile', '--conda-profile',
                        help='Path to conda profile. Used for local conda installations.',
                        default='/etc/profile.d/conda.sh')
    parser.add_argument('-f', '-file', '--file', type=argparse.FileType('rU'),
                        help='Read commands from a file, one per line. If a "command" is specified as a positional '
                             'argument this will be ignored.')
    parser.add_argument('-l', '-disable-log', '--disable-log', action='store_true',
                        help='Disable job logging.')
    parser.add_argument('-L', '-log-path', '--log-path', default=os.path.join(HOME, '.pbs_log'),
                        type=argparse.FileType('a'),
                        help='Where to log submitted jobs.')
    parser.add_argument('-E', '-email', '--email', default=None,
                        help='Send an email to this address when a job ends or is aborted')
    parser.add_argument('-n', '-name', '--name', default=None,
                        help='Give submitted job(s) a verbose name.')
    parser.add_argument('-a', '-args', '--args', type=argparse.FileType('rU'),
                        help='File with a list of arguments to the job for batch submitting. '
                             'Works only with direct command, not -f. '
                             'Batch-size of arguments are appended to the end of command, '
                             'or they replace a "{}" if found.')
    # noinspection PyTypeChecker
    parser.add_argument('-b', '-batch-size', '--batch-size', type=int,
                        help='Number of arguments from <args> to use per job.')
    parser.add_argument('-p', '-pretend', '--pretend', action='store_true',
                        help='Don\'t submit, print the commands out instead.')

    args = parser.parse_args()

    # VARIOUS CHECKS

    if not args.command and not args.file:
        parser.error("Missing command to submit")

    if args.command and re.search(r'^\d', args.command[0]):
        parser.error(
            "You are trying to use the obsolete syntax for submitjob. Please run it with --help to see the new usage.")

    if args.args:
        if args.file:
            parser.error("Arguments (-a) only work with single command, not -f.")
        if not args.batch_size:
            parser.error(
                "Trying to use arguments without batch size. "
                "Please add -b to define how many arguments should be added to command per submitted job.")

    # noinspection PyBroadException
    try:
        if args.conda_environment and not environment_exists(args.conda_environment):
            parser.error(
                'Could not find conda environment "%s", '
                'please check spelling and make sure the environment exists' % args.conda_environment)
    except Exception:
        parser.error('Conda environments are not supported on this system')

    # PREP WORK

    if args.pretend:  # Don't write log if we don't submit
        args.disable_log = True

    node = "1"
    if args.node:
        node = None
        for existing_node in list_node_names():
            if existing_node.startswith(args.node):
                node = existing_node
                break

        if not node:
            parser.error("%s node does not exist" % args.node)

    commands = []

    if args.command:
        # single command takes precedence
        if args.file:
            sys.stderr.write("WARNING: Ignoring commands from file (-f/--file), direct command takes precedence.\n")

        if args.args:
            cmd = args.command

            if '{}' not in cmd:
                cmd.append('{}')  # add the args placeholder to the end for appending

            cmd_args = [arg_fragment.strip() for arg_fragment in args.args]

            for arg_batch in batch(cmd_args, args.batch_size):
                insert_idx = cmd.index('{}')
                expanded_cmd = cmd[:insert_idx] + [('"%s"' % b) for b in arg_batch] + cmd[insert_idx + 1:]
                commands.append(' '.join(map(sanitize_cmd, expanded_cmd)))
        else:
            commands.append(' '.join(map(sanitize_cmd, args.command)))
    elif args.file:
        # commands from file should be formatted correctly already
        commands = [c.strip() for c in args.file if c.strip()]

    if args.email and len(commands) > 10:
        parser.error("Sending email is not supported when submitting more than 10 jobs in a batch")

    submit_jobs(
        commands,
        args.log_path if not args.disable_log else None,
        args.pretend,
        walltime=args.walltime,
        mem=args.mem,
        cpu=args.cpu,
        email=args.email,
        job_name=args.name,
        pretend=args.pretend,
        environment=args.conda_environment,
        conda_profile=args.conda_profile,
        node=node
    )


if __name__ == '__main__':
    main()
