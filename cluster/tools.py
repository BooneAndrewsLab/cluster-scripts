import hashlib
import os
import sys
import xml.etree.ElementTree as Et
from datetime import datetime, timedelta
from subprocess import PIPE, Popen

from cluster.config import ANSI_ESC, WIDTH, USER


def get_input():
    input_func = input
    if '__builtin__' in sys.modules:  # if we're using python2, fallback to raw_input
        # noinspection PyUnresolvedReferences
        input_func = sys.modules['__builtin__'].raw_input
    return input_func


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    Code adapted from: https://stackoverflow.com/a/3041990
    """

    input_func = get_input()

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


def confirm_delete(question, confirmation_string):
    """Ask a question via raw_input() with a string the user must repeat to confirm.
    Code adapted from: https://stackoverflow.com/a/3041990

    :param question: Question to show
    :param confirmation_string: String to be repeated
    :type question: str
    :type confirmation_string: str
    :return: Conformation
    :rtype: bool
    """
    input_func = get_input()

    prompt = "\nConfirm by typing in the number of jobs to be deleted: "

    while True:
        sys.stdout.write(question + prompt)
        choice = input_func().lower()
        return choice == confirmation_string


def run_cmd(cmd, inp=None):
    proc = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, universal_newlines=True)
    out, err = proc.communicate(input=inp)
    if err:
        raise Exception("Error running command: %s" % err)

    return ANSI_ESC.sub('', out)


def cache_cmd(cmd, max_seconds=60, ignore_cache=False):
    """ Run and cache the command for 1min

    :param cmd: Command to execute
    :param max_seconds: How many seconds should the output be cached
    :param ignore_cache: Ignore cached output, re-run the command
    :type cmd: str
    :type max_seconds: int
    :type ignore_cache: bool
    :return: cmd output
    :rtype: str
    """

    hsh = hashlib.sha1(cmd.encode()).hexdigest()
    cached_file = os.path.join('/tmp', '{user}-{hash}'.format(user=USER, hash=hsh))
    now = datetime.now()

    if not ignore_cache and os.path.exists(cached_file):
        age = now - datetime.fromtimestamp(os.path.getmtime(cached_file))
        if age.total_seconds() < max_seconds:
            with open(cached_file) as cached_in:
                return cached_in.read()

    ret = run_cmd(cmd)

    with open(cached_file, 'w') as cached_out:
        cached_out.write(ret)

    return ret


def parse_xml(xml_string):
    if not xml_string:
        return []

    return Et.fromstring(xml_string)


def read_xml(cmd):
    """ Execute cmd and parse the output XML

    :param cmd: Command to run
    :type cmd: str
    :return: List of children elements in xml root
    :rtype: list[Et.Element]
    """
    return parse_xml(run_cmd(cmd))


def print_table(headers, data):
    """ Print a table in terminal, properly padded

    :param headers: Table headers
    :param data: Table data
    :type headers: list[str]
    :type data: list[list]
    """
    sizes = [max(map(len, col)) for col in zip(headers, *data)]  # Find optimal column size
    columns = ['%%-%ds' % s for s in sizes]

    # Pad last column with leftover space
    out_len = ' | '.join(columns[:-1]) % tuple(headers[:-1])
    free_space = max(32, WIDTH - 3 - len(out_len))
    columns[-1] = '%%-%ds' % free_space
    columns_format = ' | '.join(columns)
    header = columns_format % tuple(headers)

    print(header)
    print('=' * len(header))

    for node in data:
        print(columns_format % tuple(node))


def environment_exists(env_name):
    """Checks if conda environment exists. Throws SubmitException if conda is not available.

    :param env_name: Conda environment name
    :type env_name: str
    :return: Environment exists
    :rtype: bool
    """
    ret = run_cmd('conda env list')

    environments = set()
    for line in ret.splitlines():
        if line and not line.startswith('#'):
            environments.add(line.split()[0])

    return env_name in environments


def batch(iterable, n=1):
    """ Adapted from https://stackoverflow.com/a/8290508

    :param iterable: iterable we want to split
    :param n: size of a batch
    :type iterable: [list|tuple]
    :type n: int
    :return: Input interable split into batches
    :rtype: [list|tuple]
    """

    size = len(iterable)
    for ndx in range(0, size, n):
        yield iterable[ndx:min(ndx + n, size)]


def parse_timearg(arg, since=datetime.now()):
    """Parse a human readable timedelta option: 5h,3w,2d,... and subtracts it from the date

    :param arg: timedelta string to parse
    :param since: reference datetime, or now() by default
    :type arg: string
    :type since: datetime
    :return: Adjusted datetime
    :rtype: datetime
    """
    amount = int(arg[:-1])
    period = arg[-1]

    return since - timedelta(
        **{{'h': 'hours', 'd': 'days', 'w': 'weeks'}[period]: amount}
    )


def generic_to_gb(val):
    """Convert any random unit to GB

    :param val: size in any unit (including two letter unit)
    :type val: str
    :return: size in GB
    :rtype: float
    """
    unit = val[-2:].lower()
    val = int(val[:-2])
    return val / {'kb': 1048576., 'mb': 1024., 'gb': 1.}[unit]


def truncate_str(s, length=32):
    """ Shorten the string to length and add 3 dots

    :param s: String to truncate
    :param length: Truncate threshold
    :type s: str
    :type length: int
    :return: Truncated string
    """
    if len(s) > length:
        s = s[:length - 3] + '...'
    return s
