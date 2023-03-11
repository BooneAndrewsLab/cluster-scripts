#!/usr/bin/env python3

if __name__ == '__main__':
    import argparse
    import sys
    import smtplib

    parser = argparse.ArgumentParser(
        prog='PBS sendmail hook for RC cluster',
        description='Filters out emails that should not be sent')

    parser.add_argument('to')  # positional argument
    parser.add_argument('-f', '--from-addr')

    args = parser.parse_args()

    if args.to.endswith('.ccbr.utoronto.ca'):
        # Ignore all emails where recepient is hostname of one of our nodes
        exit(0)

    body = sys.stdin.read()

    with smtplib.SMTP('smtp.ccbr.utoronto.ca') as server:
        server.sendmail(args.from_addr, [args.to], body)
