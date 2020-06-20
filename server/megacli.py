import os


def main():
    if os.getuid() != 0:
        print("This script must be run by root!")
        exit(1)

    # noinspection PyCompatibility
    import argparse
    parser = argparse.ArgumentParser(description='Analyze MegaCli output')
    _ = parser.parse_args()

    


if __name__ == '__main__':
    main()
