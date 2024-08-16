from __future__ import annotations

import argparse


def remove_lines_after(file_path, target_line):
    """
    Remove all lines in a file after the first occurrence of a specified target line.

    This function reads a file and rewrites it, keeping only the lines up to and including
    the first occurrence of the target line. All lines following the target line are removed.

    Args:
        file_path (str): The path to the file to be modified.
        target_line (str): The line after which all content in the file should be removed.
    """
    with open(file_path) as input_file:
        lines = input_file.readlines()

    with open(file_path, "w") as output_file:
        for line in lines:
            if target_line in line:
                break
            output_file.write(line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file_path", help="file path", type=str)
    args = parser.parse_args()
    target_line = "from tests.system.utils import get_test_run"
    remove_lines_after(args.file_path, target_line)
