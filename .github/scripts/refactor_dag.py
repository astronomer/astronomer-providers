from __future__ import annotations

import argparse


def remove_lines_after(file_path, target_line):
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
