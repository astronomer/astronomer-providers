import re
import sys

if __name__ == "__main__":
    pattern = r"^Release \d{1,}\.\d{1,}\.\d{1,}$"
    if len(sys.argv) == 2:
        commit_message = sys.argv[1]
        if not re.fullmatch(pattern, commit_message):
            print(f"{commit_message} does not match '{pattern}'")
            sys.exit(1)
        else:
            print(f"{commit_message} matches '{pattern}'")
    else:
        raise ValueError("One positional argument 'commit message' is required")
