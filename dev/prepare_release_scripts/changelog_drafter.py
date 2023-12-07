import argparse
import re
import subprocess  # nosec B404
from datetime import datetime

CHANGELOG_HEADER = """Changelog
=========

"""

CHANGELOG_PATH = "CHANGELOG.rst"

# fmt: off
EXCLUDE_PATTERN = (
    r"("
    r"(\[pre\-commit\.ci\] pre-commit autoupdate)|"
    r"((ci|tests|build)\(.*\): (.*))|"
    r"(Bump version to \d+.\d+.\d+-dev1)|"
    r"(Update runtime image tag to \d+.\d+.\d+-python-3.\d+-base)"
    r")"
    r"( \(#\d+\)){0,1}"
)
# fmt: on


def draft_changelog(release_version: str) -> None:
    """Extract commit messages after release_version and write them into changelog"""
    latest_tag = (
        subprocess.run(  # nosec B603 B607
            ["git", "tag", "--sort=-authordate", "--merged"],
            capture_output=True,
            text=True,
        )
        .stdout.split("\n")[0]
        .strip()
    )
    print(f"The latest version is {latest_tag}")
    git_log_messages = (
        subprocess.run(
            ["git", "log", "--format=%s", f"{latest_tag}.."], capture_output=True, text=True
        )  # nosec B603 B607
        .stdout.strip()
        .split("\n")
    )

    potential_included_messages = []
    for message in git_log_messages:
        if re.match(EXCLUDE_PATTERN, message):
            print(f'Exclude commit: "{message}"')
        else:
            potential_included_messages.append(message)
    concatenated_message = "\n.. ".join(potential_included_messages)

    with open(CHANGELOG_PATH) as changelog_file:
        changelog_content = changelog_file.read()

    release_date = datetime.now().strftime("%Y-%m-%d")
    HEADER_WITH_LATEST_SECTION = f"""{CHANGELOG_HEADER}{release_version} ({release_date})
-------------------

.. {concatenated_message}

"""
    changelog_content = changelog_content.replace(CHANGELOG_HEADER, HEADER_WITH_LATEST_SECTION)

    with open(CHANGELOG_PATH, "w") as output_file:
        output_file.write(changelog_content)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("version_to_release", help="the next version to release (e.g., 1.0.0)", type=str)
    args = parser.parse_args()
    draft_changelog(args.version_to_release)
