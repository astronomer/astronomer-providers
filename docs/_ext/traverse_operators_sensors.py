from __future__ import annotations

import ast
import importlib
import os
from pathlib import Path

from docutils import nodes
from sphinx.util.docutils import SphinxDirective

OPERATORS_DIR = "operators"
SENSORS_DIR = "sensors"
OPERATOR_CLASS_NAME_SUBSTRING = "Operator"
SENSOR_CLASS_NAME_SUBSTRING = "Sensor"
ASYNC_SUBSTRING = "Async"

BASE_IMPORT_PATH = "astronomer.providers"

CURRENT_FILE_PATH = Path(__file__)
ASTRONOMER_PROVIDERS_PATH = CURRENT_FILE_PATH.parent.parent.parent / "astronomer" / "providers"


def collect_elements(
    directory_path: str,
    files: list[str],
    element_type: str,
    elements_list: list[tuple[str, bool, str, str]],
    current_module_path: str,
):
    """
    Checks that ``Async`` class definitions exist using ``ast.parse`` in the given files, that those are
    ``operators/sensors`` and appends the operator/sensor along with its import path and deprecation status to the
    given output list.

    :param directory_path: path of the directory in which the given ``files`` are located
    :param files: list of files to look for async operator/sensor class definitions
    :param element_type: type of element to look for i.e. Operator or Sensor
    :param elements_list: existing list of collected elements i.e. operators and sensors in which to append
        new discoveries
    :param current_module_path: module path to be used to generate the import path for the element
    """
    for file_name in files:
        if file_name[-3:] != ".py":
            continue
        module_import_path = f"{current_module_path}.{file_name[:-3]}"
        full_file_path = os.path.join(directory_path, file_name)
        with open(full_file_path) as file:
            node = ast.parse(file.read())
            for element in node.body:
                if isinstance(element, ast.ClassDef):
                    element_name = element.name
                    if element_type in element_name and ASYNC_SUBSTRING in element_name:
                        module = importlib.import_module(module_import_path)
                        cls = getattr(module, element_name)
                        is_deprecated = bool(getattr(cls, "is_deprecated", False))
                        post_deprecation_replacement = str(getattr(cls, "post_deprecation_replacement", ""))
                        elements_list.append(
                            (element_name, is_deprecated, module_import_path, post_deprecation_replacement)
                        )


def search_providers():
    """
    Walks through the ``astronomer providers`` repository to search ``operators`` and ``sensors`` and their import
    paths.
    """
    operators, sensors = [], []
    base_dir_posix_path = ASTRONOMER_PROVIDERS_PATH.as_posix()
    for directory, _, files in os.walk(ASTRONOMER_PROVIDERS_PATH):
        directory_name = directory.split("/")[-1]
        current_module_path = BASE_IMPORT_PATH + directory.split(base_dir_posix_path)[-1].replace("/", ".")
        if directory_name == OPERATORS_DIR:
            collect_elements(directory, files, OPERATOR_CLASS_NAME_SUBSTRING, operators, current_module_path)
        elif directory_name == SENSORS_DIR:
            collect_elements(directory, files, SENSOR_CLASS_NAME_SUBSTRING, sensors, current_module_path)

    operators.sort(key=lambda operator_tuple: operator_tuple[0])
    sensors.sort(key=lambda sensor_tuple: sensor_tuple[0])
    return operators, sensors


class TraverseOperatorsSensors(SphinxDirective):
    """Directive to list async operators and sensors available in the providers repository"""

    has_content = True

    def run(self):
        """Generates raw html to list the operators and sensors parsed using `ast`."""
        operators, sensors = search_providers()
        operators_html = (
            "<h3>Operators</h3> "
            "<table>"
            "<th>#</th>"
            "<th>Operator name</th>"
            "<th>Is deprecated?</th>"
            "<th>Import path</th>"
        )
        for index, operator in enumerate(operators, start=1):
            class_def_link = operator[2].replace(".", "/") + "/index.html#" + operator[2] + "." + operator[0]
            operators_html += (
                f"<tr>" f"<td>{index}</td>" f"<td><span><a id={operator[0]}>{operator[0]}</a></span></td>"
            )
            if operator[1]:
                operators_html += "<td style='text-align: center;'><b> Yes </b></td>"
                operators_html += f"<td><b>Old Path:</b>\n <span><pre><code class='python'>from {operator[2]} import {operator[0]}</code></pre></span>\n <b>New Path:</b> \n<span><pre><code class='python'>{operator[3]}</code></pre></span></td>"
            else:
                operators_html += "<td style='text-align: center;'><b> No </b></td>"
                operators_html += f"<td><span><pre><code class='python'>from {operator[2]} import {operator[0]}</code></pre></span></td>"
            operators_html += "</tr>"
            # The below script generates the URL for the class definition by extracting the selected doc version from
            # the current browser URL location.
            operators_html += (
                f"<script> "
                f"var base_url = '' + window.location.pathname.split('/providers/')[0] + '/_api/';"
                f"var class_def_link = base_url + '{class_def_link}';"
                f"document.getElementById('{operator[0]}').setAttribute('href', class_def_link);"
                f"</script>"
            )
        operators_html += "</table> <br/>"

        sensors_html = (
            "<h3>Sensors</h3>"
            "<table>"
            "<th>#</th>"
            "<th>Sensor name</th>"
            "<th>Is deprecated?</th>"
            "<th>Import path</th>"
        )
        for index, sensor in enumerate(sensors, start=1):
            class_def_link = sensor[2].replace(".", "/") + "/index.html#" + sensor[2] + "." + sensor[0]
            sensors_html += (
                f"<tr>" f"<td>{index}</td>" f"<td><span><a id={sensor[0]}>{sensor[0]}</a></span></td>"
            )
            if sensor[1]:
                sensors_html += "<td style='text-align: center;'><b> Yes </b></td>"
                sensors_html += f"<td><b>Old Path:</b>\n <span><pre><code class='python'>from {sensor[2]} import {sensor[0]}</code></pre></span>\n <b>New Path:</b> \n<span><pre><code class='python'>{sensor[3]}</code></pre></span></td>"
            else:
                sensors_html += "<td style='text-align: center;'><b> No </b></td>"
                sensors_html += f"<td><span><pre><code class='python'>from {sensor[2]} import {sensor[0]}</code></pre></span></td>"
            sensors_html += "</tr>"
            sensors_html += (
                f"<script> "
                f"var base_url = '' + window.location.pathname.split('/providers/')[0] + '/_api/';"
                f"var class_def_link = base_url + '{class_def_link}';"
                f"document.getElementById('{sensor[0]}').setAttribute('href', class_def_link);"
                f"</script>"
            )
        sensors_html += "</table> <br/>"
        base_html = (
            "<head>"
            "<link rel='stylesheet' "
            "href='https://cdnjs.cloudflare.com/ajax/libs/highlight.js/10.0.3/styles/default.min.css'> "
            "<script src='https://cdnjs.cloudflare.com/ajax/libs/highlight.js/10.0.3/highlight.min.js'>"
            "</script>"
            "<script>hljs.initHighlightingOnLoad();</script>"
            "<style>"
            "table {border: 1px solid black; border-collapse:collapse} "
            "tr, th, td {border: 1px solid black;} "
            "th, td {padding-top: 10px; padding-bottom: 10px; padding-left: 5px; padding-right: 20px;} "
            "td{font-family: "
            "'Consolas', 'Menlo', 'DejaVu Sans Mono', 'Bitstream Vera Sans Mono', "
            "monospace; font-size: 0.9em;} "
            "th {font-family: 'Georgia, serif'; font-size: 17px;} "
            "span{ background-color: #ecf0f3; "
            "-webkit-background-clip: content-box; background-clip: content-box;} "
            "</style>"
            "</head>"
            "<body>"
        )
        html = base_html + operators_html + sensors_html + "</body>"
        node = nodes.raw("", html, format="html")
        return [node]


def setup(app):
    """Register ``traverse_operators_sensors`` directive"""
    app.add_directive("traverse_operators_sensors", TraverseOperatorsSensors)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }


if __name__ == "__main__":
    search_providers()
