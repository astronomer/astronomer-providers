from docutils import nodes
from docutils.parsers.rst import directives
from sphinx.directives.code import LiteralIncludeReader
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import set_source_info


class ExampleInclude(SphinxDirective):
    """Extract the text between two tag"""

    has_content = True

    git_repo = "https://github.com/astronomer/astronomer-providers/tree/main/"

    option_spec = {
        "dedent": int,
        "linenos": directives.flag,
        "lineno-start": int,
        "lineno-match": directives.flag,
        "language": directives.unchanged_required,
        "pyobject": directives.unchanged_required,
        "lines": directives.unchanged_required,
        "start-after": directives.unchanged_required,
        "end-before": directives.unchanged_required,
        "class": directives.class_option,
        "name": directives.unchanged,
    }

    def run(self):
        """Extract text and return list of node"""
        location = self.state_machine.get_source_and_line(self.lineno)
        rel_filename, filename = self.env.relfn2path(self.content[0])
        reader = LiteralIncludeReader(filename, self.options, self.config)
        text, lines = reader.read(location=location)
        example_git_location = f"# {self.git_repo}{rel_filename[3:]}"
        text = text.__add__(example_git_location)
        retnode = nodes.literal_block(text, text, source=filename)
        set_source_info(self, retnode)

        return [retnode]


def setup(app):
    """Register directive"""
    app.add_directive("exampleinclude", ExampleInclude)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
