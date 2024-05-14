from __future__ import annotations

import importlib.util
import sys


def load_function(file_path, function_name) -> callable | None:
    try:
        # Load the module from the file path
        spec = importlib.util.spec_from_file_location("module_name", file_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        # Get the function from the module
        if isinstance(callable, function_name):
            function_name = function_name.__name__
        function = getattr(module, function_name)
        return function
    except Exception:
        print(f"Function '{function_name}' not found in module '{module.__name__}'")
        return None
