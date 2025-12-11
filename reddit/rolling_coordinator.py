import ast
import tokenize
from io import StringIO

def remove_comments_and_docstrings(source):
    """
    Returns 'source' with all comments and docstrings removed.
    """
    # Step 1: remove docstrings using AST
    parsed = ast.parse(source)

    class RemoveDocstrings(ast.NodeTransformer):
        def visit_FunctionDef(self, node):
            self.generic_visit(node)
            if (node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)):
                node.body.pop(0)
            return node

        def visit_AsyncFunctionDef(self, node):
            return self.visit_FunctionDef(node)

        def visit_ClassDef(self, node):
            self.generic_visit(node)
            if (node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)):
                node.body.pop(0)
            return node

        def visit_Module(self, node):
            self.generic_visit(node)
            if (node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)):
                node.body.pop(0)
            return node

    no_doc_ast = RemoveDocstrings().visit(parsed)
    ast.fix_missing_locations(no_doc_ast)

    # Convert AST back to source
    import astor  # install: pip install astor
    no_doc = astor.to_source(no_doc_ast)

    # Step 2: remove comments using tokenize
    io_obj = StringIO(no_doc)
    out = ""

    prev_toktype = tokenize.INDENT
    last_lineno = -1
    last_col = 0

    for tok in tokenize.generate_tokens(io_obj.readline):
        token_type, token_string, start, end, _ = tok
        start_line, start_col = start
        end_line, end_col = end

        if start_line > last_lineno:
            last_col = 0
        if start_col > last_col:
            out += " " * (start_col - last_col)

        if token_type != tokenize.COMMENT:
            out += token_string

        prev_toktype = token_type
        last_col = end_col
        last_lineno = end_line

    return out

# Usage:
with open("collect_data_2.py") as f:
    cleaned = remove_comments_and_docstrings(f.read())

with open("collect_data_2_v2.py", "w") as f:
    f.write(cleaned)
