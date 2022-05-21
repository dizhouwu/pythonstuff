import ast
import functools
import inspect
ns = {}


import ast
import inspect
import types

class Rewrite(ast.NodeTransformer):
    # def visit_FunctionDef(self, node):
    #     print(node.name)
    #     return node
    
    def visit_arg(self, node):
        print(dir(node))
        print(node.arg)
        return node
        
def dag(f):

    source = inspect.getsource(f)
    source = '\n'.join(source.splitlines()[1:]) # remove the decorator first line.
    print(source)

    old_code_obj = f.__code__
    old_ast = ast.parse(source)
    new_ast = Rewrite().visit(old_ast)
    new_code_obj = compile(new_ast, old_code_obj.co_filename, 'exec')
    new_f = types.FunctionType(new_code_obj.co_consts[0], f.__globals__)
    return new_f

@dag
def foo(x):
    return x
