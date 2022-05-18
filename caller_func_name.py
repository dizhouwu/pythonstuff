import inspect

def bar():
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe,1000000)
    print('caller name:', calframe[1][3])
def commit():
    fuck()

def foo():
    commit()
    
foo()
