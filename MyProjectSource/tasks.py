
class Task():
    
    def __init__(self, func) -> None:
        self.func = func
        
    def run(self, *args, **kwargs):
        result = self.func(*args, **kwargs)
        return result
    
"""    
Class TaskInput():
    def __init__(self, func) -> None:
        self.func = func
        return
"""