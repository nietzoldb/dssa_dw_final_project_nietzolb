from asyncio import queue as ASqueue
from queue import queue as TSqueue
from typing import Union 





class QueueFactory:
    """Factory class that returns a supported queue type """
    @staticmethod
    def factory(type: str = 'default') -> Union[TSqueue, ASqueue]:
        
        '''Factory that returns a queue based on type

        Args:
            type (str): type of queue to use. Defaults to
            FIFO thread-safe queue. Other accepted types are 'multi-threading' or 'asyncio' 

        Returns:
            Either a Queue | AsyncQueue 
        '''
        
        if type == 'default':
            return TSqueue()
        elif type == 'multi-threading':
            return TSqueue()
        elif type == 'asyncio':
            return ASqueue()
        else:
            raise ValueError(type)