
import sys
import time
import traceback
import asyncio
import threading


# import asyncio
# 
# @asyncio.coroutine
# def periodic():
#     while True:
#         print('periodic')
#         yield from asyncio.sleep(1)
# 
# def stop():
#     task.cancel()
# 
# task = asyncio.Task(periodic())
# loop = asyncio.get_event_loop()
# loop.call_later(5, stop)
# 
# try:
#     loop.run_until_complete(task)
# except asyncio.CancelledError:
#     pass


class Worker(object):
    
    def __init__( self, func, sleep=None, crons=[], precond=[], timelimit=None ):
        
        self._method = func
        
        self.sleep = sleep
        self.crons = crons
        self.precond = precond
        self.timelimit = timelimit
        
        self.name = func.__name__
        
        return
        
    def makeTask( self, parent ):
        
        precond = [ p.name for p in self.precond ]
        
        _method = self._method
        _name = self.name
        
        _init = asyncio.Event( loop=parent.loop )
        _event = asyncio.Event( loop=parent.loop )
        
        
        async def _task( _parent ):
            
            try :
                
                rc = [_parent._works[_pname].init.wait() for _pname in precond ]
                rc = list(rc)
                
                if rc :
                    await asyncio.wait( rc )
                
                print('TASK %s FIRST START' % _name)
                
                await _method(_parent)
            except Exception:
                traceback.print_exc()
                sys.stdout.flush()
                
            return
        
        task = asyncio.Task( _task(parent), loop=parent.loop )
        taskid = id(task)
        
        def _touch( x=None ):
            
            _task = asyncio.Task.current_task()
            
            if id(_task) != taskid :
                print('TouchSign')
                task.touchsign = True
                
            if x != None :
                parent.loop.call_soon_threadsafe( task.hint.add, x )
            
            parent.loop.call_soon_threadsafe( _event.set )
            
            return
        
        task.init = _init
        task.event = _event
        task.name = _name
        task.hint = set()
        task.sleep = self.sleep
        task.touch = _touch
        task.touchsign = False
        task.count = 0
        
        return task

def worker( sleep=None, cron=[], precond=[], timelimit=None ):
    return lambda f : Worker( f, sleep, cron, precond, timelimit )