
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


class AsyncPipeline( object ):
    
    def __init__( self ):
        
        self.loop = asyncio.new_event_loop()
        
        self._works = {}
        self._crons = []
        for methodname in dir(self):
            
            m = getattr( self, methodname )
            
            if type(m) != Worker:
                continue
            
            t = m.makeTask(self)
            setattr( self, methodname, t )
            self._works[methodname] = t
            
            for c in m.crons :
                self._crons.append( (methodname, c) )
            
        self._cm = asyncio.Task( self._cron_serv(), loop=self.loop )
        
        return
    
    def _run( self ):
        
        #loop = asyncio.get_event_loop()
        asyncio.set_event_loop(self.loop)
        
        tasks = list(self._works.values())
        self.loop.run_until_complete( asyncio.wait(tasks) )
        
        return
    
    def run( self ):
        
        asyncthread = threading.Thread(target=self._run)
        asyncthread.start()
        
        return
    
    async def _cron_serv( self ):
        
        cur = time.time()
        cur = cur - time.localtime(cur).tm_sec
        
        while True:
            
            nxt = 61 - time.localtime().tm_sec
            await asyncio.sleep(nxt)
            
            try :
                while( cur+60 < time.time() ):
                    
                    cur += 60
                    
                    t = time.localtime(cur)
                    t = (t.tm_min, t.tm_hour, t.tm_mday, t.tm_mon, t.tm_wday)
                    
                    for m, c in self._crons:
                        print(t, c, m)
                        print(list([ ( bool((it%ic) == 0) if ic < 0 else bool(it==ic) ) for it, ic in zip(t,c) ]))
                        if all([ ( bool((it%ic) == 0) if ic < 0 else bool(it==ic) ) for it, ic in zip(t,c) ]):
                            print('CRON TOUCH %s %s' % (m, c) )
                            self._works[m].touch()
                            
            except Exception as e :
                print(e)
                raise
                
        return
    
    async def wait( self ):
        
        task = asyncio.Task.current_task()
        
        if not task.init.is_set():
            task.init.set()
        
        task.count += 1
        
        print('TASK %s STOP' % task.name)
        
        r = None
        try :
            await asyncio.wait_for( task.event.wait(), task.sleep )
            task.event.clear()
            task.touchsign = False
            r = task.hint
            task.hint = set()
        except asyncio.TimeoutError :
            pass
        
        print('TASK %s START $ %s' % (task.name, time.asctime()) )
        
        return r
        
        
        