
from .memory import SharedFrame
from functools import partial
import threading
import uuid
from collections import deque
import numpy as np
import itertools
class Client:
    def __init__(self,name = ''):
        if name:
            from redis import Redis
            self.redis_client = Redis()
        self.id = str(uuid.uuid4())[:8]
        self.name = name
        self.resources = {}
        self.res = {}
        self.update = {}
        self.concat = {}
        self.running = True
        self.threads = []
    def request(self, model, buf=None):
        if not buf:
            buf = 10
        else:
            self.concat[model] = True
        self.resources[model] = SharedFrame(model, mode="r")
        self.resources[model].signin(self.id)
        self.update[model] = threading.Lock()
        self.update[model].acquire()
        self.res[model] = deque([(np.zeros(self.resources[model].shape,self.resources[model].dtype),i) for i in range(buf)],maxlen=buf)
        
    def run(self):
        for model in self.resources:
            thread = threading.Thread(target=partial(self.retrive, model=model))
            thread.start()
            self.threads.append(thread)
            print(f"start retriving {model}")

    def retrive(self, model):
        while self.running:
            try:
                data, stm = self.resources[model].read(self.id)
                self.res[model].append((data.copy(), stm))
                if self.update[model].locked():
                    self.update[model].release()
            except KeyError:
                # If the semaphore doesn't exist (client signed out), break the loop
                print(f"Client {self.id} semaphore not found, stopping retrieval for {model}")
                break
            except Exception as e:
                print(f"Error in retrive for {model}: {e}")
                break
    def report(self,txt):
        self.redis_client.rpush(self.name, txt)
    def latest(self, model):
        self.update[model].acquire()
        return np.stack([i[0] for i in list(itertools.islice(self.res[model],len(self.res[model])))]) if self.concat.get(model,None) else self.res[model][-1]
    def close(self):
        self.running = False
        for res in self.resources.values():
            res.signout(self.id)
        # Wait for all threads to finish
        for thread in self.threads:
            thread.join(timeout=1.0)  # Wait up to 1 second for each thread

    def __del__(self):
        self.close()