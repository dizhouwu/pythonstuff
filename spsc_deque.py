import concurrent.futures
import queue
import os
import shutil
from collections import deque
import threading
import time


%%time 
CPU_WORK = 1000000000
N = 100
sum(range(CPU_WORK))


# without SPSC
try:
    shutil.rmtree("test_data1")
except:
    pass
os.makedirs("test_data1")
start = time.time()
for i in range(N):
    sum(range(CPU_WORK)) # mock CPU intensive, > 1s
    with open(f"test_data1/{i}", "w") as f:
        f.write(str(i))

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    def _work(i):
        with open(f"test_data1/{i}") as f:
            time.sleep(1) # mock IO intensive
    [executor.submit(_work, i) for i in range(N)]
print(f"Took {time.time() - start} secs")


# with SPSC
try:
    shutil.rmtree("test_data1")
except:
    pass
os.makedirs("test_data1")
start = time.time()
q = deque([])
def _consume():
    cnt = 0
    while True:
        if cnt == N:
            break
        if len(q) > 0:
            item = q.pop()
            with open(f"test_data1/{item}") as f:
                time.sleep(1) # mock IO intensive
            cnt+=1
    
t = threading.Thread(target=_consume)
t.start()
for i in range(N):
    sum(range(CPU_WORK)) # mock CPU intensive, > 1s
    with open(f"test_data1/{i}", "w") as f:
        f.write(str(i))
    q.appendleft(i)
t.join()
print(f"Took {time.time() - start} secs")
