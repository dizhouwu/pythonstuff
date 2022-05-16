from queue import Queue
import concurrent.futures
import threading
 
q = Queue(1000)
start = time.time()
finished = False
 
def _produce(q):
    for i in range(1000):
        time.sleep(0.05)
        print(f"Procuding: {i}")
        q.put(i)
 
def _consume(q):
    while True:
        time.sleep(0.1)
         i = q.get()
        if i == 999:
            global finished
            finished=True
        print(f"Thread: {threading.get_ident()} Uploading: {i}")
        q.task_done()
     
produce_thread = threading.Thread(target=_produce, args=(q,))
produce_thread.start()
consume_thread = threading.Thread(target=_consume, args=(q,))
consume_thread.start()
q.join()
while not finished:
    time.sleep(0.1)
print(f"{time.time() - start} seconds")



from queue import Queue
import concurrent.futures
import threading
import time
 
q = Queue(1000)
start = time.time()
finished = False
 
def _produce(q):
    for i in range(1000):
        time.sleep(0.05)
        print(f"Procuding: {i}")
        q.put(i)
 
def _consume(q):
    while True:
        def _work(q):
            time.sleep(0.1)
            i = q.get()
            if i == 999:
                global finished
                finished=True
            print(f"Thread: {threading.get_ident()} Uploading: {i}")
            q.task_done()
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures= [executor.submit(_work, q=q) for i in range(20)]
     
produce_thread = threading.Thread(target=_produce, args=(q,))
produce_thread.start()
consume_thread = threading.Thread(target=_consume, args=(q,))
consume_thread.start()
q.join()
while not finished:
    time.sleep(0.1)
print(f"{time.time() - start} seconds")
