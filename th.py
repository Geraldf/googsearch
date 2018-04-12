from multiprocessing import Queue
from threading import Thread

def foo(bar):
    print ('hello {0}'.format(bar))
    return bar

que = Queue()
threads_list = list()

t = Thread(target=lambda q, arg1: q.put(foo(arg1)), args=(que, 'world!'))
t.start()
threads_list.append(t)

# Add more threads here
t2 = Thread(target=lambda q, arg1: q.put(foo(arg1)), args=(que, 't2!'))
t2.start()
threads_list.append(t2)
t3 = Thread(target=lambda q, arg1: q.put(foo(arg1)), args=(que, 't3!'))
t3.start()
threads_list.append(t3)
# Join all the threads
for t in threads_list:
    t.join()

# Check thread's return value
while not que.empty():
    result = que.get()
    print (result)