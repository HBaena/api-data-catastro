import multiprocessing

workers = 4
# workers = multiprocessing.cpu_count()*2 + 1   # Or
threads = 2
# threads = 2  # Or
worker_connections = 1000  # Or more deppending the number or simultaneously clients
# worker_class = 'gevent'
worker_class = 'eventlet'  # testing both for performance
limit_request_line = 0  # unlimited