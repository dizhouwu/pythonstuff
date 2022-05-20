class SignalHandlerLockMixin:
    def _register_lock_signal_handler(self):
        signal.signal(signal.SIGINT, self._release_lock)
        signal.signal(signal.SIGTERM, self._release_lock)

    def _release_lock(self, signum, frame):
        _logger.info(f"Handling {signum}, release lock")
        try:
            self.redis_lock.release_lock()
        except:
            pass

import concurrent.futures
import signal
import os
import time
def run_process():
    print(f"Child process: {os.getpid()}")
    def handler(signum, frame):
        print(f'Signal handler called with signal {signum} for process :{os.getpid()}\n')
    signal.signal(signal.SIGINT, handler)
    time.sleep(10)


def handler(signum, frame):
    print(f'Signal handler called with signal {signum} for process :{os.getpid()}\n')


signal.signal(signal.SIGINT, handler)


with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:

    print(f"main process: {os.getpid()}")
    for i in range(2):
        executor.submit(run_process)
    print(f"all process submitted")


