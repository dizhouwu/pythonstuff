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
