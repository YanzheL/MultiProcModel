import logging
from threading import Thread, stack_size
from multiprocessing import Pool


class ThreadPool:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.workers = {}
        stack_size(65536)

    def add_worker(self, worker_instance, method='run', args=(), name=None, nworkers=1, daemon=True, kwargs=None):
        func = getattr(worker_instance, method)
        worker_info = self.workers.get(worker_instance.__class__, None)

        if worker_info:
            worker_info['nworkers'] += nworkers
            worker_info['threads'].extend(
                ThreadPool._make_threads_list(self, worker_instance, nworkers, target=func, args=args, daemon=daemon,
                                              name=name, kwargs=kwargs)
            )

        else:
            self.workers[worker_instance.__class__] = {
                'nworkers': nworkers,
                'threads': ThreadPool._make_threads_list(self, worker_instance, nworkers, target=func, args=args,
                                                         daemon=daemon, name=name, kwargs=kwargs)
            }

    def start(self, all_start=True, worker_cls=None, **kwargs):
        if all_start:
            for worker_class in self.workers:
                self._start_worker_threads(worker_class)
        elif worker_cls:
            self._start_worker_threads(worker_cls)

    def stop(self, all_start=True, worker_cls=None, **kwargs):
        if all_start:
            for worker_class in self.workers:
                self._stop_worker_threads(worker_class)
        elif worker_cls:
            self._stop_worker_threads(worker_cls)

    def _make_threads_list(self, worker_instance, nworkers, **thread_params):
        self.logger.info("Made %d threads for worker class %s" % (nworkers, worker_instance.__class__))
        return [(Thread(**thread_params), worker_instance) for i in range(nworkers)]

    def _start_worker_threads(self, worker_class):
        worker_info = self.workers.get(worker_class, None)
        if worker_info:
            for th in worker_info['threads']:
                th[0].start()
        else:
            self.logger.error('Cannot find specific thread class %s, skipped' % worker_class)

    def _stop_worker_threads(self, worker_class, all_instance=True):
        worker_info = self.workers.get(worker_class, None)
        if worker_info:
            for th in worker_info['threads']:
                if all_instance:
                    th[1].stop(all_instance=True)
                    break
                    # TODO: implement stop thread based on worker number rather than stop all
        else:
            self.logger.error('Cannot find specific thread class %s, skipped' % worker_class)


class SingletonProcessPool:
    _pool = None

    def __init__(self, **kwargs):
        self.pool = Pool(**kwargs)

    def apply(self, func, args=(), kwds={}):
        return self.pool.apply(func, args, kwds)

    def close(self):
        self.pool.close()

    @property
    def pool(self):
        assert self._pool, "Processes pool not initialized"
        return self._pool

    @pool.setter
    def pool(self, pool):
        if not self._pool:
            self._pool = pool
