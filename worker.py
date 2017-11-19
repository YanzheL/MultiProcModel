import abc
import logging

import six

from pooling import SingletonProcessPool
from work_procedure import *


@six.add_metaclass(abc.ABCMeta)
class Worker:
    global_running = True

    @classmethod
    def from_name(cls, name, **kwargs):
        for sub_cls in cls.__subclasses__():
            if name.lower() + "worker" == sub_cls.__name__.lower():
                return sub_cls(**kwargs)

    def __init__(self, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.logger = logging.getLogger(__name__)
        self.running = True

    def run(self):
        try:
            while self.global_running and self.running:
                in_data = self.input_queue.get()
                ret = self.process(in_data)
                self.output_queue.put(ret)
        except Exception as e:
            self.exception_handler(e)
        finally:
            self.stop()

    def stop(self, all_instance=False):
        if all_instance:
            self.global_running = False
        else:
            self.running = False

        self._clean_up()

    @abc.abstractmethod
    def process(self, data):
        return data

    def exception_handler(self, exc):
        self.logger.error("Unexpected exception %s, now stopping..." % exc)

    def _clean_up(self):
        pass


class VideoWorker(Worker):
    def __init__(self, input_queue, output_queue, use_multiprocess=False):
        super(VideoWorker, self).__init__(input_queue, output_queue)
        self.use_multiproc = use_multiprocess
        if self.use_multiproc:
            self.process_pool = SingletonProcessPool()

    def process(self, data):
        return (data[0], self.process_pool.apply(analysis, (data[1],)),) if self.use_multiproc else (
            data[0], analysis(data[1]))

    def exception_handler(self, exc):
        if isinstance(exc, KeyboardInterrupt):
            self.logger.info("Detected keyboard interrupt, %s is performing gracefully shut down..." % __name__)
        else:
            self.logger.error("Unexpected exception %s" % exc)

    def _clean_up(self):
        if self.use_multiproc:
            self.process_pool.close()
