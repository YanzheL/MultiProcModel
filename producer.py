import cv2
import logging
import six
import abc
from datasource import VideoDataSource


@six.add_metaclass(abc.ABCMeta)
class Producer:
    global_running = True

    @classmethod
    def from_name(cls, name, **kwargs):
        for sub_cls in cls.__subclasses__():
            if name.lower() + "producer" == sub_cls.__name__.lower():
                return sub_cls(**kwargs)

    def __init__(self, output_queue, datasource):
        self.datasource = datasource
        self.output_queue = output_queue
        self.logger = logging.getLogger(__name__)
        self.running = True

    def run(self):
        try:
            while self.global_running and self.running:
                data = self.datasource.get()
                ret = self.process(data)
                self.output_queue.put(ret)
        except Exception as e:
            self.exception_handler(e)
        finally:
            self.stop()

    @abc.abstractmethod
    def process(self, data):
        return data

    def stop(self, all_instance=False):
        if all_instance:
            self.global_running = False
        else:
            self.running = False
        self._clean_up()

    def exception_handler(self, exc):
        self.logger.error("Unexpected exception %s, now stopping..." % exc)

    def _clean_up(self):
        self.datasource.close()


class VideoProducer(Producer):
    sequence = 0

    def __init__(self, output_queue, video_source=0):
        super(VideoProducer, self).__init__(output_queue, VideoDataSource(video_source))

    def process(self, data):
        img = data[1]
        # cv2.resize(img, (1024, 768))
        ret = (self.sequence, img)
        self.sequence += 1
        return ret

    def exception_handler(self, exc):
        if isinstance(exc, KeyboardInterrupt):
            self.logger.info("Detected keyboard interrupt, %s is performing gracefully shut down..." % __name__)
        else:
            self.logger.error("Unexpected exception %s" % exc)
