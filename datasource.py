import cv2
import six
import abc


@six.add_metaclass(abc.ABCMeta)
class DataSource:
    @abc.abstractmethod
    def get(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @property
    @abc.abstractmethod
    def source(self):
        pass

    @source.setter
    @abc.abstractmethod
    def source(self, source):
        pass


class VideoDataSource(DataSource):
    _source = None

    def __init__(self, video_source=0):
        self.source = cv2.VideoCapture(video_source)

    def get(self):
        return self.source.read()

    def close(self):
        self.source.release()
        cv2.destroyAllWindows()

    @property
    def source(self):
        assert self._source, "DataSource not initialized"
        return self._source

    @source.setter
    def source(self, source):
        if not self._source:
            self._source = source
