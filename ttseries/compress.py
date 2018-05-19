# encoding:utf-8
import abc


class BaseCompress(abc.ABCMeta):

    @abc.abstractmethod
    def compress(self, obj, *args, **kwargs):
        pass

    @abc.abstractmethod
    def decompress(self, obj, *args, **kwargs):
        pass


class Lz4Compress(BaseCompress):
    """
    """

    def compress(self, obj, *args, **kwargs):
        pass

    def decompress(self, obj, *args, **kwargs):
        pass
