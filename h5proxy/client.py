import h5py
import zmq
import numpy
import cPickle as pickle
import pdb

class Client(object):
    def __init__(self, host, port):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REQ)
        self._ser = Serializer(self, self._socket)
        self._socket.connect("tcp://"+host+":"+str(port))

        
    def file_init(self, fileName,mode,driver,libver,userblock_size,**kwds):
        args = dict(
            func = "file_init",
            fileName = fileName,
            mode = mode,
            driver = driver,
            libver = libver,
            userblock_size = userblock_size,
            kwds = kwds
        )
        self._ser.send(args)
        return self._ser.recv()

    def create_dataset(self, fileName, path, name, shape, dtype, data, **kwds):
        args = dict(
            func = "create_dataset",
            fileName = fileName,
            path = path,
            name = name,
            shape = shape,
            dtype = dtype,
            data = data,
            kwds = kwds
        )
        self._ser.send(args)
        return self._ser.recv()

    def create_group(self, fileName, path, groupName):
        args = dict(
            func = "create_group",
            fileName = fileName,
            path = path,
            groupName = groupName
        )
        self._ser.send(args)
        return self._ser.recv()

    def close(self, fileName):
        args = dict(
            func = "close",
            fileName = fileName
        )
        self._ser.send(args)
        return self._ser.recv()

    def keys(self, fileName, path, attrs = False):
        args = dict(
            func = "keys",
            fileName = fileName,
            path = path,
            attrs = attrs
        )
        self._ser.send(args)
        return self._ser.recv()

    def getitem(self, fileName, path, fargs, attrs = False):
        args = dict(
            func = "getitem",
            fileName = fileName,
            path = path,
            fargs = fargs,
            attrs = attrs
        )
        self._ser.send(args)
        return self._ser.recv()

    def setitem(self, fileName, path, fargs, vals, attrs = False):
        args = dict(
            func = "setitem",
            fileName = fileName,
            path = path,
            fargs = fargs,
            vals = vals,
            attrs = attrs
        )
        self._ser.send(args)
        return self._ser.recv()

    def shape(self, fileName, path):
        args = dict(
            func = "shape",
            fileName = fileName,
            path = path
        )
        self._ser.send(args)
        return self._ser.recv()

    def len(self, fileName, path, attrs = False):
        args = dict(
            func = "len",
            fileName = fileName,
            path = path,
            attrs = attrs
        )
        self._ser.send(args)
        return self._ser.recv()

    def dtype(self, fileName, path):
        args = dict(
            func = "dtype",
            fileName = fileName,
            path = path
        )
        self._ser.send(args)
        return self._ser.recv()

    def attrs(self, fileName, path):
        args = dict(
            func = "attrs",
            fileName = fileName,
            path = path
        )
        self._ser.send(args)
        return self._ser.recv()

    def array(self, fileName, path, dtype):
        args = dict(
            func = "array",
            fileName = fileName,
            path = path,
            dtype = dtype
        )
        self._ser.send(args)
        return self._ser.recv()

from .h5proxy import Dataset,Group,File,Attributes        
from .serializer import Serializer
