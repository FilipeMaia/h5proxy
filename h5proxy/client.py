import zmq
import threading

class Client(object):
    def __init__(self, host, port):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REQ)
        self._ser = Serializer(self, self._socket)
        self._socket.connect("tcp://"+host+":"+str(port))
        self.lock = threading.Lock()
        
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
        with self.lock:
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
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def create_group(self, fileName, path, groupName):
        args = dict(
            func = "create_group",
            fileName = fileName,
            path = path,
            groupName = groupName
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def close(self, fileName):
        args = dict(
            func = "close",
            fileName = fileName
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def keys(self, fileName, path, attrs = False):
        args = dict(
            func = "keys",
            fileName = fileName,
            path = path,
            attrs = attrs
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def getitem(self, fileName, path, fargs, attrs = False):
        args = dict(
            func = "getitem",
            fileName = fileName,
            path = path,
            args = fargs,
            attrs = attrs
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def setitem(self, fileName, path, fargs, vals, attrs = False):
        args = dict(
            func = "setitem",
            fileName = fileName,
            path = path,
            args = fargs,
            vals = vals,
            attrs = attrs
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def shape(self, fileName, path):
        args = dict(
            func = "shape",
            fileName = fileName,
            path = path
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def len(self, fileName, path, attrs = False):
        args = dict(
            func = "len",
            fileName = fileName,
            path = path,
            attrs = attrs
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def repr(self, fileName, path, attrs = False):
        args = dict(
            func = "repr",
            fileName = fileName,
            path = path,
            attrs = attrs
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def dtype(self, fileName, path):
        args = dict(
            func = "dtype",
            fileName = fileName,
            path = path
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def attrs(self, fileName, path):
        args = dict(
            func = "attrs",
            fileName = fileName,
            path = path
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def array(self, fileName, path, dtype):
        args = dict(
            func = "array",
            fileName = fileName,
            path = path,
            dtype = dtype
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def mode(self, fileName):
        args = dict(
            func = "mode",
            fileName = fileName
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def contains(self, fileName, path, name):
        args = dict(
            func = "contains",
            fileName = fileName,
            path = path,
            name = name
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def values(self, fileName, path):
        args = dict(
            func = "values",
            fileName = fileName,
            path = path
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def items(self, fileName, path, attrs = False):
        args = dict(
            func = "items",
            fileName = fileName,
            path = path,
            attrs = attrs
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()

    def get(self, fileName, path, name, default=None, getclass=False, getlink=False, attrs = False):
        args = dict(
            func = "get",
            fileName = fileName,
            path = path,
            name = name,
            default = default,
            getclass = getclass,
            getlink = getlink,
            attrs = attrs
        )
        with self.lock:
            self._ser.send(args)
            return self._ser.recv()


from .h5proxy import Dataset,Group,File,Attributes        
from .serializer import Serializer
