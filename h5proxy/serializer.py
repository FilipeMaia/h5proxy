import numpy
import h5py
import cPickle as pickle
import zmq
import pdb

class Serializer(object):
    def __init__(self, parent, socket):
        self._socket = socket
        self._parent = parent

    def recv(self):
        data = pickle.loads(self._socket.recv())
        ret = self._deserialize(data)
        return ret
        

    def _deserialize(self, data):
        if(isinstance(data, dict)):
            if('className' in data):
                if(data['className'] == "Dataset"):
                    data = Dataset(self._parent, data['fileName'], data['path'])
                elif(data['className'] == "Group"):
                    data = Group(self._parent, data['fileName'], data['path'])
                elif(data['className'] == "Attributes"):
                    data = Attributes(self._parent, data['fileName'], data['path'])
                elif(data['className'] == "exception"):
                    exc_type = data['exc_type']
                    exc_value = data['exc_value']
                    raise exc_type(exc_value)
                elif(data['className'] == "ndarray"):
                    d = self._socket.recv()
                    data = numpy.frombuffer(buffer(d), dtype=data['dtype']).reshape(data['shape'])
                elif(data['className'] == "File"):
                    pass
                else:
                    raise RuntimeError('Unknown class: %s' % data['className'])
            else:
                # We need to sort to be able to receive any possible arrays
                # in the correct order
                for k in sorted(data.keys()):
                    data[k] = self._deserialize(data[k])
                    
        return data


    def send(self,data):
        data, arrays = self._serialize(data, [])

        flags = 0
        if(len(arrays)):
            flags = zmq.SNDMORE
        self._socket.send(pickle.dumps(data), flags)

        for i in range(len(arrays)):
            # When sending the last array change the flag back
            if(i == len(arrays) -1):
                flags = 0
            self._socket.send(arrays[i], flags)            

    def _serialize(self, data, arrays):
        if type(data) is h5py.Dataset:
            data = dict(
                className = "Dataset",
                fileName = data.file.filename,
                path = data.name
            )
        elif type(data) is h5py.Group:
            data = dict(
                className = "Group",
                fileName = data.file.filename,
                path = data.name
            )
        elif type(data) is h5py.AttributeManager:
            data = dict(
                className = "Attributes",
            )
        elif type(data) is h5py.File:
            data = dict(
                className = "File",
                fileName = data.file.filename,
                path = "/"
            )
        elif isinstance(data, numpy.ndarray):
            arrays.append(data)
            data = dict(
                className = "ndarray",
                dtype = data.dtype,
                shape = data.shape
            )
        elif isinstance(data, dict):
            # We need to sort to be able to receive any possible arrays
            # in the correct order
            for k in sorted(data.keys()):
                data[k], arrays = self._serialize(data[k], arrays)
        return data, arrays

from .h5proxy import Dataset,Group,File,Attributes 
