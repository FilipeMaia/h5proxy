import h5py
import zmq
import numpy
import cPickle as pickle
import pdb
import sys


class Server(object):
    def __init__(self, interface="*", port=30572, heartbeat=None):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REP)
        self._socket.bind("tcp://"+interface+":"+str(port))
        self._heartbeat = heartbeat
        # maximum interval between hearbeats
        # corresponds to the socket timeout interval in miliseconds
        self._hbInterval = 10000
        if(self._heartbeat):            
            self._socket.set(zmq.RCVTIMEO, self._hbInterval)
        self.files = {}
        self._ser = Serializer(self, self._socket)

    def start(self):    
        print "Starting server"
        while(True):
            #  Wait for next request from client
            try:
                fc = self._ser.recv()
            except zmq.error.Again:
                raise RuntimeError('Did not receive heartbeat message in time. Aborting...')
                
            self.handleRPC(fc)


    def handleRPC(self, fc):
        # unpickle what's necessary
        try:
            if(fc['func'] == 'file_init'):            
                self._ser.send(self.file_init(fc['fileName'], fc['mode'], fc['driver'], fc['libver'], fc['userblock_size'], **fc['kwds']))
            if(fc['func'] == 'keys'):            
                self._ser.send(self.keys(fc['fileName'], fc['path'], fc['attrs']))
            if(fc['func'] == 'getitem'):            
                self._ser.send(self.getitem(fc['fileName'], fc['path'], fc['fargs'], fc['attrs']))
            if(fc['func'] == 'setitem'):            
                self._ser.send(self.setitem(fc['fileName'], fc['path'], fc['fargs'], fc['vals'], fc['attrs']))
            if(fc['func'] == 'shape'):            
                self._ser.send(self.shape(fc['fileName'], fc['path']))
            if(fc['func'] == 'attrs'):            
                self._ser.send(self.attrs(fc['fileName'], fc['path']))
            if(fc['func'] == 'dtype'):            
                self._ser.send(self.dtype(fc['fileName'], fc['path']))
            if(fc['func'] == 'len'):            
                self._ser.send(self.len(fc['fileName'], fc['path'], fc['attrs']))
            if(fc['func'] == 'close'):            
                self._ser.send(self.fileClose(fc['fileName']))
            if(fc['func'] == 'array'):            
                self._ser.send(self.array(fc['fileName'], fc['path'], fc['dtype']))
            if(fc['func'] == 'create_dataset'):            
                self._ser.send(self.create_dataset(fc['fileName'], fc['path'], fc['name'], fc['shape'], fc['dtype'], fc['data'], **fc['kwds']))
            if(fc['func'] == 'create_group'):            
                self._ser.send(self.create_group(fc['fileName'], fc['path'], fc['groupName']))
#        except EnvironmentError:
        except:
            ret = dict()
            ret['className'] = 'exception'
            ret['exc_type'] = sys.exc_type
            ret['exc_value'] = sys.exc_value
            self._ser.send(ret)
                
            

    def file_init(self, fileName,mode,driver,libver,userblock_size,**kwds):
        f = h5py.File(fileName,mode,driver,libver,userblock_size,**kwds)
        self.files[f.file.filename] = f
        return f

    def create_dataset(self, fileName, path, name, shape, dtype, data, **kwds):
        return self.files[fileName][path].create_dataset(name,shape,dtype,data,**kwds)

    def create_group(self, fileName, path, groupName):
        return self.files[fileName][path].create_group(groupName)

    def fileClose(self, fileName):
        return self.files[fileName].close()

    def getitem(self, fileName, path, args, attrs):                
        if(attrs):
            return self.files[fileName][path].attrs[args]
        else:
            return self.files[fileName][path][args]

    def array(self, fileName, path, dtype):
        return numpy.array(self.files[fileName][path], dtype = dtype)

    def setitem(self, fileName, path, args, vals, attrs):
        if(attrs):
            self.files[fileName][path].attrs[args] = vals
        else:
            self.files[fileName][path][args] = vals

    def keys(self, fileName, path, attrs):
        if(attrs):
            return self.files[fileName][path].attrs.keys()
        else:
            return self.files[fileName][path].keys()
        
    def dtype(self,fileName,path):
        return self.files[fileName][path].dtype

    def shape(self,fileName,path):
        return self.files[fileName][path].shape

    def attrs(self,fileName,path):
        return self.files[fileName][path].attrs

    def len(self,fileName,path,attrs):
        if(attrs):
            return len(self.files[fileName][path].attrs)
        else:
            return len(self.files[fileName][path])


from .h5proxy import Dataset,Group,File,Attributes 
from .serializer import Serializer
