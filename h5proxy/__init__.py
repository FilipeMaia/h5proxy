# By having the imports inside the function we only ask for the modules that we really need

def startServer():
    import h5proxy
    server = h5proxy.Server()
    server.start()

def File(locator, mode=None, driver=None, libver=None, userblock_size=None, **kwds):
    if(locator.find(':') != -1):
        import h5proxy
        return h5proxy.File(locator, mode=None, driver=None, libver=None, userblock_size=None, **kwds)
    else:
        import h5py
        return h5py.File(locator,mode,driver,libver,userblock_size,**kwds)

