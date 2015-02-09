from h5proxy import Group, Dataset, File

def startServer():
    from h5proxy import Server
    server = Server()
    server.start()

