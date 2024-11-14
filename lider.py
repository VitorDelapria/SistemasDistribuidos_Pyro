import Pyro5
import Pyro5.core
import Pyro5.server
import Pyro5.client

@Pyro5.server.expose
class Lider(object):
    # ... methods that can be called go here...
    pass

daemon = Pyro5.server.Daemon()

uri = daemon.register(Lider)
servidor_nomes = Pyro5.core.locate_ns()
servidor_nomes.register("Lider_Epoca1", uri)

daemon.requestLoop()