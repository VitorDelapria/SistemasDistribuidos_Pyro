import Pyro5
import Pyro5.client
import Pyro5.core

nameserver = Pyro5.core.locate_ns(host=None, port=None, broadcast=True)
print(nameserver)
uri = nameserver.lookup("Lider_Epoca1")
print(uri)
proxy = Pyro5.client.Proxy(uri)
print(proxy)