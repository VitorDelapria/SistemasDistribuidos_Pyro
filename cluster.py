import Pyro5.api
import threading
import time
from lider import Lider
from votante_observador import Votante
from votante_observador import Observador
from publicador import Publicador  # Importando a classe Publicador

class Cluster:
    def __init__(self, uri_lider):
        self.lider = Pyro5.api.Proxy(uri_lider)
        self.publicador = Publicador(uri_lider)

    def enviar_mensagem(self, mensagem):
        try:
            print(f"Enviando mensagem para o líder: {mensagem}")
            self.publicador.enviar_mensagem(mensagem)
            print(f"Mensagem enviada para o Líder: {mensagem}")
        except Pyro5.errors.CommunicationError as e:
            print(f"Erro ao enviar mensagem para o Líder: {e}")

def iniciar_lider():
    """Inicia o Líder e registra no servidor de nomes."""
    lider = Lider(max_falhas=1)
    daemon = Pyro5.server.Daemon()  # Cria um daemon para o líder
    uri = daemon.register(lider)
    
    try:
        servidor_nomes = Pyro5.api.locate_ns()
        servidor_nomes.register("Lider_Epoca1", uri)
        print(f"Líder registrado com URI: {uri}")
        threading.Thread(target=send_heartbeat, args=(lider,), daemon=True).start()
    except Pyro5.errors.NamingError as e:
        print(f"Erro ao registrar no servidor de nomes: {e}")
    
    daemon.requestLoop()  # Mantém o líder ativo para receber mensagens

def send_heartbeat(lider):
    """Envia heartbeats periodicamente para os votantes registrados."""
    while True:
        print("Enviando heartbeat para os votantes...")
        lider.enviar_heartbeat()
        time.sleep(5)

def iniciar_votante(votante_id, uri_lider):
    """Inicializa um Votante e o registra no servidor de nomes."""
    votante = Votante(votante_id, uri_lider)
    daemon = Pyro5.server.Daemon()  # Cria um daemon para o votante
    uri = daemon.register(votante)
    
    try:
        servidor_nomes = Pyro5.api.locate_ns()
        servidor_nomes.register(f"Votante_{votante_id}", uri)
        print(f"Votante {votante_id} registrado com URI: {uri}")

        # Cria um novo proxy para o líder em cada thread
        lider_proxy = Pyro5.api.Proxy(uri_lider)
        lider_proxy.registrar_votante(uri)  # Registra o votante no líder
        print(f"Votante {votante_id} registrado no lider")
    except Pyro5.errors.NamingError as e:
        print(f"Erro ao registrar votante {votante_id} no servidor de nomes: {e}")
    
    daemon.requestLoop()  # Mantém o votante ativo para interagir com o líder

def iniciar_observador(uri_lider):
    """Inicializa um Votante e o registra no servidor de nomes."""
    votante = Observador()
    daemon = Pyro5.server.Daemon()  # Cria um daemon para o votante
    uri = daemon.register(votante)
    
    try:
        servidor_nomes = Pyro5.api.locate_ns()
        servidor_nomes.register(f"Observador", uri)
        print(f"Observador registrado com URI: {uri}")

        # Cria um novo proxy para o líder em cada thread
        lider_proxy = Pyro5.api.Proxy(uri_lider)
        lider_proxy.registrar_observador(uri)  # Registra o votante no líder
        print(f"Observador registrado no lider")
    except Pyro5.errors.NamingError as e:
        print(f"Erro ao registrar observador no servidor de nomes: {e}")
    
    daemon.requestLoop()  # Mantém o votante ativo para interagir com o líder


def main():
    # Inicia o Líder em uma thread separada
    lider_thread = threading.Thread(target=iniciar_lider, daemon=True)
    lider_thread.start()
    time.sleep(2)  # Aguarde um pouco para garantir que o líder seja registrado


    # Localiza o líder no servidor de nomes e envia uma mensagem
    try:
        ns = Pyro5.api.locate_ns()
        uri_lider = ns.lookup("Lider_Epoca1")
       # lider = Pyro5.api.Proxy(uri_lider)  # Criando um proxy para o líder
        print(f"Líder encontrado com URI: {uri_lider}")
        cluster = Cluster(uri_lider)
        
        # Inicia os Votantes (em paralelo)
        for i in range(1, 4):  # Inicia 3 votantes
            votante_thread = threading.Thread(target=iniciar_votante, args=(i, uri_lider), daemon=True)
            votante_thread.start()
            time.sleep(1)  # Aguarde um pouco entre os votantes

        observador_thread = threading.Thread(target=iniciar_observador, args=(uri_lider,), daemon=True)
        observador_thread.start()
        time.sleep(1)

        # Envia uma mensagem de teste
        #cluster.enviar_mensagem("MENSAGEM1234!!!.")

    except Pyro5.errors.NamingError as e:
        print(f"Erro ao localizar o servidor de nomes ou líder: {e}")
    except Pyro5.errors.CommunicationError as e:
        print(f"Erro de comunicação com o líder: {e}")

    # Manter o processo principal ativo
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
