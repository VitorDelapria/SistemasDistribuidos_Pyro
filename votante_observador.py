import Pyro5.api
import Pyro5.errors
import Pyro5.server
import sys
import time
import threading

class Votante:
    def __init__(self, votante_id, uri_lider):
        self.mensagens = []  # Armazena mensagens replicadas
        self.votante_id = votante_id
        self.log = []
        self.ultima_epoca = 0
        self.lider_uri = uri_lider  # URI do líder

    @Pyro5.api.expose
    def buscar(self, offset, epoca):
        print("Aqui0")
        # Obtém os dados do líder
        try:
            print("Aqui1")
            lider_proxy = Pyro5.api.Proxy(self.lider_uri)
            resposta = lider_proxy.fornecer_dados(offset, epoca)
            print("Aqui1")
            if resposta["erro"]:
                print("Aqui2")
                print(f"Votante {self.votante_id}: Log inconsistente. Truncando até offset {resposta['maior_offset']}.")
                self.log = self.log[:resposta["maior_offset"] + 1]
                self.ultima_epoca = resposta["maior_epoca"]
                self.buscar(resposta["maior_offset"], resposta["maior_epoca"])  # Repetir busca com dados atualizados
            else:
                print("Aqui3")
                self.log.extend(resposta["dados"])
                print(f"Votante {self.votante_id}: Log atualizado com {len(resposta['dados'])} novas entradas.")
                for i in range(len(resposta["dados"])):
                    self.confirmar(offset + i)
        except Pyro5.errors.CommunicationError as e:
            print(f"Erro ao tentar acessar o Líder para buscar dados: {e}")

    @Pyro5.api.expose
    def confirmar(self, offset):
        try:
            lider_proxy = Pyro5.api.Proxy(self.lider_uri)
            lider_proxy.receber_confirmacao(offset, self.votante_id)
            print(f"Votante {self.votante_id}: Confirmação enviada para offset {offset}.")
            self.replicar(self.log[offset]["mensagem"])
        except Pyro5.errors.CommunicationError as e:
            print(f"Erro ao enviar confirmação: {e}")

    def replicar(self, mensagem):  
        if mensagem not in self.mensagens:                          # Replica as mensagens
            print("EstouAqui3!!!")
            self.mensagens.append(mensagem)
            print(f"[Votante: {self.votante_id}] - Mensagem Replicada: {mensagem}")
        else:
            print(f"Mensagem já replicada: {mensagem}")

    @Pyro5.api.expose
    def heartbeat(self): 
        print("Heartbeat recebido do líder.")
        return True    
    
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
    
class Observador:
    def __init__(self, observador_id, lider_uri):
        self.messagens_notifications = []
        self.observador_id = observador_id
        self.uri_lider = lider_uri
        self.log = []
        self.ultima_epoca = 0

    def replicar_notificacao(self, menssage):                   # Recebe uma notificação do Líder
        if menssage not in self.messagens_notifications:
            self.messagens_notifications.append(menssage)
            print(f"[Observador] Notificação recebida: {menssage}")
        else:
            print(f"[Observador] Notificação já recebida: {menssage}")
    
    @Pyro5.api.expose
    def heartbeat(self):
        return True
    
    @Pyro5.api.expose
    def buscar(self, offset, epoca):
        # Obtém os dados do líder
        try:
            print("Aqui1")
            lider_proxy = Pyro5.api.Proxy(self.uri_lider)
            resposta = lider_proxy.fornecer_dados(offset, epoca)
            print("Aqui1")
            if resposta["erro"]:
                print("Aqui2")
                print(f"Observador {self.observador_id}: Log inconsistente. Truncando até offset {resposta['maior_offset']}.")
                self.log = self.log[:resposta["maior_offset"] + 1]
                self.ultima_epoca = resposta["maior_epoca"]
                self.buscar(resposta["maior_offset"], resposta["maior_epoca"])  # Repetir busca com dados atualizados
            else:
                print("Aqui3")
                self.log.extend(resposta["dados"])
                print(f"Observador {self.observador_id}: Log atualizado com {len(resposta['dados'])} novas entradas.")
                for i in range(len(resposta["dados"])):
                    self.confirmar(offset + i)
        except Pyro5.errors.CommunicationError as e:
            print(f"Erro ao tentar acessar o Líder para buscar dados: {e}")
        
    def confirmar(self, offset):
        try:
            lider_proxy = Pyro5.api.Proxy(self.uri_lider)
            lider_proxy.receber_confirmacao(offset, self.observador_id)
            print(f"Observador {self.observador_id}: Confirmação enviada para offset {offset}.")
            self.replicar_notificacao(self.log[offset]["mensagem"])
        except Pyro5.errors.CommunicationError as e:
            print(f"Erro ao enviar confirmação: {e}")

    @Pyro5.api.expose
    def notificado_promocao(self):
        print(f"Observador promovido a Votante!")
        self.notificar_observadores(len(self.log) - 1)

    

def iniciar_observador(id_observador,uri_lider):
    """Inicializa um Votante e o registra no servidor de nomes."""
    observador = Observador(id_observador, uri_lider)
    daemon = Pyro5.server.Daemon()  # Cria um daemon para o votante
    uri = daemon.register(observador)
    
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

def conection(role):
    daemon = Pyro5.server.Daemon()  # Criar um único daemon
    ns = Pyro5.api.locate_ns()

    # Conectar com o Líder
    try:
        uri_lider = ns.lookup("Lider_Epoca1")  # Buscando URI do líder no serviço de nomes
        lider = Pyro5.api.Proxy(uri_lider)  # Criando um proxy para o líder
        print("[Conexão] Líder encontrado com sucesso.")
    except Pyro5.errors.NamingError:
        print("[Erro] Serviço de nomes não está acessível.")
        return
    except Pyro5.errors.CommunicationError as e:
        print(f"[Erro de Comunicação] Não foi possível localizar o líder: {e}")
        return


    if role == "votante":
        print("EstouAqui!!!")
        votante_id = f"Votante_{time.time()}"  # Identificador único baseado no timestamp
        votante = Votante(votante_id, uri_lider)
        uri = daemon.register(votante)
        ns.register("Votante1", uri)
        print(f"Votante registrado com URI: {uri} - id {votante_id}")

        # Teste de Comunicação com o Líder
        try:
            votante_thread = threading.Thread(target=iniciar_votante, args=(votante_id, uri_lider), daemon=True)
            votante_thread.start()
            time.sleep(1)  # Aguarde um pouco entre os votantes
            # lider.registrar_votante(uri)  # Registrar o votante no Líder
            # print(f"Votante {votante_id} registrado no Líder com URI {uri}.")
            #print(f"Votante {uri} registrado no Líder.")
        except Pyro5.errors.CommunicationError as e:
            print(f"[Erro de Comunicação] Falha ao comunicar com o Líder: {e}")

    elif role == "observador":
        observador_id = f"Observador_{time.time()}"  # Identificador único
        observador = Observador(observador_id, uri_lider)
        uri = daemon.register(observador)
        ns.register(observador_id, uri)
        print(f"Observador registrado com URI: {uri} - id: {observador_id}")

        # Teste de Comunicação com o Líder
        try:
            observador_thread = threading.Thread(target=iniciar_observador, args=(observador_id,uri_lider,), daemon=True)
            observador_thread.start()
            time.sleep(1)
            # lider.registrar_observador(uri)  # Registrar o observador no Líder
            # print(f"Observador {uri} registrado no Líder.")
        except Pyro5.errors.CommunicationError as e:
            print(f"[Erro de Comunicação] Falha ao comunicar com o Líder: {e}")
    
    daemon.requestLoop()  # Loop aqui usa o mesmo daemon


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 script.py [votante|observador]")
    else:
        role = sys.argv[1].lower()
        conection(role)
