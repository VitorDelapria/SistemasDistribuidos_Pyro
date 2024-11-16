import Pyro5.api
import Pyro5.server
import sys

@Pyro5.api.expose
class Votante:
    def __init__(self):
        self.mensagens = []  # Armazena mensagens replicadas

    def replicar(self, mensagem):                            # Replica as mensagens
        print("EstouAqui2!!!")
        self.mensagens.append(mensagem)
        print(f"Mensagem replicada: {mensagem}")

    def heartbeat(self):                                     
        print("Heartbeat recebido do líder.")
        return True
    

@Pyro5.server.expose
class Observador:
    def __init__(self):
        self.messagens_notifications = []

    def replicar_notificacao(self, menssage):                   # Recebe uma notificação do Líder
        self.messagens_notifications.append(menssage)
        print(f"[Observador] Notificação recebida: {menssage}")

def conection(role):
    daemon = Pyro5.server.Daemon()  # Criar um único daemon
    ns = Pyro5.api.locate_ns()

    # Conectar com o Líder
    try:
        uri_lider = ns.lookup("Lider_Epoca1")  # Buscando URI do líder no serviço de nomes
        lider = Pyro5.api.Proxy(uri_lider)  # Criando um proxy para o líder
        print("[Conexão] Líder encontrado com sucesso.")
    except Pyro5.errors.CommunicationError as e:
        print(f"[Erro de Comunicação] Não foi possível localizar o líder: {e}")
        return


    if role == "votante":
        print("EstouAqui!!!")
        votante = Votante()
        uri = daemon.register(votante)
        ns.register("Votante1", uri)
        print(f"Votante registrado com URI: {uri}")

        # Teste de Comunicação com o Líder
        try:
            lider.registrar_votante(uri)  # Registrar o votante no Líder
            print(f"Votante {uri} registrado no Líder.")
            #lider.publicar_mensagem("Mensagem de teste do Votante")  # Enviar mensagem ao Líder
        except Pyro5.errors.CommunicationError as e:
            print(f"[Erro de Comunicação] Falha ao comunicar com o Líder: {e}")

    elif role == "observador":
        observador = Observador()
        uri = daemon.register(observador)
        ns.register("Observador1", uri)
        print(f"Observador registrado com URI: {uri}")

        # Teste de Comunicação com o Líder
        try:
            lider.registrar_observador(uri)  # Registrar o observador no Líder
            print(f"Observador {uri} registrado no Líder.")
            #lider.publicar_mensagem("Mensagem de teste do Observador")  # Enviar mensagem ao Líder
        except Pyro5.errors.CommunicationError as e:
            print(f"[Erro de Comunicação] Falha ao comunicar com o Líder: {e}")
    
    daemon.requestLoop()  # Loop aqui usa o mesmo daemon


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 script.py [votante|observador]")
    else:
        role = sys.argv[1].lower()
        conection(role)
