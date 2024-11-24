import Pyro5
import Pyro5.api
import Pyro5.core
import Pyro5.errors
import Pyro5.server
import Pyro5.client
import time


@Pyro5.server.expose
class Lider(object):
    def __init__(self):
        self.votantes = []
        self.observadores = []
        self.mensagens = []
        self.mensagens_commitadas = []

    def registrar_votante(self, uri):
        print(f"ESTOU AQUI NO VOTANTE")
        if uri not in self.votantes:
            self.votantes.append(uri)
            print(f"Votante registrado: {uri}")
            print(f"Lista atual de votantes: {self.votantes}")
            #print(f"Votante registrado teste: {self.votantes[0]}")
        else:
            print(f"Votante já registrado: {uri}")

    def registrar_observador(self, uri):
        self.observadores.append(uri)   
        print(f"Observador registrado: {uri}")

    def publicar_mensagem(self, menssage):
        if menssage not in self.mensagens:
            # for votante_uri in self.votantes:
            #     try:
            #         print(f"Mensagem Registrada: {menssage}")
            #         votante = Pyro5.api.Proxy(votante_uri)
            #         votante.replicar(menssage)
            #     except Pyro5.errors.CommunicationError as e:
            #         print(f"Erro ao enviar mensagem para o votante {votante_uri}: {e}")
            print(f"ESTOU AQUI AGORA")
            print(f"Mensagem Registrada: {menssage}")
            self.mensagens.append(menssage)
            self.replicar_para_votante(menssage)
            self.notificar_observadores(menssage)
        else:
           print(f"Mensagem já registrada: {menssage}")

    def replicar_para_votante(self, menssage):
        print("Estou Aqui2!")
        print(f"Votantes registrados: {self.votantes}")  # Verificando se os votantes estão registrados
        for uri in self.votantes:
            try:
                print("Estou Aqui!")
                print(f"votante uri: {uri}")
                votante = Pyro5.api.Proxy(uri)
                votante.replicar(menssage)
                print(f"Mensagem replicada para: {uri}")
            except Pyro5.errors.CommunicationError as e:
                print(f"Falha ao replicar para: {uri}. Erro: {e}")
                self.remover_votante(uri)

    def notificar_observadores(self, menssage):
        for uri in self.observadores:
            try:
                observador = Pyro5.api.Proxy(uri)
                observador.replicar_notificacao(menssage)
                print(f"Notificação enviada para: {uri}")
            except Pyro5.errors.CommunicationError:
                print(f"Falha ao notificar {uri}. Observador pode estar offline. ")

    def enviar_heartbeat(self):
        print("EstouAQUI6!")
        for uri in self.votantes:
            try:
                votante = Pyro5.api.Proxy(uri)
                if uri == self.votantes[0]:
                    raise Pyro5.errors.CommunicationError("Falha programática para teste.")
                if not votante.heartbeat():
                    raise Pyro5.errors.CommunicationError("Heartbeat não reconhecido.")  # Forçar falha                    
                print(f"Heartbeat enviado e reconhecido por {uri}")
            except Pyro5.errors.CommunicationError:
                print(f"Falha no Heartbeat com {uri}. Considerando votante como falho. ")
                self.remover_votante(uri)
                self.promover_observador()

    def remover_votante(self, uri):
        if uri in self.votantes:
            self.votantes.remove(uri)
            print(f"Votante {uri} removido por falha.")
    
    def promover_observador(self):
        if self.observadores:
            novo_votante_uri = self.observadores.pop(0) 
            self.votantes.append(novo_votante_uri)
            print(f"Observador promovido a votante: {novo_votante_uri}")
        else:
            print("Nenhum observador disponível para promoção. ")

    def commit_mensagem(self, mensagem): # marca mensagem no indice fornecido como comitada
        if mensagem not in self.mensagens:
            self.mensagens_commitadas.append(mensagem)
            print(f"Mensagem comitada: {mensagem}")
        else:
            print(f"Mensagem já foi commitada: {mensagem}")

    def obter_mensagens_commitadas(self): # Retorna todas as mensagens que foram commitadas.
        return self.mensagens_commitadas    
                
def conection(): 
    lider = Lider()
    daemon = Pyro5.server.Daemon()
    uri = daemon.register(lider)
    try: 
        servidor_nomes = Pyro5.api.locate_ns()
        servidor_nomes.register("Lider_Epoca1", uri)
        print(f"Líder registado com uri: {uri}")
    except Pyro5.errors.NamingError as e:
        print(f"Erro ao registrar no servidor de nomes: {e}")
    daemon.requestLoop()

if __name__ == "__main__":
    conection()