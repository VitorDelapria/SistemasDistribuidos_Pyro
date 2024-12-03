import Pyro5
import Pyro5.api
import Pyro5.core
import Pyro5.errors
import Pyro5.server
import Pyro5.client
import time
import threading

class Lider(object):
    def __init__(self):
        self.votantes = []
        self.observadores = []
        self.mensagens = []
        self.log = [] # Log normal
        self.log_commitadas = []
        self.confirmacoes = {} # confirmações recebidas dos votantes
        self.quorum = 5 # tamanho do quorum
        self.falhas = {}

    @Pyro5.api.expose
    def registrar_votante(self, uri):
        if uri not in self.votantes:
            self.votantes.append(uri)
            print(f"Votante registrado: {uri}")
        else:
            print(f"Votante já registrado: {uri}")

    @Pyro5.api.expose
    def registrar_observador(self, uri):
        self.observadores.append(uri)   
        print(f"Observador registrado: {uri}")

    @Pyro5.api.expose
    def publicar_mensagem(self, mensagem):
        #print(f"ESTOU EM PUBLICAR MENSAGEM!")
        if mensagem not in [entry["mensagem"] for entry in self.log]:
            nova_entrada = {"mensagem": mensagem, "confirmado": False, "epoca": len(self.log)}
            self.log.append(nova_entrada)
            print(f"Mensagem gravada: {mensagem}")
            self.replicar_para_votante(len(self.log) - 1)
        else:
            print(f"Mensagem já registrada: {mensagem}")


    def replicar_para_votante(self, offset):
        for uri in self.votantes:
            try:
                votante = Pyro5.api.Proxy(uri)
                print(f"Notificado votante {uri} sobre novo log no offset {offset}.")
                votante.buscar(offset, len(self.log))
            except Pyro5.errors.CommunicationError as e:
                print(f"Falha ao notificar votante {uri}: {e}")

    @Pyro5.api.expose
    def fornecer_dados(self, offset, epoca):    
        # Retorna os dados de log a partir do offset e epoca
        dados = {"erro": False, "dados": [], "maior_offset": len(self.log) - 1, "maior_epoca": epoca}
        
        # Verifica se o offset solicitado é maior que o tamanho do log
        if offset >= len(self.log):
            dados["erro"] = True
            dados["maior_offset"] = len(self.log) - 1
            dados["maior_epoca"] = epoca
        else:
            dados["dados"] = self.log[offset:]
        
        return dados
    
    @Pyro5.api.expose
    def receber_confirmacao(self, offset, votante_uri, uri_lider):
        if offset not in self.confirmacoes:
            self.confirmacoes[offset] = set()
        self.confirmacoes[offset].add(votante_uri)
        self.confirmacoes[offset].add(uri_lider)

        if len(self.confirmacoes[offset]) >= (self.quorum // 2) + 1:
            self.log[offset]["confirmado"] = True
            print(f"Quorum atingido para a mensagem no offset {offset}.")
            self.commit_mensagem()

    @Pyro5.api.expose
    def notificado_observadores(self, offset):
        for uri in self.observadores:
            try:
                observador = Pyro5.api.Proxy(uri)
                print(f"Notificado Observador {uri} sobre novo log no offset {offset}.")
                observador.buscar(offset, len(self.log))
            except Pyro5.errors.CommunicationError:
                print(f"Falha ao notificar {uri}. Observador pode estar offline. ")

    def enviar_heartbeat(self):
        while True:
            print("Enviando Heartbeats...")
            #print(f"votantes - {self.votantes}")
            for uri in self.votantes:
                try:
                    votante = Pyro5.api.Proxy(uri)
                    if not votante.heartbeat():
                        print(f"matou votante!!!")
                        raise Pyro5.errors.CommunicationError("Heartbeat não reconhecido.")
                    self.falhas[uri] = 0 
                except Pyro5.errors.CommunicationError:
                    print(f"Falha no Heartbeat com {uri}. Incrementando contador de falhas.")
                    self.falhas[uri] = self.falhas.get(uri, 0) + 1
                    if self.falhas[uri] >= (self.quorum // 2) + 1:
                        print(f"Votante {uri} atingiu o limite de falhas. Será removido.")
                        self.remover_votante(uri)
                        self.promover_observador()
            time.sleep(5)

    def remover_votante(self, uri):
        if uri in self.votantes:
            self.votantes.remove(uri)
            print(f"Votante {uri} removido por falha.")
    
    def promover_observador(self):
        if self.observadores:
            novo_votante_uri = self.observadores.pop(0) 
            self.votantes.append(novo_votante_uri)
            print(f"Observador promovido a votante: {novo_votante_uri}")
            try:
                observador = Pyro5.api.Proxy(novo_votante_uri)
                print(f"PROMOVENDO OBSERVADOR!")
                observador.notificado_promocao()
            except:
                print(f"Erro ao notificar promoção de {novo_votante_uri}")
        else:
            print("Nenhum observador disponível para promoção. ")

    def commit_mensagem(self): # marca mensagem no indice fornecido como comitada
        if not self.log:  # Verifica se há mensagens no log normal
            print("Log normal está vazio. Nenhuma mensagem para commitar.")
            return
        mensagem_a_commit = self.log[0]  # Seleciona a primeira mensagem do log normal
        if mensagem_a_commit in self.log_commitadas:
            print(f"Mensagem já foi commitada: {mensagem_a_commit}")
            self.log.pop(0)
        else:
            commit = self.log.pop(0)
            self.log_commitadas.append(commit)
            print(f"Mensagem comitada: {commit}")
        # print(f"Log Normal - {self.log}")
        # print(f"Log Commitada - {self.log_commitadas}")

    @Pyro5.api.expose
    def obter_mensagens_commitadas(self, offset): # Retorna todas as mensagens que foram commitadas a partir de um offset.
        #print(f"teste - {self.log_commitadas[offset]}")
        return self.log_commitadas[offset:]

def send_heartbeat(lider):
    """Envia heartbeats periodicamente para os votantes registrados."""
    while True:
        print("Enviando heartbeat para os votantes...")
        lider.enviar_heartbeat()
        time.sleep(5)

def conection(): 

    lider = Lider()
    daemon = Pyro5.server.Daemon()
    uri = daemon.register(lider)
    try: 
        servidor_nomes = Pyro5.api.locate_ns()
        servidor_nomes.register("Lider_Epoca1", uri)
        print(f"Líder registado com uri: {uri}")
        threading.Thread(target=lider.enviar_heartbeat, args=(), daemon=True).start()
    except Pyro5.errors.NamingError as e:
        print(f"Erro ao registrar no servidor de nomes: {e}")
    daemon.requestLoop()

if __name__ == "__main__":
    conection()