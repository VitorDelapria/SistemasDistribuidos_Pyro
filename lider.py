import Pyro5
import Pyro5.api
import Pyro5.core
import Pyro5.errors
import Pyro5.server
import Pyro5.client
import time
import threading


@Pyro5.server.expose
class Lider(object):
    def __init__(self, max_falhas):
        self.votantes = []
        self.observadores = []
        self.mensagens = []
        self.log = [] # Log de gravações do Lider (mensagem + metadados)
        self.mensagens_commitadas = []
        self.confirmacoes = {} # confirmações recebidas dos votantes
        self.max_falhas = max_falhas # Numero maximo de falhas toleradas
        self.quorum = 2 * max_falhas + 1 # tamanho do quorum
        self.falhas = {}
        self.falhas_toleradas = 1
        self.quorum = self.falhas_toleradas + 1 

    def registrar_votante(self, uri):
        print(f"ESTOU AQUI NO VOTANTE")
        if uri not in self.votantes:
            self.votantes.append(uri)
            print(f"Votante registrado: {uri}")
        else:
            print(f"Votante já registrado: {uri}")

    def registrar_observador(self, uri):
        self.observadores.append(uri)   
        print(f"Observador registrado: {uri}")

    def publicar_mensagem(self, mensagem):
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
                print("OLÁ ESTOU")
                votante.buscar(offset, len(self.log))
                print(f"Notificado votante {uri} sobre novo log no offset {offset}.")
            except Pyro5.errors.CommunicationError as e:
                print(f"Falha ao notificar votante {uri}: {e}")

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
    

    def receber_confirmacao(self, offset, votante_uri):
        if offset not in self.confirmacoes:
            self.confirmacoes[offset] = set()
        self.confirmacoes[offset].add(votante_uri)

        if len(self.confirmacoes[offset]) >= (self.quorum // 2) + 1:
            self.log[offset]["confirmado"] = True
            print(f"Quorum atingido para a mensagem no offset {offset}.")
            self.commit_mensagem(self.log[offset]["mensagem"])

    def notificar_observadores(self, menssage):
        for uri in self.observadores:
            try:
                observador = Pyro5.api.Proxy(uri)
                observador.replicar_notificacao(menssage)
                print(f"Notificação enviada para: {uri}")
            except Pyro5.errors.CommunicationError:
                print(f"Falha ao notificar {uri}. Observador pode estar offline. ")

    def enviar_heartbeat(self, intervalo=5):
        def _heartbeat_task():
            while True:
                print("Enviando Heartbeats...")
                for uri in self.votantes:
                    try:
                        votante = Pyro5.api.Proxy(uri)
                        if not votante.heartbeat():
                            raise Pyro5.errors.CommunicationError("Heartbeat não reconhecido.")
                        print(f"Heartbeat enviado e reconhecido por {uri}")
                        self.falhas[uri] = 0 
                    except Pyro5.errors.CommunicationError:
                        print(f"Falha no Heartbeat com {uri}. Incrementando contador de falhas.")
                        self.falhas[uri] = self.falhas.get(uri, 0) + 1
                        if self.falhas[uri] >= self.max_falhas:
                            print(f"Votante {uri} atingiu o limite de falhas. Será removido.")
                            self.remover_votante(uri)
                            self.promover_observador()
                time.sleep(intervalo)

        threading.Thread(target=_heartbeat_task, daemon=True).start()


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
        if mensagem not in self.mensagens_commitadas:
            self.mensagens_commitadas.append(mensagem)
            print(f"posição 1 {self.mensagens_commitadas[0]}")
            print(f"Mensagem comitada: {mensagem}")
        else:
            print(f"Mensagem já foi commitada: {mensagem}")

    def obter_mensagens_commitadas(self, offset): # Retorna todas as mensagens que foram commitadas a partir de um offset.
        return self.mensagens_commitadas[offset:]

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