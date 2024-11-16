import Pyro5
import time
import sys
import Pyro5.api

class Consumidor:
    def __init__(self):
        self.mensagens_consumidas = []

    def consumir_mensagem(self, menssage):
        print(f"[Consumidor] Consumindo mensagem: {menssage}")
        self.mensagens_consumidas.append(menssage)

    def esperar_mensagens_comitadas(self, uri):
        lider = Pyro5.api.Proxy(uri)
        while True:
            mensagens_commitadas = lider.obter_mensagens_commitadas()
            for menssage in mensagens_commitadas:
                self.consumir_mensagem(menssage)
            time.sleep(2)

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

    if role == "consumidor":
        consumidor = Consumidor()
        uri = daemon.register(consumidor)
        ns.register("Consumidor1", uri)
        print(f"Consumidor registrado com URI: {uri}")

        # O consumidor começa a escutar por mensagens commitadas
        consumidor.esperar_mensagens_comitadas(uri_lider)

    daemon.requestLoop()  # Loop aqui usa o mesmo daemon

if __name__ == "__main__":  # Corrigido para "__main__"
    if len(sys.argv) != 2:
        print("Uso: python3 consumidor.py [consumidor]")
    else:
        role = sys.argv[1].lower()
        conection(role)  # Passando o role como parâmetro
