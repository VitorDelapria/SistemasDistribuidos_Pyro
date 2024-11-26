import Pyro5.api
import sys

# Classe do Publicador
class Publicador:
    def __init__(self, uri_lider):
        # A URI do líder será passada para se conectar ao líder.
        self.lider = Pyro5.api.Proxy(uri_lider)
    
    def enviar_mensagem(self, mensagem):
        # Envia a mensagem para o líder
        try:
            self.lider.publicar_mensagem(mensagem)
            print(f"Mensagem enviada para o Líder: {mensagem}")
        except Pyro5.errors.CommunicationError as e:
            print(f"Erro ao enviar mensagem para o Líder: {e}")

# Função principal para executar o publicador
def main():
    if len(sys.argv) != 2:
        print("Uso: python3 publicador.py <mensagem>")
        return
    
    mensagem = sys.argv[1]  # Mensagem que será enviada

    try:
        # Localiza o servidor de nomes e obtém a URI do Líder
        ns = Pyro5.api.locate_ns()
        uri_lider = ns.lookup("Lider_Epoca1")  # Buscar o líder no serviço de nomes
        
        # Cria a instância do publicador
        publicador = Publicador(uri_lider)
        
        # Envia a mensagem para o líder
        publicador.enviar_mensagem(mensagem)

    except Pyro5.errors.CommunicationError as e:
        print(f"Erro ao conectar com o servidor de nomes ou líder: {e}")

if __name__ == "__main__":
    main()
