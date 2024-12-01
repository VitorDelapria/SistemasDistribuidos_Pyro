import Pyro5.api

class Consumidor:
    def __init__(self, uri_lider):
        self.broker = Pyro5.api.Proxy(uri_lider)

    def consume_messages(self, offset):
        print(f"Buscando mensagens a partir do offset: {offset}")
        try:
            print(f"EstouAQui! - {offset}")
            messages = self.broker.obter_mensagens_commitadas(offset)
            print(f"Mensagem - {messages}")
            if messages:
                for message in messages:
                    print(f"Mensagem consumida: {message}")
                return offset + len(messages) 
            else:
                print("Nenhuma nova mensagem disponível.")
        except Exception as e:
            print(f"Erro ao buscar mensagens: {e}")
        return offset

if __name__ == "__main__":

    ns = Pyro5.api.locate_ns()
    uri_lider = ns.lookup("Lider_Epoca1")  
    print(uri_lider)
    consumidor = Consumidor(uri_lider)
    current_offset = 0

    while True:
        input("Pressione Enter para buscar novas mensagens...")
        current_offset = consumidor.consume_messages(current_offset)
