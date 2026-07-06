"""Tipos de mensagem do protocolo.

Toda mensagem trafega como uma linha JSON terminada em '\\n' e carrega:
  { "type": <tipo>, "sender": <id>, "clock": <timestamp de Lamport> }
"""

# --- Ricart-Agrawala (exclusao mutua) ---
REQUEST = "REQUEST"          # pedido de acesso a secao critica
REPLY = "REPLY"              # permissao concedida

# --- Bully (eleicao de lider) ---
ELECTION = "ELECTION"        # inicia eleicao; enviado aos IDs MAIORES
OK = "OK"                    # "estou vivo, eu assumo daqui" (resposta a ELECTION)
COORDINATOR = "COORDINATOR"  # anuncio do novo lider a todos
