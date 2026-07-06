"""Transporte TCP ponto-a-ponto (troca de mensagens real).

Cada no escuta em (host, port). O envio abre uma conexao nova por mensagem
(fire-and-forget): e simples, robusto e -- de quebra -- serve como detector
de falhas, pois um no "morto" recusa a conexao. As respostas nao voltam pela
mesma conexao: chegam de forma assincrona como novas mensagens ao listener
do remetente.
"""
import json
import socket
import threading


class Transport:
    def __init__(self, node_id, host, port, peers, on_message, log, connect_timeout=1.5):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers            # {id: (host, port)}
        self.on_message = on_message  # callback(dict)
        self.log = log
        self.connect_timeout = connect_timeout
        self._server = None
        self._listening = False
        self._lock = threading.Lock()

    # ---------------------------------------------------------- listener
    def start(self):
        self._open()

    def _open(self):
        with self._lock:
            if self._listening:
                return
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen(32)
            self._server = s
            self._listening = True
        threading.Thread(target=self._accept_loop, args=(s,), daemon=True).start()

    def _accept_loop(self, server):
        while True:
            with self._lock:
                if not self._listening or self._server is not server:
                    break
            try:
                conn, _ = server.accept()
            except OSError:
                break
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        try:
            with conn:
                f = conn.makefile("r", encoding="utf-8")
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    self.on_message(msg)
        except OSError:
            pass

    # ------------------------------------------------------------- envio
    def send(self, peer_id, msg) -> bool:
        """Envia uma mensagem. Retorna False se o destino esta inacessivel."""
        host, port = self.peers[peer_id]
        data = (json.dumps(msg) + "\n").encode("utf-8")
        try:
            with socket.create_connection((host, port), timeout=self.connect_timeout) as s:
                s.sendall(data)
            return True
        except OSError:
            return False

    def is_reachable(self, peer_id) -> bool:
        """Sonda TCP usada pelo detector de falhas."""
        host, port = self.peers[peer_id]
        try:
            with socket.create_connection((host, port), timeout=self.connect_timeout):
                return True
        except OSError:
            return False

    # ------------------------------------------------- simulacao de falha
    def crash(self):
        """Fecha o socket de escuta: passa a recusar conexoes (no 'morto')."""
        with self._lock:
            self._listening = False
            if self._server is not None:
                try:
                    self._server.close()
                except OSError:
                    pass
                self._server = None

    def recover(self):
        self._open()
