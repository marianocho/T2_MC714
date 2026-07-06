import sys
import time
import threading

from lamport import LamportClock
from transport import Transport
import messages as M

# ---- parametros de temporizacao (segundos) ----
STARTUP_DELAY = 2.0         # espera os peers subirem antes da 1a eleicao
HEARTBEAT_INTERVAL = 3.0    # periodo com que o monitor sonda o lider
ELECTION_OK_TIMEOUT = 3.0   # espera por OK apos enviar ELECTION
COORDINATOR_TIMEOUT = 4.0   # espera por COORDINATOR apos receber OK
CONNECT_TIMEOUT = 1.5       # timeout de conexao TCP
CS_HOLD_TIME = 1.5          # tempo segurando a secao critica (demo)


class Node:
    def __init__(self, node_id, peers, mode="interactive"):
        self.node_id = node_id
        self.mode = mode
        self.all_ids = sorted(peers.keys())
        self.peer_ids = [i for i in self.all_ids if i != node_id]
        self.host, self.port = peers[node_id]

        self.clock = LamportClock()
        self.lock = threading.RLock()
        self.start_time = time.time()
        self.alive = True

        # --- estado Ricart-Agrawala ---
        self.ra_state = "RELEASED"      # RELEASED | WANTED | HELD
        self.request_ts = None          # timestamp do nosso pedido corrente
        self.deferred = set()           # replies adiados (ids)
        self.pending = set()            # replies que ainda esperamos (ids)
        self.ra_event = threading.Event()

        # --- estado Bully ---
        self.leader = None
        self.election_in_progress = False
        self.ok_event = threading.Event()

        self.transport = Transport(
            node_id, self.host, self.port, peers,
            self.handle_message, self.log, connect_timeout=CONNECT_TIMEOUT,
        )

    # ------------------------------------------------------------------ log
    def log(self, text):
        t = time.time() - self.start_time
        print(f"[{t:8.3f}] N{self.node_id} | {text}", flush=True)

    def _names(self, ids):
        return "{" + ",".join("N%d" % i for i in ids) + "}"

    # --------------------------------------------------------------- ciclo
    def run(self):
        self.transport.start()
        self.log(f"online em {self.host}:{self.port}  (peers: {self._names(self.peer_ids)})")
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        threading.Timer(STARTUP_DELAY, self._start_election_async).start()

        if self.mode == "demo":
            self._run_demo()
            while True:
                time.sleep(1)
        elif self.mode == "interactive":
            self._interactive_loop()
        else:  # passive
            while True:
                time.sleep(1)

    # ------------------------------------------- recebimento (roteador)
    def handle_message(self, msg):
        if not self.alive:
            return
        mtype = msg.get("type")
        sender = msg.get("sender")
        ts = int(msg.get("clock", 0))
        new_t = self.clock.update(ts)          # regra de recebimento de Lamport
        self.log(f"recv {mtype:<11} de N{sender} (ts={ts}) -> clock={new_t}")

        if mtype == M.REQUEST:
            self._on_request(sender, ts)
        elif mtype == M.REPLY:
            self._on_reply(sender)
        elif mtype == M.ELECTION:
            self._on_election(sender)
        elif mtype == M.OK:
            self._on_ok(sender)
        elif mtype == M.COORDINATOR:
            self._on_coordinator(sender)

    # =====================================================================
    #  EXCLUSAO MUTUA -- Ricart & Agrawala (1981)
    # =====================================================================
    def request_cs(self):
        with self.lock:
            if self.ra_state != "RELEASED":
                self.log("[MUTEX] pedido ignorado (ja em WANTED/HELD)")
                return
            self.ra_state = "WANTED"
            self.request_ts = self.clock.tick()     # carimbo do pedido
            my_ts = self.request_ts
            targets = list(self.peer_ids)
            self.pending = set(targets)
            self.ra_event = threading.Event()
            if not targets:
                self.ra_event.set()
        self.log(f"[MUTEX] quer a SC  ts={my_ts}  -> REQUEST para {self._names(targets)}")
        for pid in targets:
            ok = self.transport.send(
                pid, {"type": M.REQUEST, "sender": self.node_id, "clock": my_ts}
            )
            if not ok:
                self.log(f"[MUTEX] N{pid} inacessivel; fora do quorum")
                self._got_reply(pid)

        self.ra_event.wait()                         # espera todos os REPLY
        with self.lock:
            self.ra_state = "HELD"
        self.log(f"[MUTEX] >>>>> ENTROU na secao critica  (clock={self.clock.time}) <<<<<")

    def release_cs(self):
        with self.lock:
            if self.ra_state != "HELD":
                return
            self.ra_state = "RELEASED"
            self.request_ts = None
            deferred = list(self.deferred)
            self.deferred = set()
        self.log(f"[MUTEX] <<<<< SAIU da secao critica  -> REPLY p/ adiados {self._names(deferred)}")
        for pid in deferred:
            self._send_reply(pid)

    def _on_request(self, sender, sender_ts):
        defer = False
        with self.lock:
            if self.ra_state == "HELD":
                defer = True
            elif self.ra_state == "WANTED":
                mine = (self.request_ts, self.node_id)
                theirs = (sender_ts, sender)
                if mine < theirs:                    # temos prioridade -> adia
                    defer = True
            my_ts = self.request_ts
            if defer:
                self.deferred.add(sender)
        if defer:
            self.log(f"[MUTEX] adia REPLY p/ N{sender} (ts deles={sender_ts}; meu={my_ts})")
        else:
            self._send_reply(sender)

    def _on_reply(self, sender):
        self._got_reply(sender)

    def _got_reply(self, pid):
        with self.lock:
            self.pending.discard(pid)
            if not self.pending and self.ra_state == "WANTED":
                self.ra_event.set()

    def _send_reply(self, target):
        ts = self.clock.tick()
        self.log(f"[MUTEX] REPLY -> N{target}")
        self.transport.send(target, {"type": M.REPLY, "sender": self.node_id, "clock": ts})

    # =====================================================================
    #  ELEICAO DE LIDER -- Bully (Garcia-Molina, 1982)
    # =====================================================================
    def _start_election_async(self):
        threading.Thread(target=self.start_election, daemon=True).start()

    def start_election(self):
        with self.lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
            higher = [i for i in self.all_ids if i > self.node_id]
            self.ok_event = threading.Event()
            ev = self.ok_event
        self.log(f"[ELECTION] iniciando eleicao; contatando IDs maiores {self._names(higher)}")
        if not higher:
            self._become_leader()
            return
        sent_any = False
        for pid in higher:
            ts = self.clock.tick()
            if self.transport.send(pid, {"type": M.ELECTION, "sender": self.node_id, "clock": ts}):
                sent_any = True
        if not sent_any:
            self.log("[ELECTION] nenhum ID maior acessivel; eu venco")
            self._become_leader()
            return

        got = ev.wait(timeout=ELECTION_OK_TIMEOUT)
        # Enquanto esperavamos, um ID maior pode ter se anunciado lider
        # diretamente (COORDINATOR). Se ja sabemos quem manda, recuamos.
        with self.lock:
            leader_known = self.leader is not None and self.leader > self.node_id
            if leader_known:
                self.election_in_progress = False
        if leader_known:
            return
        if got:
            self.log("[ELECTION] recebi OK; aguardando anuncio de COORDINATOR")
            with self.lock:
                self.election_in_progress = False
            threading.Timer(COORDINATOR_TIMEOUT, self._check_coordinator).start()
        else:
            self.log("[ELECTION] nenhum OK no prazo; eu venco")
            self._become_leader()

    def _become_leader(self):
        with self.lock:
            self.leader = self.node_id
            self.election_in_progress = False
            others = list(self.peer_ids)
        self.log(f"[ELECTION] *** SOU O LIDER (N{self.node_id}) *** anunciando a todos")
        for pid in others:
            ts = self.clock.tick()
            self.transport.send(pid, {"type": M.COORDINATOR, "sender": self.node_id, "clock": ts})

    def _on_election(self, sender):
        # so nos chega ELECTION de um ID menor. Se eu ja sou o lider estabelecido,
        # basta reafirmar a coordenacao (evita a "tempestade de anuncios" do Bully
        # ingenuo). Caso contrario, respondo OK e disputo a eleicao.
        with self.lock:
            i_am_leader = self.leader == self.node_id
        if i_am_leader:
            self._send_coordinator(sender)
            return
        self._send_ok(sender)
        threading.Thread(target=self.start_election, daemon=True).start()

    def _send_ok(self, target):
        ts = self.clock.tick()
        self.log(f"[ELECTION] OK -> N{target} (assumo a partir daqui)")
        self.transport.send(target, {"type": M.OK, "sender": self.node_id, "clock": ts})

    def _send_coordinator(self, target):
        ts = self.clock.tick()
        self.log(f"[ELECTION] COORDINATOR -> N{target} (reafirmo que sou o lider)")
        self.transport.send(target, {"type": M.COORDINATOR, "sender": self.node_id, "clock": ts})

    def _on_ok(self, sender):
        self.log(f"[ELECTION] recebi OK de N{sender}; recuo")
        self.ok_event.set()

    def _on_coordinator(self, sender):
        with self.lock:
            self.leader = sender
            self.election_in_progress = False
            self.ok_event.set()   # libera uma eleicao nossa que estava em espera
        self.log(f"[ELECTION] *** N{sender} e o novo LIDER ***")

    def _check_coordinator(self):
        with self.lock:
            leader = self.leader
            in_progress = self.election_in_progress
        ok = leader is not None and leader >= self.node_id
        if not ok and not in_progress:
            self.log("[ELECTION] sem COORDINATOR no prazo; reiniciando eleicao")
            threading.Thread(target=self.start_election, daemon=True).start()

    # =====================================================================
    #  DETECCAO DE FALHA (monitor do lider)
    # =====================================================================
    def _monitor_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self.alive:
                continue
            with self.lock:
                leader = self.leader
                in_progress = self.election_in_progress
            if leader is None or in_progress or leader == self.node_id:
                continue
            if not self.transport.is_reachable(leader):
                self.log(f"[MONITOR] lider N{leader} nao responde; iniciando eleicao")
                with self.lock:
                    self.leader = None
                threading.Thread(target=self.start_election, daemon=True).start()

    # =====================================================================
    #  SIMULACAO DE FALHA / RECUPERACAO
    # =====================================================================
    def crash(self):
        self.log("[CRASH] simulando queda (ficando offline)")
        with self.lock:
            self.alive = False
        self.transport.crash()

    def recover(self):
        self.log("[RECOVER] voltando a ficar online")
        self.transport.recover()
        with self.lock:
            self.alive = True
            self.leader = None
        threading.Thread(target=self.start_election, daemon=True).start()

    # =====================================================================
    #  MODOS DE OPERACAO
    # =====================================================================
    def print_status(self):
        with self.lock:
            leader = ("N%d" % self.leader) if self.leader else "?"
            self.log(f"[STATUS] clock={self.clock.time}  mutex={self.ra_state}  "
                     f"lider={leader}  alive={self.alive}")

    def _do_cs(self):
        def run():
            self.request_cs()
            time.sleep(CS_HOLD_TIME)
            self.release_cs()
        threading.Thread(target=run, daemon=True).start()

    def _interactive_loop(self):
        self._print_help()
        try:
            for line in sys.stdin:
                cmd = line.strip().lower()
                if cmd in ("cs", "c"):
                    self._do_cs()
                elif cmd in ("election", "e"):
                    self._start_election_async()
                elif cmd in ("status", "s"):
                    self.print_status()
                elif cmd == "crash":
                    self.crash()
                elif cmd == "recover":
                    self.recover()
                elif cmd in ("help", "h", "?"):
                    self._print_help()
                elif cmd in ("quit", "q", "exit"):
                    self.log("encerrando")
                    return
                elif cmd == "":
                    continue
                else:
                    self.log(f"comando desconhecido: {cmd!r} (digite 'help')")
        except (KeyboardInterrupt, EOFError):
            return

    def _print_help(self):
        self.log("comandos: cs | election | status | crash | recover | help | quit")

    # ---- roteiro automatico (modo demo) ----
    def _sleep_to(self, t):
        now = time.time() - self.start_time
        if t > now:
            time.sleep(t - now)

    def _demo_cs(self):
        self.request_cs()
        time.sleep(CS_HOLD_TIME)
        self.release_cs()

    def _run_demo(self):
        nid = self.node_id
        max_id = max(self.all_ids)

        # Fase 0: eleicao inicial acontece via timer (~t2). Deixe assentar.
        self._sleep_to(6.0)
        if nid == 1:
            self.log(f"[DEMO] ===== Fase 1: disputa pela secao critica (lider: N{self.leader}) =====")

        # Fase 1: nos 1, 2 e 3 disputam a SC quase ao mesmo tempo.
        if nid in (1, 2, 3):
            self._sleep_to(6.0 + 0.05 * nid)
            self._demo_cs()

        if nid == max_id:
            # Fase 2: o lider (maior ID) cai.
            self._sleep_to(16.0)
            self.log("[DEMO] ===== Fase 2: queda do lider =====")
            self.crash()
            # Fase 4: volta mais tarde e reassume (comportamento 'bully').
            self._sleep_to(30.0)
            self.log("[DEMO] ===== Fase 4: ex-lider se recupera e reassume =====")
            self.recover()
        else:
            # Fase 3: nova rodada de SC ja sob o novo lider.
            if nid in (1, 2):
                self._sleep_to(24.0)
                if nid == 1:
                    self.log("[DEMO] ===== Fase 3: nova rodada de SC sob o novo lider =====")
                self._demo_cs()
