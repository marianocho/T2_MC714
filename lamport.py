"""Relogio logico de Lamport (thread-safe).

"""
import threading


class LamportClock:
    def __init__(self):
        self._t = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Evento interno / envio: incrementa e retorna o novo valor."""
        with self._lock:
            self._t += 1
            return self._t

    def update(self, received: int) -> int:
        """Recebimento: L = max(L, ts_recebido) + 1."""
        with self._lock:
            self._t = max(self._t, int(received)) + 1
            return self._t

    @property
    def time(self) -> int:
        with self._lock:
            return self._t
