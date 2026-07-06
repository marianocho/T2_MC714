# MC714 — 2º Trabalho: Algoritmos Distribuídos

Implementação integrada de três algoritmos clássicos de sistemas distribuídos,
com **troca de mensagens real via TCP** (sockets da biblioteca padrão do Python —
não há simulação por arquivo):

- **Relógio lógico de Lamport** — carimba *toda* mensagem do protocolo.
- **Exclusão mútua — Ricart & Agrawala (1981)** — usa os timestamps de Lamport
  `(ts, id)` para ordenar pedidos concorrentes.
- **Eleição de líder — Bully (Garcia-Molina, 1982)** — com detecção de falha do
  líder e recuperação.

Os três algoritmos rodam no **mesmo** processo/nó: cada nó participa da eleição,
disputa a seção crítica e mantém seu relógio de Lamport. Uma frota de 4 nós
demonstra o sistema completo.

## Requisitos

- **Python 3.8+** (só a biblioteca padrão — nenhum `pip install`).
- Para a demo em contêineres: **Docker** + **Docker Compose**.

> Nos comandos abaixo uso `python`. Em algumas máquinas o executável é `python3`.

## Estrutura

```
mc714-trabalho2/
├── lamport.py            # relógio lógico de Lamport (thread-safe)
├── messages.py           # tipos e formato das mensagens (JSON por linha)
├── transport.py          # camada de transporte TCP + detector de falha
├── node.py               # o nó: Ricart-Agrawala + Bully + monitor
├── run_node.py           # ponto de entrada (CLI): inicia um nó
├── config.local.json     # mapa dos 4 nós em 127.0.0.1:5001-5004
├── config.docker.json    # mapa dos 4 nós em node1..node4:5000
├── Dockerfile
├── docker-compose.yml
├── scripts/
│   ├── node.sh           # sobe 1 nó interativo
│   ├── demo_local.sh     # sobe os 4 nós em modo demo
│   ├── stop_local.sh     # encerra a demo local
│   └── merge_logs.py     # junta os logs em ordem cronológica
├── RELATORIO.md          # relatório do trabalho
```

## Como executar

Há três formas. As duas primeiras são locais; a terceira usa contêineres
(reforça o "rede de verdade", já que cada nó fica isolado com seu próprio host).

### 1. Modo interativo

Abra **4 terminais**, um por nó:

```bash
bash scripts/node.sh 1
bash scripts/node.sh 2
bash scripts/node.sh 3
bash scripts/node.sh 4
```

Após ~2s ocorre a eleição inicial. Em qualquer terminal, digite comandos:

| comando    | efeito                                                        |
|------------|---------------------------------------------------------------|
| `cs`       | pede a seção crítica (Ricart-Agrawala) e a libera após ~1.5s  |
| `election` | dispara uma eleição (Bully)                                   |
| `status`   | mostra relógio de Lamport, estado do mutex e líder atual      |
| `crash`    | simula a queda deste nó (fica offline)                        |
| `recover`  | volta a ficar online e dispara nova eleição                   |
| `help`     | lista os comandos                                             |
| `quit`     | encerra o nó                                                  |

Experimentos sugeridos: peça `cs` em dois terminais quase ao mesmo tempo e veja
a ordenação por Lamport; dê `crash` no líder e observe a reeleição; depois
`recover` e veja o Bully reassumir.

### 2. Modo demo local 

Sobe os 4 nós executando um roteiro que exercita tudo (eleição inicial →
disputa de SC → queda do líder → reeleição → nova disputa de SC → recuperação):

```bash
bash scripts/demo_local.sh          # sobe os 4 nós (logs em ./logs/)
python scripts/merge_logs.py        # visão cronológica unificada dos 4 nós
bash scripts/stop_local.sh          # encerra tudo
```

Acompanhar ao vivo: `tail -f logs/node*.log`.

### 3. Docker Compose (4 contêineres, rede isolada)

```bash
docker compose up --build           # sobe os 4 nós e mostra os logs juntos
# em outro terminal, se quiser:
docker compose logs -f
docker compose down                 # derruba tudo
```

Cada serviço (`node1`..`node4`) é um contêiner com hostname próprio; eles se
encontram por DNS na rede bridge `sdnet` — exatamente os hosts de
`config.docker.json`.

## Como ler a saída

Cada linha tem o formato:

```
[   6.052] N1 | [MUTEX] >>>>> ENTROU na secao critica  (clock=32) <<<<<
   │         │      │
   │         │      └─ evento
   │         └─ nó
   └─ tempo (s) relativo ao início daquele processo
```

**Importante:** o tempo do prefixo é o relógio *físico local* de cada processo.
Como os processos começam em instantes ligeiramente diferentes, no log unificado
dois eventos de nós distintos podem parecer "fora de ordem" — é justamente por
isso que usamos o **relógio lógico de Lamport** (`clock=...`) para raciocinar
sobre a ordem causal. Veja a discussão no `RELATORIO.md`.

## Referências

Ver `RELATORIO.md` (seção *Fontes*). O código foi escrito do zero para este
trabalho; as referências são conceituais (artigos originais dos algoritmos e o
Tanenbaum).
