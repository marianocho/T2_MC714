# Relatório — MC714, 2º Trabalho

**Disciplina:** MC714 — Sistemas Distribuídos — IC/UNICAMP
**Tema:** implementação integrada de três algoritmos distribuídos com troca de
mensagens real (TCP).

---

## 1. O problema

O trabalho pede a implementação de três algoritmos distribuídos clássicos, usando
uma forma real de comunicação entre processos (não simulação por arquivo):

1. relógio lógico de **Lamport**;
2. um algoritmo de **exclusão mútua**;
3. um algoritmo de **eleição de líder** / consenso.

Em vez de três programas isolados, optamos por um **único sistema coeso**: uma
frota de nós idênticos em que cada nó (a) mantém um relógio de Lamport, (b) disputa
uma seção crítica compartilhada por exclusão mútua e (c) participa da eleição de um
líder, com tolerância à queda do líder. Assim os três algoritmos interagem no mesmo
processo, o que é mais próximo de um sistema real e evidencia como o relógio de
Lamport serve de base para os outros dois.

## 2. Algoritmos escolhidos

### 2.1 Relógio lógico de Lamport (1978)

Cada processo mantém um contador inteiro `L`. As regras (`lamport.py`):

- **evento local / envio:** `L ← L + 1` (`tick()`), e o valor vai carimbado na
  mensagem;
- **recebimento** de uma mensagem com timestamp `ts`: `L ← max(L, ts) + 1`
  (`update(ts)`).

Isso garante a relação *happened-before* (→): se `a → b`, então `L(a) < L(b)`.
No sistema, **toda** mensagem de aplicação (REQUEST/REPLY do mutex e
ELECTION/OK/COORDINATOR da eleição) carrega e atualiza o relógio.

### 2.2 Exclusão mútua — Ricart & Agrawala (1981)

Algoritmo distribuído baseado em permissão, sem coordenador central. Escolhido
porque **usa diretamente os timestamps de Lamport** para ordenar pedidos
concorrentes — é a vitrine natural do relógio lógico. Cada nó tem um estado
`RELEASED | WANTED | HELD`.

Para entrar na seção crítica (SC), o nó carimba o pedido com `ts = tick()` e envia
`REQUEST(ts, id)` a **todos** os outros. Só entra quando recebe `REPLY` de todos.
Ao receber um `REQUEST(ts_j, j)`, o nó `i` decide:

```
se estou HELD:                                   -> adia o REPLY (defer)
senão se estou WANTED e (meu_ts, meu_id) < (ts_j, j):  -> adia o REPLY (defer)
senão:                                            -> responde REPLY na hora
```

O par `(ts, id)` é comparado lexicograficamente: o `id` desempata timestamps
iguais, garantindo uma ordem total. Ao sair da SC, o nó envia os `REPLY` que havia
adiado. **Invariante:** no máximo um nó em `HELD` por vez, e a ordem de entrada
respeita os timestamps de Lamport.

### 2.3 Eleição de líder — Bully (Garcia-Molina, 1982)

Elege como líder o nó de **maior id** vivo. Quando um nó percebe que precisa de
líder (na inicialização ou ao detectar a queda do líder), ele:

```
manda ELECTION para todos os ids MAIORES que o seu
  - se ninguem responde no prazo  -> ele vence; manda COORDINATOR a todos
  - se algum maior responde OK     -> ele recua e espera o anuncio COORDINATOR
ao receber ELECTION de um id menor -> responde OK e inicia sua propria eleicao
ao receber COORDINATOR             -> registra o remetente como lider
```

**Otimização adotada:** um líder já estabelecido, ao receber um `ELECTION` de um id
menor, responde **diretamente com `COORDINATOR`** (reafirmando a liderança) em vez
de disparar uma eleição inteira de novo. Isso reduz a "tempestade de anúncios" do
Bully ingênuo quando vários nós iniciam eleição ao mesmo tempo. (Ver §6.3 uma sutileza
de corretude que essa otimização exigiu tratar.)

## 3. Arquitetura e comunicação

### 3.1 Componentes

```
        +-------------------------------------------------------+
        |                        Node                           |
        |  +-------------+  +------------------+  +-----------+  |
        |  | LamportClock|  | Ricart-Agrawala  |  |   Bully   |  |
        |  |  (contador) |  | (estado da SC)   |  | (eleicao) |  |
        |  +-------------+  +------------------+  +-----------+  |
        |         \______________ | _______________/            |
        |                    handle_message                     |
        |                 (roteador + monitor)                  |
        +--------------------------|----------------------------+
                                   | usa
                          +--------v---------+
                          |    Transport     |   listener TCP + envio
                          | (sockets, JSON)  |   + sonda de falha
                          +--------|---------+
                                   | TCP
              rede: localhost (portas 5001-5004) ou bridge Docker
```

### 3.2 Formato das mensagens (`messages.py`)

Cada mensagem é **uma linha JSON** terminada em `\n`, com três campos:

```json
{"type": "REQUEST", "sender": 1, "clock": 18}
```

Tipos: `REQUEST`, `REPLY` (Ricart-Agrawala); `ELECTION`, `OK`, `COORDINATOR`
(Bully). O `clock` é o timestamp de Lamport do remetente.

### 3.3 Transporte TCP (`transport.py`)

- Cada nó abre um **socket servidor** e aceita conexões numa thread dedicada; cada
  conexão é lida em sua própria thread (mensagens delimitadas por `\n`).
- O envio é **conexão-por-mensagem** ("fire-and-forget"): abre, envia a linha,
  fecha. As respostas chegam de forma assíncrona no listener. Isso simplifica o
  código e evita manter N² conexões.
- **Detector de falha:** `is_reachable(peer)` tenta apenas um `connect` TCP. Um nó
  "caído" fecha seu socket servidor → o `connect` falha (*connection refused*) → a
  falha é detectada. `send()` devolve `False` quando o destino está inacessível.
- `crash()` fecha o socket servidor; `recover()` reabre (com `SO_REUSEADDR`).

### 3.4 Concorrência

Um `threading.RLock` por nó protege o estado compartilhado (relógio, estado da SC,
líder). Regra seguida à risca: **o lock nunca é mantido durante I/O de rede** — as
mensagens são preparadas sob o lock e enviadas fora dele, evitando deadlock. A
espera por `REPLY`s e por `OK` usa `threading.Event`.

## 4. Detalhes de implementação

- **Linguagem:** Python 3, **somente biblioteca padrão** (`socket`, `threading`,
  `json`, `time`). Nenhuma dependência externa.
- **Arquivos:** `lamport.py`, `messages.py`, `transport.py`, `node.py`,
  `run_node.py` (CLI com `--id`, `--config`, `--mode`).
- **Modos de execução:** `interactive` (comandos via teclado), `demo` (roteiro
  automático) e `passive`.
- **Parâmetros de tempo** (em `node.py`): atraso de partida 2s, período do monitor
  3s, timeout de OK 3s, timeout de COORDINATOR 4s, timeout de conexão 1.5s, tempo
  segurando a SC 1.5s.

## 5. Ambiente de execução e testes

Testado de duas formas (instruções completas no `README.md`):

- **Local:** 4 processos em `127.0.0.1`, portas 5001–5004.
- **Docker Compose:** 4 contêineres (`node1`..`node4`) numa rede bridge isolada,
  cada um com hostname próprio, comunicando-se por DNS interno. Reforça que a
  comunicação é por rede de verdade.

O **modo demo** executa um roteiro determinístico de ~35s que exercita todos os
mecanismos, coordenado de forma descentralizada (cada nó age pelo seu id e pelo
tempo decorrido, sem um "maestro" central):

| tempo | fase | o que acontece |
|-------|------|----------------|
| ~2s   | 0 | eleição inicial → **N4** vira líder |
| ~6s   | 1 | N1, N2 e N3 disputam a SC quase juntos |
| 16s   | 2 | o líder **N4 cai** |
| ~18s  | – | os nós detectam a falha e reelegem **N3** |
| 24s   | 3 | N1 e N2 disputam a SC (N4 fora do quórum) |
| 30s   | 4 | **N4 se recupera** e reassume a liderança |

### 5.1 Experimento 1 — exclusão mútua ordenada por Lamport (Fase 1)

Trecho real do log unificado (`python scripts/merge_logs.py`):

```
[   6.050] N1 | [MUTEX] quer a SC  ts=18  -> REQUEST para {N2,N3,N4}
[   6.052] N1 | [MUTEX] >>>>> ENTROU na secao critica  (clock=28) <<<<<
[   6.100] N2 | [MUTEX] quer a SC  ts=26  -> REQUEST para {N1,N3,N4}
[   6.111] N1 | [MUTEX] adia REPLY p/ N2 (ts deles=26; meu=18)
[   6.150] N3 | [MUTEX] quer a SC  ts=29  -> REQUEST para {N1,N2,N4}
[   6.160] N1 | [MUTEX] adia REPLY p/ N3 (ts deles=29; meu=18)
[   7.552] N1 | [MUTEX] <<<<< SAIU da secao critica -> REPLY p/ adiados {N2,N3}
[   9.043] N2 | [MUTEX] <<<<< SAIU da secao critica -> REPLY p/ adiados {N3}
[   9.045] N3 | [MUTEX] >>>>> ENTROU na secao critica  (clock=34) <<<<<
```

N1 (ts=18) entra primeiro; N2 (ts=26) e N3 (ts=29) ficam adiados por N1 e só entram
na ordem exata dos timestamps de Lamport — **N1 → N2 → N3**. Em nenhum instante há
dois nós na SC ao mesmo tempo.

### 5.2 Experimento 2 — detecção de falha e reeleição (Fases 2–3)

```
[  16.000] N4 | [CRASH] simulando queda (ficando offline)
[  18.004] N3 | [MONITOR] lider N4 nao responde; iniciando eleicao
[  18.004] N3 | [ELECTION] nenhum ID maior acessivel; eu venco
[  18.004] N3 | [ELECTION] *** SOU O LIDER (N3) *** anunciando a todos
```

Cerca de 2s após a queda (uma passada do monitor), os nós vivos detectam o líder
inacessível e o Bully elege **N3** (maior id sobrevivente). Na disputa de SC
seguinte, o nó caído é corretamente excluído do quórum:

```
[  24.000] N1 | [MUTEX] quer a SC  ts=38  -> REQUEST para {N2,N3,N4}
[  24.001] N1 | [MUTEX] N4 inacessivel; fora do quorum
[  24.002] N1 | [MUTEX] >>>>> ENTROU na secao critica  (clock=45) <<<<<
```

### 5.3 Experimento 3 — recuperação e "bully" (Fase 4)

```
[  30.000] N4 | [RECOVER] voltando a ficar online
[  30.000] N4 | [ELECTION] *** SOU O LIDER (N4) *** anunciando a todos
```

Ao voltar, N4 (maior id) dispara eleição e reassume — o comportamento que dá nome
ao algoritmo.

## 6. Decisões de projeto

### 6.1 O detector de falha fica fora do relógio de Lamport

As sondas de vivacidade (`is_reachable`, feitas por `connect` TCP) são um mecanismo
de **plano de controle** e **deliberadamente não incrementam** o relógio de
Lamport. Motivo: o relógio de Lamport deve capturar a ordem causal dos **eventos da
aplicação** (pedidos de SC e a eleição). Misturar os *heartbeats* poluiria os
timestamps sem agregar significado causal. Foi uma escolha consciente, não um
descuido.

### 6.2 Tratamento de nó caído no Ricart-Agrawala (e suas premissas)

O Ricart-Agrawala original assume que ninguém falha. Aqui, se um destino está
inacessível no momento do `REQUEST`, ele é excluído do quórum daquela rodada
(tratado como um `REPLY` implícito). **Premissa:** isso é seguro porque um nó caído
não pode estar na SC. A limitação conhecida é o caso de partição de rede (um nó vivo
mas isolado poderia ser excluído indevidamente) — fora do escopo deste trabalho,
mas registrado honestamente.

### 6.3 Otimização do Bully e a sutileza de corretude

Fazer o líder responder `COORDINATOR` direto (em vez de `OK`) a um `ELECTION`
atrasado reduz mensagens, mas expôs uma condição de corrida: um nó que iniciou
eleição fica esperando um `OK`; se o líder responde só com `COORDINATOR`, esse nó
poderia estourar o timeout e se autodeclarar líder por engano. Corrigimos fazendo o
recebimento de `COORDINATOR` **também** liberar a espera da eleição e o nó recuar
assim que souber que existe um líder de id maior. Depois da correção, a eleição
converge para um único líder em todas as fases (verificado no log).

## 7. Fontes

O **código foi escrito do zero** para este trabalho — não foi copiado de repositórios
ou tutoriais da internet. As referências abaixo são **conceituais** (os algoritmos e
sua base teórica):

- L. Lamport. *Time, Clocks, and the Ordering of Events in a Distributed System.*
  CACM, 1978.
- G. Ricart, A. K. Agrawala. *An Optimal Algorithm for Mutual Exclusion in Computer
  Networks.* CACM, 1981.
- H. Garcia-Molina. *Elections in a Distributed Computing System.* IEEE ToC, 1982.
- A. S. Tanenbaum, M. van Steen. *Distributed Systems: Principles and Paradigms*
  (relógios lógicos, exclusão mútua e eleição — caps. 6).

## 8. Comentários sobre a experiência

O ponto mais instrutivo foi ver, na prática, por que relógios físicos não servem
para ordenar eventos entre máquinas. No log unificado, como cada processo mede o
tempo a partir do seu próprio início, eventos de nós diferentes às vezes aparecem
"fora de ordem" pelo relógio de parede — enquanto os timestamps de Lamport dão a
ordem causal consistente. Foi exatamente o argumento do artigo de 1978 aparecendo
sozinho nos dados.

O segundo aprendizado foi de engenharia de concorrência: a regra de **nunca segurar
o lock durante I/O de rede** e usar `Event` para as esperas foi o que manteve o
sistema livre de deadlocks. E a correção descrita em §6.3 mostrou como uma
"otimização óbvia" pode quebrar uma garantia sutil se o protocolo não for revisto
com cuidado.
