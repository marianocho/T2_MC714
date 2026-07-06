"""Ponto de entrada: inicia UM no do sistema distribuido.


"""
import argparse
import json
import sys

from node import Node


def main():
    ap = argparse.ArgumentParser(
        description="No SD: Lamport + Ricart-Agrawala + Bully"
    )
    ap.add_argument("--id", type=int, required=True, help="ID (inteiro) deste no")
    ap.add_argument("--config", default="config.local.json",
                    help="arquivo JSON com o mapa de nos")
    ap.add_argument("--mode", choices=["interactive", "demo", "passive"],
                    default="interactive",
                    help="interactive: comandos via teclado | demo: roteiro "
                         "automatico | passive: so responde a mensagens")
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    peers = {int(k): (v["host"], int(v["port"])) for k, v in cfg["nodes"].items()}

    if args.id not in peers:
        print(f"erro: id {args.id} nao esta no config {args.config}", file=sys.stderr)
        sys.exit(1)

    Node(args.id, peers, mode=args.mode).run()


if __name__ == "__main__":
    main()
