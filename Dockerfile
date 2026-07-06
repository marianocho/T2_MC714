# Imagem unica usada por todos os nos. Nenhuma dependencia externa:
# o projeto usa apenas a biblioteca padrao do Python.
FROM python:3.11-slim

WORKDIR /app

# saida sem buffer -> os logs aparecem em tempo real no `docker compose logs`
ENV PYTHONUNBUFFERED=1

COPY . .

# comando padrao (sobrescrito por cada servico no docker-compose.yml)
CMD ["python", "run_node.py", "--id", "1", "--config", "config.docker.json", "--mode", "demo"]
