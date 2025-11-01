# Mapa de Calor

Projeto em Python que coleta notícias de múltiplos feeds RSS, identifica ocorrências relacionadas a crimes, geocodifica os locais mencionados e gera um mapa de calor em HTML para visualização

## Estrutura do projeto

```
.
├── Bot/
│   ├── app.py           # Pipeline completo de coleta e geração do mapa
|   ├── event.db         # Saída padrão dos dados coletados
│   ├── heatmap.html     # Saída padrão do mapa de calor
│   └── requirements.txt # Dependências do projeto
└── README.md
```

## Como utilizar

1. Crie e ative um ambiente virtual Python:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
2. Instale as dependências:
   ```bash
   pip install -r Bot/requirements.txt
   ```
3. Execute o pipeline principal:
   ```bash
   python Bot/app.py
   ```
4. Abra o arquivo `Bot/heatmap.html` no navegador para visualizar o mapa de calor gerado

## Observações

- O script cria um banco `events.db` na pasta raiz para armazenar os eventos.
- O pipeline usa serviços externos de RSS e geocodificação; é necessário acesso à internet
