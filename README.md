# 6Sem2025ETL

## Pre-Commit

Este repositório utiliza [pre-commit](https://pre-commit.com/) para gerenciar hooks de Git que ajudam a manter a qualidade do código. Para instalar os hooks localmente, execute:

```bash
pre-commit install
```

Para verificar todos os arquivos com os hooks configurados, execute:

```bash
pre-commit run --all-files
```

## Estrutura de arquivos e diretórios

Descrição dos principais arquivos e diretórios deste repositório:

```text
.
├── README.md                       # Documento com visão geral do projeto e instruções de uso
├── requirements.txt                # Lista de dependências Python (para pip install)
├── setup.py                        # Script setuptools para empacotamento (wheel/sdist) e instalação
├── sonar-project.properties        # Configuração do SonarCloud/SonarQube (project key, fontes, cobertura)
└── src/                            # Código-fonte do projeto
    ├── config/                     # Carregamento de configurações (variáveis de ambiente, arquivos YAML/JSON)
    ├── entities/                   # Modelos de domínio (dataclasses, schemas)
    ├── process/                    # Lógica de orquestração do ETL (pipelines e fluxos de processamento)
    ├── services/                   # Implementações de serviços (API clients, integrações, acesso a dados)
    ├── test/                       # Testes (pytest) — caminho de descoberta usado pelo CI
    │   └── test_hello.py           # Teste de smoke/exemplo para validar configuração de testes
    └── utils/                      # Funções utilitárias e helpers reutilizáveis
```
