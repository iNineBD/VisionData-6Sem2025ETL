import os
from elasticsearch import Elasticsearch
from .logger import setup_logger

logger = setup_logger(__name__)

INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "ticket_id": {"type": "keyword"},
            "title": {
                "type": "text",
                "analyzer": "brazilian",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "description": {"type": "text", "analyzer": "brazilian"},
            "channel": {"type": "keyword"},
            "device": {"type": "keyword"},
            "current_status": {"type": "keyword"},
            "sla_plan": {"type": "keyword"},
            "priority": {"type": "keyword"},
            "dates": {
                "properties": {
                    "created_at": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                    },
                    "first_response_at": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                    },
                    "closed_at": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                    },
                }
            },
            "company": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "analyzer": "brazilian",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "cnpj": {"type": "keyword"},
                    "segment": {"type": "keyword"},
                }
            },
            "created_by_user": {
                "properties": {
                    "id": {"type": "keyword"},
                    "full_name": {
                        "type": "text",
                        "analyzer": "brazilian",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "email": {"type": "keyword"},
                    "phone": {"type": "keyword"},
                    "cpf": {"type": "keyword"},
                    "is_vip": {"type": "boolean"},
                }
            },
            "assigned_agent": {
                "properties": {
                    "id": {"type": "keyword"},
                    "full_name": {
                        "type": "text",
                        "analyzer": "brazilian",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "email": {"type": "keyword"},
                    "department": {"type": "keyword"},
                }
            },
            "product": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "analyzer": "brazilian",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "code": {"type": "keyword"},
                    "description": {"type": "text", "analyzer": "brazilian"},
                }
            },
            "category": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "analyzer": "brazilian",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                }
            },
            "subcategory": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "analyzer": "brazilian",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                }
            },
            "attachments": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "filename": {
                        "type": "text",
                        "analyzer": "brazilian",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "mime_type": {"type": "keyword"},
                    "size_bytes": {"type": "long"},
                    "storage_path": {"type": "keyword", "index": False},
                    "uploaded_at": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                    },
                },
            },
            "tags": {"type": "keyword"},
            "status_history": {
                "type": "nested",
                "properties": {
                    "from_status": {"type": "keyword"},
                    "to_status": {"type": "keyword"},
                    "changed_at": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                    },
                    "changed_by_agent_id": {"type": "keyword"},
                    "changed_by_agent_name": {"type": "text", "analyzer": "brazilian"},
                },
            },
            "audit_logs": {
                "type": "nested",
                "properties": {
                    "entity_type": {"type": "keyword"},
                    "entity_id": {"type": "keyword"},
                    "operation": {"type": "keyword"},
                    "performed_by": {"type": "keyword"},
                    "performed_at": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                    },
                    "details": {"type": "object", "enabled": False},
                },
            },
            "sla_metrics": {
                "properties": {
                    "first_response_time_minutes": {"type": "integer"},
                    "resolution_time_minutes": {"type": "integer"},
                    "first_response_sla_breached": {"type": "boolean"},
                    "resolution_sla_breached": {"type": "boolean"},
                }
            },
            "search_text": {"type": "text", "analyzer": "brazilian"},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "analysis": {
            "analyzer": {
                "brazilian": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "brazilian_stop",
                        "brazilian_stemmer",
                        "asciifolding",
                    ],
                }
            },
            "filter": {
                "brazilian_stop": {"type": "stop", "stopwords": "_brazilian_"},
                "brazilian_stemmer": {"type": "stemmer", "language": "brazilian"},
            },
        },
    },
}


class ElasticClient:

    def __init__(self):
        super().__init__()
        self.elastic_url = os.getenv("ELASTICSEARCH_URL")
        self.elastic_index = os.getenv("ELASTICSEARCH_INDEX")
        self.elastic_user = os.getenv("ELASTICSEARCH_USER")
        self.elastic_password = os.getenv("ELASTICSEARCH_PASSWORD")
        self.es = Elasticsearch(
            self.elastic_url,
            basic_auth=(self.elastic_user, self.elastic_password),
            verify_certs=False,
            ssl_show_warn=False,
        )
        self._ensure_index()

    def _ensure_index(self):
        # Garante que o nome do índice está definido
        if not self.elastic_index:
            # 2. Use o logger para mensagens de erro
            logger.error(
                "[ElasticClient] Variável de ambiente ELASTICSEARCH_INDEX não definida. Índice não será criado."
            )
            return
        # Cria o índice com o mapeamento se não existir
        if not self.es.indices.exists(index=self.elastic_index):
            # 3. Use o logger para mensagens de informação
            logger.info(
                f"[ElasticClient] Índice '{self.elastic_index}' não existe. Criando..."
            )
            self.es.indices.create(index=self.elastic_index, body=INDEX_MAPPING)
        else:
            logger.info(f"[ElasticClient] Índice '{self.elastic_index}' já existe.")

    def upsert_document(self, doc_id, data):
        """
        Executa uma operação de 'update' ou 'insert' (upsert).

        :param doc_id: O ID único do documento no Elasticsearch.
        :param data: O dicionário de dados a ser salvo.
        """
        try:
            response = self.es.update(
                index=self.elastic_index,
                id=doc_id,
                body={
                    "doc": data,
                    "doc_as_upsert": True,
                },
            )
            return response
        except Exception as e:
            print(f"Erro ao fazer upsert do documento {doc_id} no Elasticsearch: {e}")
            return None
