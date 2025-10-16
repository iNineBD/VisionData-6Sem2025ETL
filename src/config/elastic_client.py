import logging
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from .singleton_conn_elastic import SingletonConnElastic

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


class ElasticClient(logging.Handler, metaclass=SingletonConnElastic):

    def __init__(self):
        super().__init__()

        self.internal_logger = logging.getLogger(__name__)
        self.internal_logger.setLevel(logging.INFO)

        self.elastic_url = os.getenv("ELASTICSEARCH_URL")
        self.elastic_index = os.getenv("ELASTICSEARCH_INDEX")
        self.log_index = os.getenv("ELASTICSEARCH_LOG_INDEX")
        self.elastic_user = os.getenv("ELASTICSEARCH_USER")
        self.elastic_password = os.getenv("ELASTICSEARCH_PASSWORD")

        self.es = Elasticsearch(
            self.elastic_url,
            basic_auth=(self.elastic_user, self.elastic_password),
            verify_certs=False,
            ssl_show_warn=False,
        )

        self._ensure_etl_index()
        self._checked_log_indices = set()

    def _ensure_etl_index(self):
        if not self.elastic_index:
            self.internal_logger.error(
                "Environment variable ELASTICSEARCH_INDEX not defined."
            )
            return
        if not self.es.indices.exists(index=self.elastic_index):
            self.internal_logger.info(
                f"ETL index '{self.elastic_index}' does not exist. Creating..."
            )
            self.es.indices.create(index=self.elastic_index, body=INDEX_MAPPING)

    def bulk_upsert(self, documents):
        """
        ETL method. Sends data to the `elastic_index`.
        """
        actions = []
        for doc in documents:
            doc_id = doc.get("ticket_id")
            if not doc_id:
                self.internal_logger.warning(f"Document without ticket_id found: {doc}")
                continue
            action = {
                "_op_type": "update",
                "_index": self.elastic_index,
                "_id": doc_id,
                "doc": doc,
                "doc_as_upsert": True,
            }
            actions.append(action)
        if not actions:
            return True, []
        try:
            success, errors = bulk(
                self.es, actions, raise_on_error=False, raise_on_exception=False
            )
            if errors:
                self.internal_logger.error(f"Bulk upsert failures in ETL: {errors[:5]}")
            return success, errors
        except Exception as e:
            self.internal_logger.error(
                f"Exception in ETL bulk upsert: {e}", exc_info=True
            )
            return False, [str(e)]

    def emit(self, record):
        """
        Logger method. Sends data to the `log_index`.
        """
        if not self.log_index:
            print(
                "ERROR: Environment variable ELASTICSEARCH_LOG_INDEX not defined. Log will not be sent."
            )
            return

        if self.log_index not in self._checked_log_indices:
            if not self.es.indices.exists(index=self.log_index):
                try:
                    self.es.indices.create(index=self.log_index)
                except Exception:
                    pass
            self._checked_log_indices.add(self.log_index)

        try:
            log_entry = self.format(record)
            actions = [{"_index": self.log_index, "_source": log_entry}]
            bulk(self.es, actions, raise_on_error=False, raise_on_exception=False)
        except Exception as e:
            print(f"ERROR: Exception when sending log to Elasticsearch: {e}")
