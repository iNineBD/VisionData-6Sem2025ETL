import logging
import os
from elasticsearch import Elasticsearch

class ElasticHandler(logging.Handler):
    
    def __init__(self):
        super().__init__()
        self.elastic_url = os.getenv('ELASTICSEARCH_URL')
        self.elastic_index = os.getenv('ELASTICSEARCH_INDEX')
        self.elastic_user = os.getenv('ELASTICSEARCH_USER')
        self.elastic_password = os.getenv('ELASTICSEARCH_PASSWORD')
        self.es = Elasticsearch(
            self.elastic_url,
            basic_auth=(self.elastic_user, self.elastic_password),
            verify_certs=False,
            headers={"Content-Type": "application/json"},
            ssl_show_warn=False
        )

    def emit(self, record):
        try:
            log_entry = self.format(record)
            if isinstance(log_entry, dict):
                self.es.index(index=self.elastic_index, document=log_entry)
            else:
                self.es.index(index=self.elastic_index, document={"message": log_entry})
        except Exception as e:
            print(f"Erro ao tentar enviar log para o Elasticsearch: {e}")