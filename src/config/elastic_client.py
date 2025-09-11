import os
from elasticsearch import Elasticsearch

class ElasticClient:
    
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
            ssl_show_warn=False
        )
        self.elastic_index = self.elastic_index

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
                    "doc_as_upsert": True  # Se o doc não existir, ele insere o conteúdo de "doc".
                }
            )
            return response
        except Exception as e:
            print(f"Erro ao fazer upsert do documento {doc_id} no Elasticsearch: {e}")
            return None