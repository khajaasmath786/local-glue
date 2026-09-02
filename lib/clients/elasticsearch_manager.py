import logging
import sys
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from elasticsearch.exceptions import TransportError
from requests_aws4auth import AWS4Auth
from lib.utils.es_utils import ESUtils

class ElasticSearchManager:
    """
    Manages Elasticsearch interactions, including creating
    indices, inserting documents, and performing bulk inserts
    with logging.
    """

    DEFAULT_SETTINGS = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
            }
        },
    }

    def __init__(self, index_name, host, port, access_key,
                 secret_key, settings=None, region="us-east-2",
                 service="es"):
        """
        Initializes the manager with Elasticsearch connection
        settings.
        :param index_name: Name of the Elasticsearch index.
        :param host: Host address of the Elasticsearch server.
        :param port: Port number of the Elasticsearch server.
        :param access_key: AWS access key for authentication.
        :param secret_key: AWS secret key for authentication.
        :param settings: Settings and mappings for the index.
        :param region: AWS region of the Elasticsearch service.
        :param service: Service code for AWS Elasticsearch.
        """
        self.index_name = index_name
        awsauth = AWS4Auth(access_key, secret_key, region, service)
        self.client = Elasticsearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=awsauth,
            maxsize=50 * 1024 * 1024,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30
        )
        self.settings = settings if settings else self.DEFAULT_SETTINGS
        self.logger = logging.getLogger(self.__class__.__name__)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.logger.info(f"Initialized for index {self.index_name}")

    def index_exists(self):
        """
        Check if the index already exists in Elasticsearch.
        :return: True if exists, False otherwise.
        """
        try:
            exists = self.client.indices.exists(index=self.index_name)
            self.logger.debug(f"Index exists check: {exists}")
            return exists
        except TransportError as e:
            self.logger.error(f"Transport error: {e}")
            return False

    def create_index(self):
        """
        Creates an index in Elasticsearch if it does not exist.
        :return: True if successful or already exists, False on error.
        """
        if not self.index_exists():
            try:
                self.client.indices.create(index=self.index_name,
                                           body=self.settings)
                self.logger.info("Index created successfully.")
                return True
            except TransportError as e:
                self.logger.error(f"Transport error: {e}")
                return False
        else:
            self.logger.info("Index already exists.")
            return True

    def bulk_insert(self, documents):
        """
        Performs a bulk insert of documents into the index.
        :param documents: A list of documents to insert.
        """
        try:
            actions = [{'_index': self.index_name, '_source': doc}
                       for doc in documents]
            helpers.bulk(self.client, actions)
            self.logger.info("Bulk insert completed successfully.")
        except helpers.BulkIndexError as e:
            self.logger.error(f"Bulk index error: {e}")

    def insert_documents(self, documents, index_name, ids):
        """
        Performs a bulk insert of documents into the index using custom
        payload construction.
        :param documents: A list of documents to insert.
        :param index_name: The name of the index.
        :param ids: List of fields to use for document IDs.
        """
        es_utils = ESUtils()
        try:
            action = {"update": {"_index": index_name}}
            payload = es_utils.payload_constructor(documents, action, ids)
            self.bulk_insert_with_payload(payload, index_name)
            self.logger.info("Documents inserted successfully.")
        except helpers.BulkIndexError as e:
            self.logger.error(f"Bulk insert error: {e}")

    def count_documents(self):
        """
        Counts the number of documents in the Elasticsearch index.
        """
        try:
            count = self.client.count(index=self.index_name)['count']
            self.logger.info(f"Document count in index {self.index_name}: {count}")
            return count
        except TransportError as e:
            self.logger.error(f"Transport error: {e}")
            return 0

    def bulk_insert_with_payload(self, payload, index_name):
        """
        Performs a bulk insert using the payload constructed from ESUtils.
        :param payload: The payload constructed for bulk insert.
        :param index_name: The name of the index.
        """
        try:
            response = self.client.bulk(body=payload, index=index_name)
            self.logger.info(f"Bulk insert with payload completed successfully. Response: {response}")
            if response.get('errors'):
                for item in response['items']:
                    if item.get('update') and item['update'].get('error'):
                        self.logger.error(f"Error in bulk insert: {item['update']['error']}")
        except helpers.BulkIndexError as e:
            self.logger.error(f"Bulk insert error: {e}")

    def insert_document(self, documents, id_fields=None):
        """
        Inserts multiple documents into Elasticsearch with optional
        document IDs. Uses bulk indexing for efficient large-scale
        document insertion.
        :param documents: A list of dictionaries, each representing
                          a document to insert.
        :param id_fields: Optional. A list of fields in each document
                          dict to use for creating the document ID in
                          Elasticsearch. If not specified, IDs are
                          auto-generated.
        """
        actions = []

        for doc in documents:
            doc_id_parts = []
            if id_fields:
                for field in id_fields:
                    try:
                        doc_id_parts.append(str(doc[field]))
                    except KeyError:
                        self.logger.error(f"Field '{field}' not found in document: {doc}")
                doc_id = "_".join(doc_id_parts) if doc_id_parts else None
            else:
                doc_id = None

            action = {
                "_op_type": "index",
                "_index": self.index_name,
                "_id": doc_id,
                "_source": doc,
                "doc_as_upsert": True
            }
            actions.append(action)

        try:
            helpers.bulk(self.client, actions)
            self.logger.info("Bulk insert completed successfully.")
        except helpers.BulkIndexError as e:
            self.logger.error(f"Bulk insert error: {e}")

    def close_connection(self):
        """
        Closes the Elasticsearch client connection.
        """
        self.client.close()
        self.logger.info("Elasticsearch connection closed.")