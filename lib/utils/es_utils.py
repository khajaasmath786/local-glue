"""
    Utility class for common Elasticsearch/OpenSearch operations.
"""
from datetime import date, datetime
import json
from lib.common.logger import Logger


class ESUtils:
    """
    Utility class for common Elasticsearch/OpenSearch operations.
    """

    def __init__(self):
        self.logger = Logger(self.__class__.__name__)

    @staticmethod
    def convert_to_list(input_data):
        """
        Convert a string of comma-separated values or a list to a list.

        :param input_data: str or list
        :return: list
        :raises ValueError: if input_data is neither a string nor a list
        """
        if isinstance(input_data, str):
            return [item.strip() for item in input_data.split(',')]
        elif isinstance(input_data, list):
            return input_data
        else:
            raise ValueError("Input must be a string or a list")

    @staticmethod
    def serialize_datetime(obj):
        """
        Serialize datetime objects to ISO format.

        :param obj: datetime or date
        :return: str
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return obj

    @staticmethod
    def split_list(lst, chunk_size):
        """
        Split a list into chunks of specified size.

        :param lst: list
        :param chunk_size: int
        :return: list of lists
        """
        chunks = [[] for _ in range((len(lst) + chunk_size - 1) // chunk_size)]
        for i, item in enumerate(lst):
            chunks[i // chunk_size].append(item)
        return chunks

    def payload_constructor(self, data, action, ids):
        """
        Construct payload for bulk indexing in OpenSearch.

        :param data: list of dicts
        :param action: dict
        :param ids: list of str
        :return: str
        """
        payload_string = ""
        ids = self.convert_to_list(ids)
        for doc_num, info in enumerate(data):
            index_id = ""
            for indx_id in ids:
                if indx_id in info:
                    index_id += str(info[indx_id]).replace(" ", "")
                else:
                    self.logger.error(f"""Field '{indx_id}' not found
                                      in document: {info}""")
            action["update"]["_id"] = index_id
            action_string = json.dumps(action,
                                       default=self.serialize_datetime) + "\n"
            payload_string += action_string
            this_line = json.dumps({"doc": info, "doc_as_upsert": True},
                                   default=self.serialize_datetime) + "\n"
            payload_string += this_line
            self.logger.info(f"Document {doc_num}: {index_id}")
        return payload_string
