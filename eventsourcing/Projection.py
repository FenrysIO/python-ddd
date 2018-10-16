from pymongo import MongoClient
from .DomainEventListener import DomainEventListener
import abc


class Projection(DomainEventListener):

    def domainEventPublished(self, event):
        obj_id = event["object_id"]
        event_name = event["event_name"]
        event = event["event"]

        self.project(obj_id, event_name, event)

    @abc.abstractmethod
    def project(self, obj_id, event_name, event):
        raise NotImplementedError()


class MongoProjection(Projection):

    def __init__(self, host="localhost", port=27017, database="fenrys", collection="event_store"):
        super().__init__()
        self.__client = MongoClient(host, port)
        self.__db = self.__client[database]
        self.collection = self.__db[collection]

    @abc.abstractmethod
    def project(self, obj_id, event_name, event):
        raise NotImplementedError()


class InMemoryProjection(Projection):

    def __init__(self):
        super().__init__()
        self.collection = list()

    @abc.abstractmethod
    def project(self, obj_id, event_name, event):
        raise NotImplementedError()
