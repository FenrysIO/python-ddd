import abc
from collections import Iterable
from copy import deepcopy
from .DomainEventListener import DomainEventListener, ApplicationDomainEventPublisher
from .DomainObject import DomainObject
from pymongo import MongoClient
import pymysql.cursors
import json


class Repository(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def load(self, object_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def exists(self, object_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def save(self, obj):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_event_stream_for(self, object_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def max_version_for_object(self, object_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def create_blank_domain_object(self):
        raise NotImplementedError()


class EventPublisherRepository(Repository, metaclass=abc.ABCMeta):
    def __init__(self):
        self.listeners = list()
        self.register_listener(ApplicationDomainEventPublisher().instance)

    def save(self, obj):
        to_emit = self.append_to_stream(obj)

        assert to_emit is not None
        assert isinstance(to_emit, Iterable)

        for event in to_emit:
            for listener in self.listeners:
                assert isinstance(listener, DomainEventListener)
                listener.domainEventPublished(event)

    def register_listener(self, listener):
        assert listener is not None
        assert isinstance(listener, DomainEventListener)

        if listener not in self.listeners:
            self.listeners.append(listener)

    @abc.abstractmethod
    def append_to_stream(self, obj):
        raise NotImplementedError()


class MongoEventSourceRepository(EventPublisherRepository, metaclass=abc.ABCMeta):
    def __init__(
        self, host="localhost", port=27017, database="fenrys", collection="event_store"
    ):
        super().__init__()
        self.__client = MongoClient(host, port)
        self.__db = self.__client[database]
        self.__collection = self.__db[collection]

    def append_to_stream(self, obj):
        assert obj is not None
        assert isinstance(obj, DomainObject)

        max_known_version = self.max_version_for_object(obj.object_id)

        events_to_add = list()
        if obj.version_number > max_known_version:
            for event in obj.event_stream:
                if event["version"] > max_known_version:
                    events_to_add.append(deepcopy(event))

        if len(events_to_add) > 0:
            self.__collection.insert_many(events_to_add)

        return deepcopy(events_to_add)

    def load(self, object_id):
        obj = self.create_blank_domain_object()
        assert isinstance(obj, DomainObject)

        stream = self.get_event_stream_for(object_id)
        obj.rehydrate(stream)

        return obj

    def exists(self, object_id):
        return len(self.get_event_stream_for(object_id)) > 0

    def get_event_stream_for(self, object_id):
        stream = list()

        objects = self.__collection.find({"object_id": object_id})
        for event in objects:
            event.pop("_id")
            stream.append(event)

        return stream

    def max_version_for_object(self, object_id):
        max_known_version = 0
        stream = self.get_event_stream_for(object_id)

        for event in stream:
            if event["version"] > max_known_version:
                max_known_version = event["version"]

        return max_known_version


class MySQLSourceRepository(EventPublisherRepository, metaclass=abc.ABCMeta):

    __CREATE_STREAM = """create table `{}`(`object_id` varchar(255) not null, `version` int not null, `event_name` varchar(255) not null, `event` longtext not null, `event_timestamp` double not null, primary key(`object_id`, `version`))"""
    __SELECT_OBJECT_STREAM = "select * from `{}` where object_id = %s"
    __INSERT_OBJECT_STREAM = "insert into `{}`(`object_id`, `version`, `event_name`, `event`, `event_timestamp`) values(%s, %s, %s, %s, %s)"
    __CHECK_TABLE_EXISTS = "show tables like %s"
    __TABLE_EXISTS = False

    def __init__(
        self,
        user="fenrys",
        password="fenrys",
        host="localhost",
        database="fenrys",
        table="event_store",
    ):
        super().__init__()
        self.__connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
        self.__table = table

        self.__create_table()

    def __del__(self):
        self.__connection.close()

    def __create_table(self):
        if not self.__table_exists():
            try:
                with self.__connection.cursor() as cursor:
                    cursor.execute(
                        MySQLSourceRepository.__CREATE_STREAM.format(self.__table)
                    )
                self.__connection.commit()
            except Exception as e:
                self.__connection.rollback()
                raise e

    def __table_exists(self):
        if not MySQLSourceRepository.__TABLE_EXISTS:
            with self.__connection.cursor() as cursor:
                cursor.execute(
                    MySQLSourceRepository.__CHECK_TABLE_EXISTS, (self.__table)
                )
                result = cursor.fetchone()
                if result:
                    MySQLSourceRepository.__TABLE_EXISTS = True
                    return True
                else:
                    return False
        else:
            return True

    def append_to_stream(self, obj):
        assert obj is not None
        assert isinstance(obj, DomainObject)

        max_known_version = self.max_version_for_object(obj.object_id)

        events_to_add = list()
        if obj.version_number > max_known_version:
            for event in obj.event_stream:
                if event["version"] > max_known_version:
                    events_to_add.append(deepcopy(event))

        if len(events_to_add) > 0:
            try:
                with self.__connection.cursor() as cursor:
                    cursor.executemany(
                        MySQLSourceRepository.__INSERT_OBJECT_STREAM.format(
                            self.__table
                        ),
                        map(
                            lambda event: (
                                event["object_id"],
                                int(event["version"]),
                                event["event_name"],
                                json.dumps(event["event"]),
                                "{:10.15f}".format(float(event["event_timestamp"])),
                            ),
                            events_to_add,
                        ),
                    )
                self.__connection.commit()
            except Exception as e:
                self.__connection.rollback()
                raise e

        return deepcopy(events_to_add)

    def load(self, object_id):
        obj = self.create_blank_domain_object()
        assert isinstance(obj, DomainObject)

        stream = self.get_event_stream_for(object_id)
        obj.rehydrate(stream)

        return obj

    def exists(self, object_id):
        return len(self.get_event_stream_for(object_id)) > 0

    def get_event_stream_for(self, object_id):
        stream = list()

        with self.__connection.cursor() as cursor:
            cursor.execute(
                MySQLSourceRepository.__SELECT_OBJECT_STREAM.format(self.__table),
                (object_id),
            )
            results = cursor.fetchall()
            for result in results:
                r = dict()
                r["object_id"] = result["object_id"]
                r["version"] = int(result["version"])
                r["event_name"] = result["event_name"]
                r["event"] = json.loads(result["event"])
                r["event_timestamp"] = float(result["event_timestamp"])
                stream.append(r)

        return stream

    def max_version_for_object(self, object_id):
        stream = self.get_event_stream_for(object_id)

        return max(map(lambda x: x["version"], stream)) if len(stream) > 0 else 0


class InMemoryEventSourceRepository(EventPublisherRepository, metaclass=abc.ABCMeta):
    def __init__(self):
        super().__init__()
        self.__repo = list()

    def append_to_stream(self, obj):
        assert obj is not None
        assert isinstance(obj, DomainObject)

        max_known_version = self.max_version_for_object(obj.object_id)

        events_to_add = list()
        if obj.version_number > max_known_version:
            for event in obj.event_stream:
                if event["version"] > max_known_version:
                    events_to_add.append(event)
                    self.__repo.append(event)

        return deepcopy(events_to_add)

    def load(self, object_id):
        obj = self.create_blank_domain_object()
        assert isinstance(obj, DomainObject)

        stream = self.get_event_stream_for(object_id)
        obj.rehydrate(stream)

        return obj

    def exists(self, object_id):
        return len(self.get_event_stream_for(object_id)) > 0

    def get_event_stream_for(self, object_id):
        stream = list()
        for event in self.__repo:
            if event["object_id"] == object_id:
                stream.append(event)
        return stream

    def max_version_for_object(self, object_id):
        max_known_version = 0
        stream = self.get_event_stream_for(object_id)

        for event in stream:
            if event["version"] > max_known_version:
                max_known_version = event["version"]

        return max_known_version
