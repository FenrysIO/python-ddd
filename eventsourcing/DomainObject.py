"""
This package is aimed to manage a DDD domain object with event sourcing

"""
from collections import Iterable
from multiprocessing import Lock
import datetime
import uuid
import json


class DomainObject:
    """
    The domain object with event sourcing

    It should be able to :
    - mutate from a received event
    - store events together with their version number
    - store its version
    - be rehydrated from its events
    """

    def __init__(self):
        """
        Initialises this with it's first event that should be a kind of ObjectCreatedEvent
        """

        self.object_id = "{}-{}".format(self.__class__.__name__, str(uuid.uuid4()))
        self.version_number = 0
        self.event_stream = list()
        self.lock = Lock()
        self.mutate("DomainObjectCreated", {"id": self.object_id})

    def mutate(self, event_name, event):
        """
        Add an event to the stream of events

        :param event: the received event. That object must be JSON serializable.
        :param event_name: The name of the received event. Must be a not None string
        :raise ValueError: if event is not JSON serializable
        """
        assert event_name is not None
        assert isinstance(event_name, str)

        if not self.__is_json_serializable(event):
            raise ValueError("Event must be JSON serializable")

        self.lock.acquire()

        self.version_number += 1
        self.event_stream.append({
            "object_id": self.object_id,
            "version": self.version_number,
            "event_name": event_name,
            "event": event,
            "event_timestamp": datetime.datetime.now().timestamp()})

        self.lock.release()

        self.__apply_event(event_name, event)

    def rehydrate(self, event_list):
        """
        Rehydrate the object from it's event list

        :param event_list: the list of events for rehydratation
        """
        assert isinstance(event_list, Iterable)

        event_list.sort(key=lambda x: x["version"])

        self.lock.acquire()

        self.__clear_stream()
        for event in event_list:
            if event["version"] < self.version_number:
                raise ValueError("Rehydrated version number is {} but actual version number is {}".format(
                    event["version"],
                    self.version_number))

            self.__apply_event(event["event_name"], event["event"])

            self.version_number += 1
            self.object_id = event["object_id"]
            self.event_stream.append({
                "object_id": self.object_id,
                "version": event["version"],
                "event_name": event["event_name"],
                "event": event["event"],
                "event_timestamp": event["event_timestamp"]})

        self.lock.release()

    def __clear_stream(self):
        self.event_stream = list()
        self.version_number = 0

    def __apply_event(self, event_name, event):
        function_name = "on_{}".format(event_name)
        if function_name in self.__dir__():
            getattr(self, function_name)(event)

    @staticmethod
    def __is_json_serializable(event):
        assert event is not None
        try:
            json.dumps(event)
            return True
        except:
            return False


