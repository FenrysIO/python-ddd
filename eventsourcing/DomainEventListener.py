import abc
from queue import Empty
from threading import Thread
from multiprocessing import Queue


class DomainEventListener(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def domainEventPublished(self, event):
        raise NotImplementedError()


class AsyncDomainEventListener(Thread, metaclass=abc.ABCMeta):

    def __init__(self):
        Thread.__init__(self)

        self.queue = Queue()
        self.__must_run = True
        self.__is_running = True

    def run(self):
        while self.__must_run:
            try:
                event = self.queue.get(timeout=5)
                self.domainEventPublished(event)
            except Empty:
                pass
            self.post_publish()
        self.__is_running = False

    def terminate(self):
        self.__must_run = False

    def post_publish(self):
        pass

    def is_running(self):
        return self.__is_running

    @abc.abstractmethod
    def domainEventPublished(self, event):
        raise NotImplementedError()


class ApplicationDomainEventPublisher:

    class __ApplicationDomainEventPublisher(DomainEventListener):

        def __init__(self):
            self.__sync_listeners = list()
            self.__async_listeners = list()

        def domainEventPublished(self, event):
            for listener in self.__async_listeners:
                listener.put(event)

            for listener in self.__sync_listeners:
                assert isinstance(listener, DomainEventListener)
                listener.domainEventPublished(event)

        def register_listener(self, obj):
            assert obj is not None
            assert isinstance(obj, DomainEventListener) or\
                   isinstance(obj, AsyncDomainEventListener)

            if isinstance(obj, DomainEventListener):
                self.__sync_listeners.append(obj)
            else:
                self.__async_listeners.append(obj.queue)

        def unregister_listener(self, listener):
            assert listener is not None
            assert isinstance(listener, DomainEventListener) or \
                   isinstance(listener, AsyncDomainEventListener)

            if isinstance(listener, DomainEventListener):
                self.__sync_listeners.remove(listener)
            else:
                self.__async_listeners.remove(listener.queue)

        def contains_listener(self, listener):
            assert listener is not None
            assert isinstance(listener, DomainEventListener) or \
                   isinstance(listener, AsyncDomainEventListener)

            if isinstance(listener, DomainEventListener):
                return listener in self.__sync_listeners
            else:
                return listener.queue in self.__async_listeners

    instance = None

    def __init__(self):
        if ApplicationDomainEventPublisher.instance is None:
            ApplicationDomainEventPublisher.instance = \
                ApplicationDomainEventPublisher.__ApplicationDomainEventPublisher()

    def __getattr__(self, name):
        return getattr(
            ApplicationDomainEventPublisher.instance, name)