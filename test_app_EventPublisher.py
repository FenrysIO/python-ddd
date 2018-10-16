import time

from eventsourcing.DomainEventListener import ApplicationDomainEventPublisher, AsyncDomainEventListener
from eventsourcing.DomainObject import DomainObject
from eventsourcing.EventSourceRepository import InMemoryEventSourceRepository, DomainEventListener


class AddDomainObject(DomainObject):
    def __init__(self):
        super().__init__()
        self.value = 0

    def add(self, a, b):
        self.mutate("adding", a + b)

    def on_adding(self, event):
        self.value = event


class AddInMemoryRepository(InMemoryEventSourceRepository):

    def __init__(self):
        super().__init__()

    def create_blank_domain_object(self):
        return AddDomainObject()


class AddDomainEventListener(DomainEventListener):

    def __init__(self):
        self.nb_events = 0

    def domainEventPublished(self, event):
        self.nb_events += 1


class AsyncAddDomainEventListener(AsyncDomainEventListener):

    def __init__(self):
        super().__init__()
        self.nb_events = 0

    def domainEventPublished(self, event):
        self.nb_events += 1


def test_simple_add():
    listener = AddDomainEventListener()

    ApplicationDomainEventPublisher().instance.register_listener(listener)

    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    repo.save(obj)

    assert listener.nb_events == 1

    ApplicationDomainEventPublisher().instance.unregister_listener(listener)


def test_big_add():
    listener = AddDomainEventListener()

    ApplicationDomainEventPublisher().instance.register_listener(listener)

    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    repo.save(obj)
    assert listener.nb_events == 1

    for i in range(0, 10000):
        obj.add(i, i-1)
    repo.save(obj)
    assert listener.nb_events == 10001

    ApplicationDomainEventPublisher().instance.unregister_listener(listener)


def test_big_add_after():
    listener = AddDomainEventListener()

    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    ApplicationDomainEventPublisher().instance.register_listener(listener)
    repo.save(obj)
    assert listener.nb_events == 1

    for i in range(0, 10000):
        obj.add(i, i-1)
    repo.save(obj)
    assert listener.nb_events == 10001

    ApplicationDomainEventPublisher().instance.unregister_listener(listener)


def test_async():
    listener = AsyncAddDomainEventListener()
    listener.start()

    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    ApplicationDomainEventPublisher().instance.register_listener(listener)
    repo.save(obj)

    time.sleep(5)

    assert listener.nb_events == 1

    for i in range(0, 10000):
        obj.add(i, i-1)
    repo.save(obj)

    time.sleep(5)

    assert listener.nb_events == 10001

    listener.terminate()
    listener.join()

    ApplicationDomainEventPublisher().instance.unregister_listener(listener)


def test_registration():
    listener = AsyncAddDomainEventListener()
    listener.start()

    ApplicationDomainEventPublisher().instance.register_listener(listener)
    assert ApplicationDomainEventPublisher().instance.contains_listener(listener)

    listener.terminate()
    listener.join()

    ApplicationDomainEventPublisher().instance.unregister_listener(listener)
    assert not(ApplicationDomainEventPublisher().instance.contains_listener(listener))

    listener = AddDomainEventListener()
    ApplicationDomainEventPublisher().instance.register_listener(listener)
    assert ApplicationDomainEventPublisher().instance.contains_listener(listener)

    ApplicationDomainEventPublisher().instance.unregister_listener(listener)
    assert not(ApplicationDomainEventPublisher().instance.contains_listener(listener))