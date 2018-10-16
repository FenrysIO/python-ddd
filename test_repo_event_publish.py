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


def test_simple_add():
    listener = AddDomainEventListener()

    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    repo.register_listener(listener)
    repo.save(obj)

    assert listener.nb_events == 1


def test_big_add():
    listener = AddDomainEventListener()

    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    repo.register_listener(listener)
    repo.save(obj)
    assert listener.nb_events == 1

    for i in range(0, 10000):
        obj.add(i, i-1)
    repo.save(obj)
    assert listener.nb_events == 10001
