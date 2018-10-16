from eventsourcing.DomainObject import DomainObject
from eventsourcing.EventSourceRepository import InMemoryEventSourceRepository


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


def test_simple_add():
    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    repo.save(obj)

    assert repo.get_event_stream_for(obj.object_id) == obj.event_stream


def test_big_add():
    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    repo.save(obj)

    for i in range(0, 10000):
        obj.add(i, i-1)
    repo.save(obj)

    assert repo.get_event_stream_for(obj.object_id) == obj.event_stream


def test_load_add():
    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    repo.save(obj)

    for i in range(0, 10000):
        obj.add(i, i-1)
    repo.save(obj)

    reloaded = repo.load(obj.object_id)
    assert reloaded.event_stream == obj.event_stream


def test_two_load_add():
    obj1 = AddDomainObject()
    obj2 = AddDomainObject()
    repo = AddInMemoryRepository()
    repo.save(obj1)

    for i in range(0, 10000):
        obj1.add(i, i-1)
        if i%2 == 0:
            obj2.add(2, i)
    repo.save(obj1)
    repo.save(obj2)

    reloaded1 = repo.load(obj1.object_id)
    reloaded2 = repo.load(obj2.object_id)

    assert reloaded1.event_stream == obj1.event_stream
    assert reloaded2.event_stream == obj2.event_stream


def test_exists():
    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    repo.save(obj)

    assert repo.exists(obj.object_id)


def test_not_exists():
    obj = AddDomainObject()
    repo = AddInMemoryRepository()
    repo.save(obj)

    assert not repo.exists(obj.object_id + "lol")
