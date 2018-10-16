from eventsourcing.DomainEventListener import ApplicationDomainEventPublisher
from eventsourcing.DomainObject import DomainObject
from eventsourcing.EventSourceRepository import InMemoryEventSourceRepository
from eventsourcing.Projection import InMemoryProjection


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


class AddProjection(InMemoryProjection):

    def __init__(self):
        super().__init__()
        self.projected = 0
        ApplicationDomainEventPublisher().instance.register_listener(self)

    def project(self, obj_id, event_name, event):
        if event_name == "adding":
            print("coucou")
            self.projected += 1
            self.collection.append(event)


projection = AddProjection()


def test_projection():
    projection.projected = 0

    obj = AddDomainObject()
    obj.add(1, 2)
    repo = AddInMemoryRepository()
    repo.save(obj)

    assert projection.projected == 1


def test_projection2():

    obj = AddDomainObject()
    obj.add(1, 2)
    repo = AddInMemoryRepository()
    repo.save(obj)

    assert projection.projected == 2
