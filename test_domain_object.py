from eventsourcing.DomainObject import DomainObject


class AddDomainObject(DomainObject):
    def __init__(self):
        super().__init__()
        self.value = 0

    def add(self, a, b):
        self.mutate("adding", a + b)

    def on_adding(self, event):
        self.value = event


def test_creation():
    test_object = AddDomainObject()
    assert test_object.version_number == 1
    assert len(test_object.event_stream) == 1
    assert test_object.event_stream[0]["object_id"] == test_object.object_id
    assert test_object.event_stream[0]["version"] == 1
    assert test_object.event_stream[0]["event_name"] == "DomainObjectCreated"
    assert test_object.event_stream[0]["event"] == {"id": test_object.object_id}


def test_one_event():
    test_object = AddDomainObject()

    test_object.add(2,3)

    assert test_object.version_number == 2
    assert len(test_object.event_stream) == 2
    assert test_object.event_stream[-1]["object_id"] == test_object.object_id
    assert test_object.event_stream[-1]["version"] == 2
    assert test_object.event_stream[-1]["event_name"] == "adding"
    assert test_object.event_stream[-1]["event"] == 5


def test_some_events():
    test_object = AddDomainObject()

    for i in range(0, 1000):
        test_object.add(2, 3)

    assert test_object.version_number == 1001
    assert len(test_object.event_stream) == 1001
    assert test_object.event_stream[-1]["object_id"] == test_object.object_id
    assert test_object.event_stream[-1]["version"] == 1001
    assert test_object.event_stream[-1]["event_name"] == "adding"
    assert test_object.event_stream[-1]["event"] == 5


def test_rehydrate_events():
    test_object = AddDomainObject()

    for i in range(0, 1000):
        test_object.add(2, 3)

    stream = test_object.event_stream
    test_object2 = AddDomainObject()
    test_object2.rehydrate(stream)

    assert test_object.version_number == test_object2.version_number
    assert len(test_object.event_stream) == len(test_object2.event_stream)
    assert test_object.event_stream == test_object2.event_stream
    assert test_object2.object_id == test_object2.event_stream[-1]["object_id"]