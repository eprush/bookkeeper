from bookkeeper.repository.memory_repository import MemoryRepository

import pytest


@pytest.fixture
def custom_class():
    class Custom():
        pk = 0

    return Custom


@pytest.fixture
def repo():
    return MemoryRepository()


def test_get_all_with_condition(repo, custom_class):
    objects = []
    for i in range(5):
        o = custom_class()
        o.name = str(i)
        o.test = 'test'
        repo.add(o)
        objects.append(o)
    assert repo.get_all({'name': '0'}) == [objects[0]]
    assert repo.get_all({'test': 'test'}) == objects
