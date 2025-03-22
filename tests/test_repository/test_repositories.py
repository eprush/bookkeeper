from bookkeeper.repository.memory_repository import MemoryRepository
from bookkeeper.repository.sqlite_repository import SQLiteRepository

import pytest
from dataclasses import dataclass

@dataclass(eq=True)
class Custom:
    pk: int = 0

@pytest.fixture()
def custom_class():
    return Custom


# to check the tests with SQLite correctly,
# you need to delete the database ".\tests\test_repository\test_db_file.db" manually
@pytest.mark.parametrize("repo", (MemoryRepository(), SQLiteRepository("test_db_file.db", Custom)))
def test_crud(repo, custom_class):
    obj = custom_class()
    pk = repo.add(obj)
    assert obj.pk == pk
    assert repo.get(pk) == obj
    obj2 = custom_class()
    obj2.pk = pk
    repo.update(obj2)
    assert repo.get(pk) == obj2
    repo.delete(pk)
    assert repo.get(pk) is None


@pytest.mark.parametrize("repo", [MemoryRepository(), SQLiteRepository("test_db_file.db", Custom)])
def test_cannot_add_with_pk(repo, custom_class):
    obj = custom_class()
    obj.pk = 1
    with pytest.raises(ValueError):
        repo.add(obj)


@pytest.mark.parametrize("repo", [MemoryRepository(), SQLiteRepository("test_db_file.db", Custom)])
def test_cannot_add_without_pk(repo):
    with pytest.raises(ValueError):
        repo.add(0)


@pytest.mark.parametrize("repo", [MemoryRepository(), SQLiteRepository("test_db_file.db", Custom)])
def test_cannot_delete_unexistent(repo):
    with pytest.raises(KeyError):
        repo.delete(-1)


@pytest.mark.parametrize("repo", [MemoryRepository(), SQLiteRepository("test_db_file.db", Custom)])
def test_cannot_update_without_pk(repo, custom_class):
    obj = custom_class()
    with pytest.raises(ValueError):
        repo.update(obj)


@pytest.mark.parametrize("repo", [MemoryRepository(), SQLiteRepository("test_db_file.db", Custom)])
def test_get_all(repo, custom_class):
    objects = [custom_class() for _ in range(5)]
    for o in objects:
        repo.add(o)
    assert repo.get_all() == objects
