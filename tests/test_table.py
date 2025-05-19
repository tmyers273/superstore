import pytest

from classes import ColumnDefinitions, Table


def test_table_without_keys():
    """Test creating a table without any keys works fine"""
    table = Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="test_table",
        columns=[ColumnDefinitions(name="col1", type="String")],
    )
    assert table.partition_keys is None
    assert table.sort_keys is None


def test_table_with_only_partition_keys():
    """Test creating a table with only partition keys works fine"""
    table = Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="test_table",
        columns=[ColumnDefinitions(name="col1", type="String")],
        partition_keys=["date"],
    )
    assert table.partition_keys == ["date"]
    assert table.sort_keys is None


def test_table_with_only_sort_keys():
    """Test creating a table with only sort keys works fine"""
    table = Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="test_table",
        columns=[ColumnDefinitions(name="col1", type="String")],
        sort_keys=["date"],
    )
    assert table.sort_keys == ["date"]
    assert table.partition_keys is None


def test_table_with_different_keys():
    """Test creating a table with different partition and cluster keys works fine"""
    table = Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="test_table",
        columns=[ColumnDefinitions(name="col1", type="String")],
        partition_keys=["date"],
        sort_keys=["id"],
    )
    assert table.partition_keys == ["date"]
    assert table.sort_keys == ["id"]


def test_table_with_overlapping_keys():
    """Test that creating a table with overlapping keys raises an error"""
    with pytest.raises(
        ValueError, match="Sort keys cannot overlap with partition keys"
    ):
        Table(
            id=1,
            schema_id=1,
            database_id=1,
            name="test_table",
            columns=[ColumnDefinitions(name="col1", type="String")],
            partition_keys=["date"],
            sort_keys=["date"],
        )


def test_table_with_multiple_overlapping_keys():
    """Test that creating a table with multiple overlapping keys raises an error"""
    with pytest.raises(
        ValueError, match="Sort keys cannot overlap with partition keys"
    ):
        Table(
            id=1,
            schema_id=1,
            database_id=1,
            name="test_table",
            columns=[ColumnDefinitions(name="col1", type="String")],
            partition_keys=["date", "id"],
            sort_keys=["date", "other"],
        )
