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
    assert table.cluster_keys is None


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
    assert table.cluster_keys is None


def test_table_with_only_cluster_keys():
    """Test creating a table with only cluster keys works fine"""
    table = Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="test_table",
        columns=[ColumnDefinitions(name="col1", type="String")],
        cluster_keys=["date"],
    )
    assert table.cluster_keys == ["date"]
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
        cluster_keys=["id"],
    )
    assert table.partition_keys == ["date"]
    assert table.cluster_keys == ["id"]


def test_table_with_overlapping_keys():
    """Test that creating a table with overlapping keys raises an error"""
    with pytest.raises(
        ValueError, match="Cluster keys cannot overlap with partition keys"
    ):
        Table(
            id=1,
            schema_id=1,
            database_id=1,
            name="test_table",
            columns=[ColumnDefinitions(name="col1", type="String")],
            partition_keys=["date"],
            cluster_keys=["date"],
        )


def test_table_with_multiple_overlapping_keys():
    """Test that creating a table with multiple overlapping keys raises an error"""
    with pytest.raises(
        ValueError, match="Cluster keys cannot overlap with partition keys"
    ):
        Table(
            id=1,
            schema_id=1,
            database_id=1,
            name="test_table",
            columns=[ColumnDefinitions(name="col1", type="String")],
            partition_keys=["date", "id"],
            cluster_keys=["date", "other"],
        )
