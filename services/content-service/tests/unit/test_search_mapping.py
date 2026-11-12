"""Contract for Content's Elasticsearch document mapping."""

from crawler_content.search.mapping import search_index_mapping


def test_mapping_contains_only_architecture_search_fields():
    mapping = search_index_mapping()
    assert mapping["dynamic"] == "strict"
    assert mapping["properties"] == {
        "page_id": {"type": "keyword"},
        "crawl_id": {"type": "keyword"},
        "url": {"type": "keyword", "ignore_above": 8191},
        "title": {"type": "text", "analyzer": "standard"},
        "text": {"type": "text", "analyzer": "standard"},
        "keywords": {"type": "keyword"},
    }
    assert mapping.get("_source", {}).get("enabled", True) is True


def test_mutating_returned_mapping_does_not_change_next_mapping():
    mapping = search_index_mapping()
    mapping["properties"]["url"]["ignore_above"] = 1
    mapping["properties"]["html"] = {"type": "text"}
    next_mapping = search_index_mapping()
    assert next_mapping["properties"]["url"]["ignore_above"] == 8191
    assert "html" not in next_mapping["properties"]
