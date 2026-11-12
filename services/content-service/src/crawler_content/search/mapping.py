"""Elasticsearch mapping for the rebuildable Content projection."""


def search_index_mapping() -> dict[str, object]:
    """Return an independent mapping for one Content search index.

    The URL remains intact in Elasticsearch's default _source, even if it is
    too long to index safely as a keyword. 8191 characters fit within
    Lucene's 32766-byte term limit even at four UTF-8 bytes per character.
    """
    return {
        "dynamic": "strict",
        "properties": {
            "page_id": {"type": "keyword"},
            "crawl_id": {"type": "keyword"},
            "url": {"type": "keyword", "ignore_above": 8191},
            "title": {"type": "text", "analyzer": "standard"},
            "text": {"type": "text", "analyzer": "standard"},
            "keywords": {"type": "keyword"},
        },
    }
