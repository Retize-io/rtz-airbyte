# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import Dict

from records import (
    breakdowns_record,
    children_record,
    clear_url_record,
    clear_url_record_transformed,
    expected_breakdown_record_transformed,
    expected_children_transformed,
    insights_record,
    insights_record_transformed,
)
from source_instagram.components import (
    GRAPH_URL,
    InstagramBreakDownResultsTransformation,
    InstagramClearUrlTransformation,
    InstagramCommentsTransformation,
    InstagramDateFilterTransformation,
    InstagramInsightsTransformation,
    InstagramMediaChildrenTransformation,
)


def mock_path(requests_mock, path: str, method: str = "GET", response: Dict = None):
    complete_url = f"{GRAPH_URL}/{path}"
    requests_mock.register_uri(method, complete_url, json=response)


def test_instagram_media_children_transformation(requests_mock, config):
    params = "?fields=id,ig_id,media_type,media_url,owner,permalink,shortcode,thumbnail_url,timestamp,username"
    children_record_data = children_record["children"]["data"]
    expected_children_transformed_data = expected_children_transformed["children"]
    for index in range(len(children_record_data)):
        mock_path(requests_mock, path=f"{children_record_data[index]['id']}{params}", response=expected_children_transformed_data[index])

    record_transformation = InstagramMediaChildrenTransformation()
    transformation_result = record_transformation.transform(children_record, config)
    assert transformation_result == expected_children_transformed


def test_instagram_clear_url_transformation():
    record_transformation = InstagramClearUrlTransformation().transform(clear_url_record)
    assert record_transformation == clear_url_record_transformed


def test_break_down_results_transformation():
    record_transformation_result = InstagramBreakDownResultsTransformation().transform(breakdowns_record)
    assert record_transformation_result == expected_breakdown_record_transformed


def test_instagram_insights_transformation(config):
    record_transformation = InstagramInsightsTransformation().transform(insights_record)
    assert record_transformation == insights_record_transformed


def test_instagram_comments_transformation():
    """Test the comments transformation that flattens nested comment structure"""
    test_comment = {
        "id": "comment123",
        "text": "Great post!",
        "timestamp": "2024-01-01T12:00:00+0000",
        "like_count": 5,
        "hidden": False,
        "parent_id": None,
        "from": {"id": "user123", "username": "testuser"},
        "replies": {"data": [{"id": "reply123", "text": "I agree!", "parent_id": "comment123"}]},
    }

    kwargs = {"stream_partition": {"media_insights_info": {"media_id": "media123"}}}

    record_transformation = InstagramCommentsTransformation()
    result = record_transformation.transform(test_comment, config=None, **kwargs)

    # Check that basic fields are preserved
    assert result["id"] == "comment123"
    assert result["text"] == "Great post!"
    assert result["is_reply"] == False
    assert result["post_id"] == "media123"

    # Check that from field is flattened
    assert result["from_id"] == "user123"
    assert result["from_username"] == "testuser"

    # Check that replies field is removed (handled by API field expansion)
    assert "replies" not in result


def test_instagram_date_filter_transformation():
    """Test the date filter transformation"""
    from datetime import datetime, timedelta

    # Test record within date range
    recent_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    recent_record = {"id": "recent123", "timestamp": recent_date}

    # Test record outside date range
    old_date = (datetime.now() - timedelta(days=40)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    old_record = {"id": "old123", "timestamp": old_date}

    # Test with start_date config
    config = {"start_date": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")}

    transformation = InstagramDateFilterTransformation()

    # Recent record should pass through
    result_recent = transformation.transform(recent_record, config=config)
    assert result_recent is not None
    assert result_recent["id"] == "recent123"

    # Old record should be filtered out
    result_old = transformation.transform(old_record, config=config)
    assert result_old is None
