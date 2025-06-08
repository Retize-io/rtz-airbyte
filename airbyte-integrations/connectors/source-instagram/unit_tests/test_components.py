# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import Dict

from records import (
    breakdowns_record,
    children_record,
    clear_url_record,
    clear_url_record_transformed,
    comments_record_missing_from,
    comments_record_no_replies,
    comments_record_with_replies,
    expected_breakdown_record_transformed,
    expected_children_transformed,
    expected_comments_transformed_missing_from,
    expected_comments_transformed_no_replies,
    expected_comments_transformed_with_replies,
    insights_record,
    insights_record_transformed,
)
from source_instagram.components import (
    GRAPH_URL,
    InstagramBreakDownResultsTransformation,
    InstagramClearUrlTransformation,
    InstagramCommentsTransformation,
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


def test_instagram_comments_transformation_with_replies():
    """Test comment transformation with nested replies"""
    record_transformation = InstagramCommentsTransformation()
    transformation_result = record_transformation.transform(comments_record_with_replies)
    assert transformation_result == expected_comments_transformed_with_replies

    # Verify that we get 3 records: 1 comment + 2 replies
    assert len(transformation_result) == 3

    # Verify the main comment
    main_comment = transformation_result[0]
    assert main_comment["id"] == "comment_123"
    assert main_comment["is_reply"] is False
    assert "user_id" in main_comment
    assert "username" in main_comment
    assert "from" not in main_comment
    assert "replies" not in main_comment
    assert "extracted_at" not in main_comment

    # Verify the replies
    reply1 = transformation_result[1]
    assert reply1["id"] == "reply_456"
    assert reply1["is_reply"] is True
    assert reply1["parent_id"] == "comment_123"

    reply2 = transformation_result[2]
    assert reply2["id"] == "reply_789"
    assert reply2["is_reply"] is True
    assert reply2["parent_id"] == "comment_123"


def test_instagram_comments_transformation_no_replies():
    """Test comment transformation without replies"""
    record_transformation = InstagramCommentsTransformation()
    transformation_result = record_transformation.transform(comments_record_no_replies)
    assert transformation_result == expected_comments_transformed_no_replies

    # Verify that we get only 1 record
    assert len(transformation_result) == 1

    # Verify the comment structure
    comment = transformation_result[0]
    assert comment["id"] == "comment_999"
    assert comment["is_reply"] is False
    assert comment["user_id"] == "user_999"
    assert comment["username"] == "alice_wonder"
    assert "from" not in comment
    assert "replies" not in comment


def test_instagram_comments_transformation_missing_from_field():
    """Test comment transformation with missing 'from' field"""
    record_transformation = InstagramCommentsTransformation()
    transformation_result = record_transformation.transform(comments_record_missing_from)
    assert transformation_result == expected_comments_transformed_missing_from

    # Verify that we get 1 record
    assert len(transformation_result) == 1

    # Verify that missing 'from' field results in empty user_id and username
    comment = transformation_result[0]
    assert comment["user_id"] == ""
    assert comment["username"] == ""


def test_instagram_comments_transformation_return_type():
    """Test that the transformation returns a list (not iterator)"""
    record_transformation = InstagramCommentsTransformation()
    transformation_result = record_transformation.transform(comments_record_with_replies)

    # Verify that result is a list
    assert isinstance(transformation_result, list)

    # Verify that each item in the list is a dict
    for item in transformation_result:
        assert isinstance(item, dict)
