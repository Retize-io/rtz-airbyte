#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import json
import unittest
from unittest import TestCase

import pytest

from airbyte_cdk.models import SyncMode
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.mock_http import HttpMocker, HttpResponse
from airbyte_cdk.test.mock_http.response_builder import (
    FieldPath,
    HttpResponseBuilder,
    RecordBuilder,
    create_record_builder,
    create_response_builder,
    find_template,
)

from .config import BUSINESS_ACCOUNT_ID, ConfigBuilder
from .pagination import NEXT_PAGE_TOKEN, InstagramPaginationStrategy
from .request_builder import RequestBuilder, get_account_request
from .response_builder import get_account_response
from .utils import config, read_output, read_output_with_parent


PARENT_FIELDS = [
    "caption",
    "comments_count",
    "id",
    "ig_id",
    "is_comment_enabled",
    "like_count",
    "media_type",
    "media_product_type",
    "media_url",
    "owner",
    "permalink",
    "shortcode",
    "thumbnail_url",
    "timestamp",
    "username",
    "children",
]
_PARENT_STREAM_NAME = "media"
_STREAM_NAME = "comments"


MEDIA_ID_GENERAL_MEDIA = "35076616084176123"
MEDIA_ID_ERROR_WITH_WRONG_PERMISSIONS = "35076616084176125"

GENERAL_MEDIA = "general_media"
ERROR_WITH_WRONG_PERMISSIONS = "error_with_wrong_permissions"

_MEDIA_IDS = {
    GENERAL_MEDIA: MEDIA_ID_GENERAL_MEDIA,
    ERROR_WITH_WRONG_PERMISSIONS: MEDIA_ID_ERROR_WITH_WRONG_PERMISSIONS,
}


def _get_parent_request() -> RequestBuilder:
    return RequestBuilder.get_media_endpoint(item_id=BUSINESS_ACCOUNT_ID).with_limit(100).with_fields(PARENT_FIELDS)


def _get_child_request(media_id) -> RequestBuilder:
    return RequestBuilder.get_comments_endpoint(item_id=media_id)


def _get_response(stream_name: str, test: str = None, with_pagination_strategy: bool = True) -> HttpResponseBuilder:
    scenario = ""
    if test:
        scenario = f"_for_{test}"
        print(f"Using scenario: {scenario}, stream_name: {stream_name}, with_pagination_strategy: {with_pagination_strategy}")
    kwargs = {
        "response_template": find_template(f"{stream_name}{scenario}", __file__),
        "records_path": FieldPath("data"),
    }
    if with_pagination_strategy:
        kwargs["pagination_strategy"] = InstagramPaginationStrategy(request=_get_parent_request().build(), next_page_token=NEXT_PAGE_TOKEN)

    return create_response_builder(**kwargs)


def _record(stream_name: str, test: str = None) -> RecordBuilder:
    scenario = ""
    if test:
        scenario = f"_for_{test}"
    return create_record_builder(
        response_template=find_template(f"{stream_name}{scenario}", __file__),
        records_path=FieldPath("data"),
        record_id_path=FieldPath("id"),
    )


class TestFullRefresh(TestCase):
    @staticmethod
    def _read(config_: ConfigBuilder, expecting_exception: bool = False) -> EntrypointOutput:
        return read_output_with_parent(
            config_builder=config_,
            parent_stream_name=_PARENT_STREAM_NAME,
            child_stream_name=_STREAM_NAME,
            sync_mode=SyncMode.full_refresh,
            expecting_exception=expecting_exception,
        )

    @HttpMocker()
    def test_instagram_comments_for_general_media(self, http_mocker: HttpMocker) -> None:
        test = GENERAL_MEDIA
        http_mocker.get(
            get_account_request().build(),
            get_account_response(),
        )
        http_mocker.get(
            _get_parent_request().build(),
            _get_response(stream_name=_PARENT_STREAM_NAME, test=test)
            .with_record(_record(stream_name=_PARENT_STREAM_NAME, test=test))
            .build(),
        )

        # Use template without pagination for this basic test
        http_mocker.get(
            _get_child_request(media_id=MEDIA_ID_GENERAL_MEDIA).build(),
            HttpResponse(json.dumps(find_template(f"{_STREAM_NAME}_for_{test}_no_pagination", __file__)), 200),
        )

        output = self._read(config_=config())
        print("*" * 100)
        print(output.records)

        # Filter to only get comment records, not media records
        comment_records = [r for r in output.records if r.record.stream == "comments"]
        assert len(comment_records) == 3  # Based on the comments_for_general_media.json template

        # Verify first record structure
        first_record = comment_records[0].record.data
        assert first_record["page_id"]
        assert first_record["business_account_id"]
        assert first_record["media_id"]
        assert first_record["id"]
        assert first_record["timestamp"]
        assert first_record["text"]
        assert first_record["from"]
        assert "hidden" in first_record
        assert "like_count" in first_record  # parent_id might be null for root comments, so just check it exists in the schema

        root_comments = [r for r in comment_records if r.record.data.get("parent_id") is None]
        reply_comments = [r for r in comment_records if r.record.data.get("parent_id") is not None]
        assert len(root_comments) == 2
        assert len(reply_comments) == 1

    @HttpMocker()
    def test_instagram_comments_for_error_with_wrong_permissions(self, http_mocker: HttpMocker) -> None:
        test = ERROR_WITH_WRONG_PERMISSIONS
        http_mocker.get(
            get_account_request().build(),
            get_account_response(),
        )
        http_mocker.get(
            _get_parent_request().build(),
            _get_response(stream_name=_PARENT_STREAM_NAME, test="error_with_wrong_permissions")  # Use the proper template
            .with_record(_record(stream_name=_PARENT_STREAM_NAME, test="error_with_wrong_permissions"))
            .build(),
        )

        http_mocker.get(
            _get_child_request(media_id=MEDIA_ID_GENERAL_MEDIA).build(),
            HttpResponse(json.dumps(find_template(f"{_STREAM_NAME}_for_general_media_no_pagination", __file__)), 200),
        )

        output = self._read(config_=config(), expecting_exception=False)

        # Filter to only get comment records, not media records
        comment_records = [r for r in output.records if r.record.stream == "comments"]
        assert len(comment_records) == 3

        for record in comment_records:
            assert record.record.data["media_id"] == MEDIA_ID_GENERAL_MEDIA

    @HttpMocker()
    def test_instagram_comments_basic_functionality(self, http_mocker: HttpMocker) -> None:
        """Test that comments retrieve records correctly from a single media ID"""
        test = GENERAL_MEDIA

        http_mocker.get(
            get_account_request().build(),
            get_account_response(),
        )

        http_mocker.get(
            _get_parent_request().build(),
            _get_response(stream_name=_PARENT_STREAM_NAME, test=test)
            .with_record(_record(stream_name=_PARENT_STREAM_NAME, test=test))
            .build(),
        )

        # Mock comments response (no pagination needed as comments are child streams)
        http_mocker.get(
            _get_child_request(media_id=MEDIA_ID_GENERAL_MEDIA).build(),
            HttpResponse(json.dumps(find_template(f"{_STREAM_NAME}_for_{test}_no_pagination", __file__)), 200),
        )

        output = self._read(config_=config())

        # Filter to only get comment records, not media records
        comment_records = [r for r in output.records if r.record.stream == "comments"]
        assert len(comment_records) == 3  # Should have exactly 3 comments from the template

        # Verify record structure and content
        first_record = comment_records[0].record.data
        assert first_record["page_id"]
        assert first_record["business_account_id"]
        assert first_record["media_id"]
        assert first_record["id"]
        assert first_record["timestamp"]
        assert first_record["text"]
        assert first_record["from"]
        assert "hidden" in first_record
        assert "like_count" in first_record

        # Verify that we have both root comments and replies
        root_comments = [r for r in comment_records if r.record.data.get("parent_id") is None]
        reply_comments = [r for r in comment_records if r.record.data.get("parent_id") is not None]
        assert len(root_comments) == 2
        assert len(reply_comments) == 1


if __name__ == "__main__":
    unittest.main()
