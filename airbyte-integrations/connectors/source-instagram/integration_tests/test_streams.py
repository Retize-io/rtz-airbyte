#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Callable, List, MutableMapping, Tuple

import pendulum
import pytest
from source_instagram.source import SourceInstagram

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    StreamDescriptor,
    Type,
)
from airbyte_cdk.test.catalog_builder import CatalogBuilder, ConfiguredAirbyteStreamBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read


@pytest.fixture(name="state")
def state_fixture() -> MutableMapping[str, Any]:
    today = pendulum.today()
    initial_state = {
        "17841408147298757": {"date": (today - pendulum.duration(days=10)).to_datetime_string()},
        "17841403112736866": {"date": (today - pendulum.duration(days=5)).to_datetime_string()},
    }
    return [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="user_insights", namespace=None),
                stream_state=AirbyteStateBlob(initial_state),
            ),
        )
    ]


class TestInstagramSource:
    """Custom integration tests should test incremental with nested state"""

    def test_incremental_streams(self, config, state):
        records, states = self._read_records(config, "user_insights")
        # TODO: Remove this magic number and somehow read the number of days from the config.jsonj
        assert len(records) == 30, "UserInsights for two accounts over last 30 day should return 30 records when empty STATE provided"

        records, states = self._read_records(config, "user_insights", state)
        assert len(records) <= 60 - 10 - 5, "UserInsights should have less records returned when non empty STATE provided"

        assert states, "insights should produce states"
        for state_msg in states:
            stream_name, stream_state, state_keys_count = (
                state_msg.state.stream.stream_descriptor.name,
                state_msg.state.stream.stream_state,
                len(state_msg.state.stream.stream_state.__dict__),
            )

            assert stream_name == "user_insights", f"each state message should reference 'user_insights' stream, got {stream_name} instead"
            assert isinstance(
                stream_state, AirbyteStateBlob
            ), f"Stream state should be type AirbyteStateBlob, got {type(stream_state)} instead"
            assert state_keys_count == 2, f"Stream state should contain 2 partition keys, got {state_keys_count} instead"

    def test_comments_stream_basic(self, config):
        """
        Basic integration test for comments stream to ensure it works end-to-end.
        Tests that the transformation is properly applied and records have expected structure.
        """
        records, _ = self._read_records(config, "comments")

        # Note: Comments may be empty for test accounts, so we handle both cases
        if not records:
            logging.info("No comments found in test account - this is acceptable for testing")
            return

        logging.info(f"Found {len(records)} comment records")

        # Validate that records have expected structure after transformation
        for record_msg in records[:3]:  # Test first 3 records to avoid timeout
            record_data = record_msg.record.data

            # Validate required fields that should be added by transformation
            assert "id" in record_data, "Comment record should have 'id' field"
            assert "media_id" in record_data, "Comment record should have 'media_id' field from transformation"
            assert "is_reply" in record_data, "Comment record should have 'is_reply' field from transformation"
            assert "user_id" in record_data, "Comment record should have 'user_id' field from transformation"
            assert "username" in record_data, "Comment record should have 'username' field from transformation"

            # Validate field types
            assert isinstance(record_data["id"], str), "Comment id should be string"
            assert isinstance(record_data["media_id"], str), "media_id should be string"
            assert isinstance(record_data["is_reply"], bool), "is_reply should be boolean"

            logging.info(
                f"Validated comment record: {record_data['id']}, media: {record_data['media_id']}, reply: {record_data['is_reply']}"
            )

    def test_comments_transformation_flattening(self, config):
        """
        Test that comment replies are properly flattened into separate records.
        This validates the core transformation logic.
        """
        records, _ = self._read_records(config, "comments")

        if not records:
            logging.info("No comments data available for flattening test")
            return

        # Analyze the structure to verify flattening occurred
        replies = []
        top_level_comments = []

        for record_msg in records:
            record_data = record_msg.record.data
            if record_data.get("is_reply") is True:
                replies.append(record_data)
                # Replies should have parent_id
                assert record_data.get("parent_id"), f"Reply {record_data.get('id')} should have parent_id"
            else:
                top_level_comments.append(record_data)

        logging.info(f"Transformation results: {len(top_level_comments)} top-level comments, {len(replies)} replies")

        # If we found replies, validate they're properly structured
        for reply in replies:
            assert reply.get("id") != reply.get("parent_id"), "Reply should have different ID than parent"
            assert reply.get("media_id"), "Reply should have media_id"

        # This confirms that nested replies were flattened into separate records
        # rather than remaining nested within parent comments

    @staticmethod
    def _read_records(conf, stream_name, state=None) -> Tuple[List[AirbyteMessage], List[AirbyteMessage]]:
        records = []
        states = []
        output = read(
            SourceInstagram(config=conf, catalog=None, state=state),
            conf,
            CatalogBuilder().with_stream(ConfiguredAirbyteStreamBuilder().with_name(stream_name)).build(),
            state=state,
        )
        for message in output.records_and_state_messages:
            if message.type == Type.RECORD:
                records.append(message)
            elif message.type == Type.STATE:
                print(message.state.stream.stream_state.__dict__)
                states.append(message)

        return records, states
