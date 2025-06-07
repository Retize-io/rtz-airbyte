#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, List, MutableMapping, Tuple

import pytest
from source_instagram.source import SourceInstagram

from airbyte_cdk.models import (
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    Type,
)
from airbyte_cdk.test.catalog_builder import CatalogBuilder, ConfiguredAirbyteStreamBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read


logger = logging.getLogger(__name__)


class TestInstagramStreamsComprehensive:
    """Comprehensive integration tests for all Instagram streams"""

    @staticmethod
    def _read_records(conf, stream_name, state=None) -> Tuple[List[AirbyteMessage], List[AirbyteMessage]]:
        """Helper method to read records from a stream"""
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
                states.append(message)

        return records, states

    def test_api_stream(self, config):
        """Test the Api stream - should return account information"""
        records, states = self._read_records(config, "Api")

        # Should have at least one account record
        assert len(records) >= 1, "Api stream should return at least one account record"

        # Check record structure
        first_record = records[0].record.data
        assert "id" in first_record, "Api record should have 'id' field"

        logger.info(f"Api stream returned {len(records)} records")

    def test_users_stream(self, config):
        """Test the users stream - should return user profile information"""
        records, states = self._read_records(config, "users")

        # Should have at least one user record (the business account)
        assert len(records) >= 1, "Users stream should return at least one user record"

        # Check record structure
        first_record = records[0].record.data
        expected_fields = ["id", "username", "followers_count", "follows_count", "media_count"]
        for field in expected_fields:
            assert field in first_record, f"Users record should have '{field}' field"

        logger.info(f"Users stream returned {len(records)} records")

    def test_media_stream(self, config):
        """Test the media stream - should return media posts"""
        records, states = self._read_records(config, "media")

        # Note: May return 0 records if account has no media
        logger.info(f"Media stream returned {len(records)} records")

        if len(records) > 0:
            # Check record structure if records exist
            first_record = records[0].record.data
            expected_fields = ["id", "media_type", "timestamp"]
            for field in expected_fields:
                assert field in first_record, f"Media record should have '{field}' field"

    def test_media_insights_stream(self, config):
        """Test the media_insights stream - should return insights for media posts"""
        records, states = self._read_records(config, "media_insights")

        # Note: May return 0 records if account has no media or no insights available
        logger.info(f"Media insights stream returned {len(records)} records")

        if len(records) > 0:
            # Check record structure if records exist
            first_record = records[0].record.data
            expected_fields = ["id", "page_id", "business_account_id"]
            for field in expected_fields:
                assert field in first_record, f"Media insights record should have '{field}' field"

    def test_media_comments_stream(self, config):
        """Test the media_comments stream - should return comments on media posts"""
        records, states = self._read_records(config, "media_comments")

        # Note: May return 0 records if account has no media with comments
        logger.info(f"Media comments stream returned {len(records)} records")

        if len(records) > 0:
            # Check record structure if records exist
            first_record = records[0].record.data
            expected_fields = ["id", "text", "timestamp"]
            for field in expected_fields:
                assert field in first_record, f"Media comments record should have '{field}' field"

    def test_stories_stream(self, config):
        """Test the stories stream - should return Instagram stories"""
        records, states = self._read_records(config, "stories")

        # Note: Stories are ephemeral and may not exist
        logger.info(f"Stories stream returned {len(records)} records")

        if len(records) > 0:
            # Check record structure if records exist
            first_record = records[0].record.data
            expected_fields = ["id", "media_type", "timestamp"]
            for field in expected_fields:
                assert field in first_record, f"Stories record should have '{field}' field"

    def test_story_insights_stream(self, config):
        """Test the story_insights stream - should return insights for stories"""
        records, states = self._read_records(config, "story_insights")

        # Note: May return 0 records if no stories exist
        logger.info(f"Story insights stream returned {len(records)} records")

        if len(records) > 0:
            # Check record structure if records exist
            first_record = records[0].record.data
            expected_fields = ["id", "page_id", "business_account_id"]
            for field in expected_fields:
                assert field in first_record, f"Story insights record should have '{field}' field"

    def test_user_lifetime_insights_stream(self, config):
        """Test the user_lifetime_insights stream - should return lifetime user insights"""
        records, states = self._read_records(config, "user_lifetime_insights")

        # Should have records for each breakdown (city, country, age,gender)
        assert len(records) >= 1, "User lifetime insights should return at least one record"

        # Check record structure
        first_record = records[0].record.data
        expected_fields = ["business_account_id", "breakdown"]
        for field in expected_fields:
            assert field in first_record, f"User lifetime insights record should have '{field}' field"

        logger.info(f"User lifetime insights stream returned {len(records)} records")

    def test_all_streams_basic_functionality(self, config):
        """Test that all streams can be called without errors"""
        stream_names = ["Api", "users", "media", "media_insights", "media_comments", "stories", "story_insights", "user_lifetime_insights"]

        results = {}
        for stream_name in stream_names:
            try:
                records, states = self._read_records(config, stream_name)
                results[stream_name] = {"success": True, "record_count": len(records), "state_count": len(states)}
                logger.info(f"Stream '{stream_name}': {len(records)} records, {len(states)} states")
            except Exception as e:
                results[stream_name] = {"success": False, "error": str(e)}
                logger.error(f"Stream '{stream_name}' failed: {e}")

        # All streams should succeed
        failed_streams = [name for name, result in results.items() if not result["success"]]
        assert len(failed_streams) == 0, f"Failed streams: {failed_streams}"

        # Api and users streams should always have records
        assert results["Api"]["record_count"] >= 1, "Api stream should have at least 1 record"
        assert results["users"]["record_count"] >= 1, "Users stream should have at least 1 record"

        logger.info("All streams tested successfully")

    def test_stream_schemas_validation(self, config):
        """Test that all streams return records with consistent schemas"""
        stream_names = ["Api", "users", "media", "media_insights", "media_comments", "stories", "story_insights", "user_lifetime_insights"]

        for stream_name in stream_names:
            records, _ = self._read_records(config, stream_name)

            if len(records) > 0:
                # Check that all records in a stream have consistent primary key fields
                first_record = records[0].record.data
                primary_key_field = "id" if "id" in first_record else None

                if primary_key_field:
                    for record in records:
                        assert (
                            primary_key_field in record.record.data
                        ), f"All records in {stream_name} should have {primary_key_field} field"
                        assert (
                            record.record.data[primary_key_field] is not None
                        ), f"Primary key {primary_key_field} should not be null in {stream_name}"

                logger.info(f"Schema validation passed for {stream_name} with {len(records)} records")

    def test_connection_check(self, config):
        """Test that the source can successfully check connection"""
        source = SourceInstagram(config=config, catalog=None, state=None)
        success, error = source.check_connection(logger, config)

        assert success, f"Connection check should succeed, but got error: {error}"
        logger.info("Connection check passed successfully")

    def test_discover_streams(self, config):
        """Test that source discovery returns all expected streams"""
        source = SourceInstagram(config=config, catalog=None, state=None)
        catalog = source.discover(logger, config)

        stream_names = {stream.name for stream in catalog.streams}
        expected_streams = {
            "media",
            "media_insights",
            "media_comments",
            "users",
            "user_lifetime_insights",
            "stories",
            "story_insights",
            "Api",
            "user_insights",
        }

        assert expected_streams.issubset(stream_names), f"Missing streams: {expected_streams - stream_names}"

        logger.info(f"Discovered {len(stream_names)} streams: {sorted(stream_names)}")
