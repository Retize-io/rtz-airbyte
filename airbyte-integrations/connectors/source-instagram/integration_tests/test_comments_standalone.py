#!/usr/bin/env python3
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

"""
Standalone integration test runner for Instagram Comments functionality.
This script can be run independently to test just the comments integration.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

from source_instagram.source import SourceInstagram

from airbyte_cdk.models import Type
from airbyte_cdk.test.catalog_builder import CatalogBuilder, ConfiguredAirbyteStreamBuilder
from airbyte_cdk.test.entrypoint_wrapper import read


# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_config() -> Dict[str, Any]:
    """Load configuration from secrets/config.json."""
    config_path = Path("secrets/config.json")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        return json.load(f)


def test_comments_integration():
    """Run a comprehensive test of the comments integration."""
    try:
        # Load config
        config = load_config()
        logger.info("Configuration loaded successfully")

        # Setup source and catalog
        source = SourceInstagram(config=config, catalog=None, state=None)
        catalog = CatalogBuilder().with_stream(ConfiguredAirbyteStreamBuilder().with_name("comments")).build()

        logger.info("Starting comments integration test...")

        # Read records
        output = read(source, config, catalog, state=None)

        records = []
        states = []

        for message in output.records_and_state_messages:
            if message.type == Type.RECORD:
                records.append(message)
            elif message.type == Type.STATE:
                states.append(message)

        logger.info(f"Retrieved {len(records)} comment records")

        if not records:
            logger.warning("No comment records found - this may be expected if there are no comments in the test account")
            return True

        # Validate first few records
        validation_errors = []

        for i, record_msg in enumerate(records[:5]):
            record_data = record_msg.record.data

            # Basic validation
            if "id" not in record_data:
                validation_errors.append(f"Record {i}: Missing 'id' field")

            if "media_id" not in record_data:
                validation_errors.append(f"Record {i}: Missing 'media_id' field")

            if "is_reply" not in record_data:
                validation_errors.append(f"Record {i}: Missing 'is_reply' field")

            if "user_id" not in record_data:
                validation_errors.append(f"Record {i}: Missing 'user_id' field")

            if "username" not in record_data:
                validation_errors.append(f"Record {i}: Missing 'username' field")

            logger.info(
                f"Record {i}: ID={record_data.get('id', 'N/A')}, "
                f"MediaID={record_data.get('media_id', 'N/A')}, "
                f"IsReply={record_data.get('is_reply', 'N/A')}, "
                f"User={record_data.get('username', 'N/A')}"
            )

        if validation_errors:
            logger.error("Validation errors found:")
            for error in validation_errors:
                logger.error(f"  {error}")
            return False

        # Check for flattening evidence
        replies = [r for r in records if r.record.data.get("is_reply") is True]
        top_level = [r for r in records if r.record.data.get("is_reply") is False]

        logger.info(f"Found {len(top_level)} top-level comments and {len(replies)} replies")

        # Validate reply structure
        for reply_msg in replies:
            reply_data = reply_msg.record.data
            if not reply_data.get("parent_id"):
                validation_errors.append(f"Reply {reply_data.get('id', 'unknown')} missing parent_id")

        if validation_errors:
            logger.error("Reply validation errors:")
            for error in validation_errors:
                logger.error(f"  {error}")
            return False

        logger.info("✅ Comments integration test PASSED")
        return True

    except Exception as e:
        logger.error(f"❌ Comments integration test FAILED: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_comments_integration()
    sys.exit(0 if success else 1)
