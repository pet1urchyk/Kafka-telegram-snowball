import logging
import asyncio
import re
from typing import Any

from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import GetChannelRecommendationsRequest

from config import Config

logger = logging.getLogger(__name__)


def _channel_type(channel) -> str:
    """Infer whether a channel object is a broadcast channel or a group."""
    try:
        if getattr(channel, "megagroup", False):
            return "group"
        if getattr(channel, "broadcast", False):
            return "channel"
    except Exception:
        pass
    return "unknown"


async def get_channel_recommendations(
    client,
    channel_entity,
    initial_channel=None,
    depth: int = 0,
    max_depth: int = 2,
    processed_channels: set | None = None,
    edge_list_writer: Any | None = None,
    link_records: list[dict[str, Any]] | None = None,
    run_name: str | None = None,
    entity_records: list[dict[str, Any]] | None = None,
    rate_limiter: asyncio.Semaphore | None = None,
    request_timeout: float | None = 15.0,
    max_flood_wait: int | None = 60,
) -> set:
    """Recursively fetch Telegram channel recommendations starting from a given channel.

    Args:
        client (TelegramClient): The initialized Telegram client.
        channel_entity (str/Channel): The channel entity to process.
        initial_channel (str): The original channel used for naming files.
        depth (int): Current recursion depth.
        max_depth (int): Maximum recursion depth.
        processed_channels (set, optional): Set of processed channel usernames/IDs.
        edge_list_writer (csv.writer or TextIO, optional): Writer for edge list entries.
        link_records (list, optional): Collector for DB link records.
        run_name (str, optional): Current run name for link records.
        entity_records (list, optional): Collector for entity upserts.

    Returns:
        set: Newly discovered channel entities.
    """
    if processed_channels is None:
        processed_channels = set()

    if initial_channel is None:
        initial_channel = channel_entity

    if depth > max_depth:
        return set()

    # Extract channel attributes safely
    try:
        current_channel_username = getattr(channel_entity, 'username', None)
        current_channel_id = getattr(channel_entity, 'id', None)
        current_channel_title = getattr(channel_entity, 'title', None)

        # Convert to string to avoid type issues
        if current_channel_id is not None:
            current_channel_id = str(current_channel_id)

        # Use string representation if attributes not found
        if current_channel_username is None:
            current_channel_username = str(channel_entity)
        if current_channel_id is None:
            current_channel_id = str(channel_entity)
        if current_channel_title is None:
            current_channel_title = str(channel_entity)
    except:
        # Fallback to string representation if any error occurs
        current_channel_id = str(channel_entity)
        current_channel_username = str(channel_entity)
        current_channel_title = str(channel_entity)

    logger.info(
        f"Fetching recommendations for channel: {current_channel_title} (@{current_channel_username}), Depth: {depth}/{max_depth}")

    new_channels = set()
    recommendation_tasks = []

    try:
        async def _do_request():
            return await client(GetChannelRecommendationsRequest(channel=channel_entity))

        async def _fetch_with_limits():
            if rate_limiter:
                async with rate_limiter:
                    if request_timeout:
                        return await asyncio.wait_for(
                            _do_request(), timeout=request_timeout
                        )
                    return await _do_request()
            if request_timeout:
                return await asyncio.wait_for(_do_request(), timeout=request_timeout)
            return await _do_request()

        recommendations = await _fetch_with_limits()

        if hasattr(recommendations, 'chats'):
            for recommended_channel in recommendations.chats:
                # Extract attributes safely
                try:
                    username = getattr(recommended_channel, 'username', None)
                    channel_id = getattr(recommended_channel, 'id', None)
                    title = getattr(recommended_channel, 'title', 'Unknown')

                    # Convert to string if not None
                    if channel_id is not None:
                        channel_id = str(channel_id)

                    if not username and not channel_id:
                        continue

                    channel_identifier = username if username else channel_id

                    if channel_identifier not in processed_channels:
                        processed_channels.add(channel_identifier)
                        new_channels.add(channel_identifier)

                        logger.info(f"Depth {depth}: Recommendation - Title: {title}, Username: @{username}")

                        # Write to edge list if writer exists
                        if edge_list_writer and current_channel_id and channel_id:
                            create_edge_list(
                                edge_list_writer,
                                current_channel_id,
                                current_channel_title,
                                current_channel_username,
                                channel_id,
                                title,
                                username,
                                connection_type="recommendation",
                            )

                        if link_records is not None:
                            link_records.append(
                                {
                                    "run_name": run_name,
                                    "origin_channel_id": current_channel_id,
                                    "origin_channel_name": current_channel_title,
                                    "origin_channel_username": current_channel_username,
                                    "target_id": channel_id,
                                    "target_name": title,
                                    "target_username": username,
                                    "entity_type": _channel_type(recommended_channel),
                                    "link_type": "recommendation",
                                    "message_id": None,
                                    "url": None,
                                    "found_at": None,
                                    "connection_weight": 1,
                                }
                            )

                        if entity_records is not None:
                            entity_records.append(
                                {
                                    "run_name": run_name,
                                    "entity_id": channel_id,
                                    "name": title,
                                    "username": username,
                                    "entity_type": _channel_type(recommended_channel),
                                }
                            )

                        # Only continue if we haven't reached max depth
                        if depth < max_depth:
                            # Schedule retrieval of recommendations for the next depth level
                            recommendation_tasks.append(
                                get_channel_recommendations(
                                    client,
                                    recommended_channel,
                                    initial_channel,
                                    depth + 1,
                                    max_depth,
                                    processed_channels,
                                    edge_list_writer
                                )
                            )
                except Exception as ex:
                    logger.warning(f"Error processing recommended channel: {ex}")
                    continue

        # Process recommendations concurrently
        if recommendation_tasks:
            results = await asyncio.gather(*recommendation_tasks)
            for result in results:
                new_channels.update(result)

    except FloodWaitError as e:
        wait_seconds = e.seconds
        if max_flood_wait is not None:
            wait_seconds = min(wait_seconds, max_flood_wait)
        logger.warning(
            "Flood wait for recommendations on %s (@%s). Respecting wait for %s seconds, "
            "then retrying once.",
            current_channel_title,
            current_channel_username,
            wait_seconds,
        )
        await asyncio.sleep(wait_seconds)
        try:
            recommendations = await _fetch_with_limits()
            if hasattr(recommendations, "chats"):
                for recommended_channel in recommendations.chats:
                    try:
                        username = getattr(recommended_channel, "username", None)
                        channel_id = getattr(recommended_channel, "id", None)
                        title = getattr(recommended_channel, "title", "Unknown")
                        if channel_id is not None:
                            channel_id = str(channel_id)
                        channel_identifier = username if username else channel_id
                        if not channel_identifier:
                            continue
                        if channel_identifier not in processed_channels:
                            processed_channels.add(channel_identifier)
                            new_channels.add(channel_identifier)
                            if link_records is not None and current_channel_id:
                                link_records.append(
                                    {
                                        "run_name": run_name,
                                        "origin_channel_id": current_channel_id,
                                        "origin_channel_name": current_channel_title,
                                        "origin_channel_username": current_channel_username,
                                        "target_id": channel_id,
                                        "target_name": title,
                                        "target_username": username,
                                        "entity_type": _channel_type(recommended_channel),
                                        "link_type": "recommendation",
                                        "message_id": None,
                                        "url": None,
                                        "found_at": None,
                                        "connection_weight": 1,
                                    }
                                )
                            if entity_records is not None:
                                entity_records.append(
                                    {
                                        "run_name": run_name,
                                        "entity_id": channel_id,
                                        "name": title,
                                        "username": username,
                                        "entity_type": _channel_type(recommended_channel),
                                    }
                                )
                        # Do not recurse here; main loop will enqueue
                    except Exception as ex:
                        logger.warning("Error processing recommended channel: %s", ex)
        except Exception as retry_ex:
            logger.warning(
                "Retry after flood wait failed for %s (@%s): %s",
                current_channel_title,
                current_channel_username,
                retry_ex,
            )

    except asyncio.TimeoutError:
        logger.warning(
            "Timed out fetching recommendations for %s (@%s); skipping.",
            current_channel_title,
            current_channel_username,
        )

    except Exception as e:
        logger.warning(
            "Error retrieving recommendations for channel %s (@%s): %s",
            current_channel_title,
            current_channel_username,
            e,
        )
        if Config.DEBUG:
            import traceback
            logger.error(traceback.format_exc())

    return new_channels


async def extract_urls_from_message(message):
    """Extract all URLs from a message."""
    if message and message.message:
        return re.findall(r'(https?://\S+)', message.message)
    return []


async def process_urls(
    client,
    channel_entity,
    edge_list_writer: Any,
    url_file: Any | None = None,
) -> set:
    """Process messages in a channel to extract and log outbound URLs.

    Args:
        client (TelegramClient): The initialized Telegram client.
        channel_entity (str/Channel): The channel entity to process.
        edge_list_writer (csv.writer or TextIO): Writer for edge list entries.
        url_file (file, optional): Open file handle to write URLs to.

    Returns:
        set: Set of extracted URLs.
    """
    # Extract channel attributes safely
    try:
        current_channel_id = getattr(channel_entity, 'id', None)
        current_channel_title = getattr(channel_entity, 'title', None)
        current_channel_username = getattr(channel_entity, 'username', None)

        # Convert ID to string
        if current_channel_id is not None:
            current_channel_id = str(current_channel_id)

        # Use string representation if attributes not found
        if current_channel_id is None:
            current_channel_id = str(channel_entity)
        if current_channel_title is None:
            current_channel_title = str(channel_entity)
        if current_channel_username is None:
            current_channel_username = str(channel_entity)
    except:
        # Fallback to string representation
        current_channel_id = str(channel_entity)
        current_channel_title = str(channel_entity)
        current_channel_username = str(channel_entity)

    url_set = set()

    try:
        async for message in client.iter_messages(channel_entity, limit=Config.DEFAULT_MAX_POSTS):
            urls = await extract_urls_from_message(message)

            for url in urls:
                url_set.add(url)

                # Write to edge list if writer exists and IDs are valid
                if edge_list_writer and current_channel_id:
                    # For URLs, use the URL as the target ID and "External URL" as the target name
                    create_edge_list(
                        edge_list_writer,
                        current_channel_id,
                        current_channel_title,
                        current_channel_username,
                        url,  # URL as ID
                        "External URL",  # Generic name
                        None,  # No username for external URLs
                        connection_type="outbound_link",
                    )

                # Write to URL file if provided
                if url_file:
                    url_file.write(f"{url}\n")
                    url_file.flush()

    except Exception as e:
        logger.error(f"Error processing URLs for channel {current_channel_username}: {e}")
        if Config.DEBUG:
            import traceback
            logger.error(traceback.format_exc())

    return url_set
