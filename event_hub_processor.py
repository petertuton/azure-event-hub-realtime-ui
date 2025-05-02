#!/usr/bin/env python
"""
Azure Event Hub Processor
Connects to Azure Event Hub and processes incoming messages.

This module handles the connection to Azure Event Hub, event processing,
and real-time data forwarding to the web UI.
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, List, Any

from azure.eventhub import EventData
from azure.eventhub import EventHubConsumerClient
from azure.eventhub.aio import EventHubConsumerClient as AsyncEventHubConsumerClient
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class EventHubProcessor:
    """
    Azure Event Hub message processor.
    
    This class connects to an Azure Event Hub and processes incoming messages.
    It maintains an in-memory cache of recent events and provides an interface
    for real-time data streaming.
    """
    
    def __init__(self, connection_string=None, eventhub_name=None, 
                 consumer_group=None, credential=None, namespace=None):
        """
        Initialize the Event Hub processor.
        
        Args:
            connection_string: The Event Hub connection string
            eventhub_name: The name of the Event Hub
            consumer_group: The consumer group to use
            credential: Azure credential object (for token-based auth)
            namespace: Event Hub namespace (for token-based auth)
        """
        # Try to get connection info from environment if not provided
        self.connection_string = connection_string or os.getenv("EVENTHUB_CONNECTION_STRING")
        self.eventhub_name = eventhub_name or os.getenv("EVENTHUB_NAME")
        self.consumer_group = consumer_group or os.getenv("EVENTHUB_CONSUMER_GROUP", "$Default")
        
        # For managed identity or service principal auth
        self.namespace = namespace or os.getenv("EVENTHUB_NAMESPACE")
        
        # Setup client based on authentication method
        if self.connection_string:
            logger.info(f"Creating Event Hub client for {self.eventhub_name} using connection string")
            self.client = EventHubConsumerClient.from_connection_string(
                self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name
            )
            self.async_client = AsyncEventHubConsumerClient.from_connection_string(
                self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name
            )
        elif self.namespace:
            logger.info(f"Creating Event Hub client for {self.eventhub_name} using DefaultAzureCredential")
            self.credential = credential or DefaultAzureCredential()
            self.client = EventHubConsumerClient(
                fully_qualified_namespace=f"{self.namespace}.servicebus.windows.net",
                eventhub_name=self.eventhub_name,
                consumer_group=self.consumer_group,
                credential=self.credential
            )
            self.async_client = AsyncEventHubConsumerClient(
                fully_qualified_namespace=f"{self.namespace}.servicebus.windows.net",
                eventhub_name=self.eventhub_name,
                consumer_group=self.consumer_group,
                credential=self.credential
            )
        else:
            raise ValueError(
                "Either connection string or namespace with DefaultAzureCredential must be provided"
            )
        
        # Data storage - in-memory cache of recent events
        self.events_cache = []
        self.max_cache_size = 1000
        
        # Callback function to be set by the web app
        self.on_event_callback = None
        
    async def receive_events(self):
        """
        Start receiving events from the Event Hub asynchronously.
        """
        logger.info(f"Starting to receive events from {self.eventhub_name}")
        
        async def on_event_batch(partition_context, events):
            logger.info(f"Received {len(events)} events from partition {partition_context.partition_id}")
            
            for event in events:
                # Process each event
                await self.process_event(event)
                
            # Update checkpoint
            await partition_context.update_checkpoint()
                
        try:
            async with self.async_client:
                await self.async_client.receive_batch(
                    on_event_batch=on_event_batch,
                    starting_position="-1",  # Start from the end
                )
        except Exception as e:
            logger.error(f"Error receiving events: {e}", exc_info=True)
            raise
            
    async def process_event(self, event):
        """
        Process a single event from Event Hub
        
        Args:
            event: The Event Hub event to process
        """
        try:
            # Parse the event body
            event_body = event.body_as_json()
                        
            if isinstance(event_body, list) and event_body:
                event_body = event_body[0]
            
            # Add time if not present
            if 'time' not in event_body:
                event_body['time'] = datetime.now(timezone.utc).isoformat()
                
            # Store event in cache
            self.events_cache.append(event_body)
            
            # Trim cache if needed
            if len(self.events_cache) > self.max_cache_size:
                self.events_cache = self.events_cache[-self.max_cache_size:]
                
            # Call the callback if registered
            if self.on_event_callback:
                await self.on_event_callback(event_body)
                
            logger.debug(f"Processed event: {event_body}")
            return event_body
            
        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)
            # Don't raise, continue processing other events
            return None
            
    def register_callback(self, callback):
        """
        Register a callback function to be called when new events are received
        
        Args:
            callback: Async function that takes an event as parameter
        """
        self.on_event_callback = callback
        
    def get_recent_events(self, limit=100):
        """
        Get the most recent events from the cache
        
        Args:
            limit: Maximum number of events to return
            
        Returns:
            List of recent events, newest first
        """
        return self.events_cache[-limit:]

# Example standalone usage
if __name__ == "__main__":
    processor = EventHubProcessor()
    
    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logger.info("Shutdown signal received, closing Event Hub client")
        loop.stop()
        
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        loop.run_until_complete(processor.receive_events())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        loop.close()