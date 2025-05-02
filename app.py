#!/usr/bin/env python
"""
Azure Event Hub Real-time UI

A web application that displays real-time data from Azure Event Hub in charts.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from threading import Thread

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
import pandas as pd
from dotenv import load_dotenv

from event_hub_processor import EventHubProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Flask app and SocketIO
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-for-testing')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# Initialize event hub processor
processor = None
event_processor_thread = None

def initialize_processor():
    """Initialize the Event Hub processor"""
    global processor
    if processor is None:
        try:
            processor = EventHubProcessor()
            
            # Check if we have the required connection info
            if not processor.connection_string and not processor.namespace:
                return False, "Missing Event Hub connection string or namespace"
            if not processor.eventhub_name:
                return False, "Missing Event Hub name"
                
            # Register callback for real-time updates
            processor.register_callback(process_and_emit_event)
            return True, "Processor initialized successfully"
        except Exception as e:
            logger.error(f"Error initializing processor: {e}", exc_info=True)
            return False, str(e)
    return True, "Processor already initialized"

async def process_and_emit_event(event):
    """Process an event and emit it to connected clients"""
    try:
        # Prepare data for the chart
        chart_data = prepare_chart_data(event)
        
        # Emit to all connected clients
        socketio.emit('new_data', chart_data)
        
        logger.debug(f"Emitted event: {chart_data}")
    except Exception as e:
        logger.error(f"Error processing and emitting event: {e}", exc_info=True)

def prepare_chart_data(event):
    """
    Prepare event data for charting
    
    Args:
        event: The event data from Event Hub
        
    Returns:
        Dictionary with formatted data for the chart
    """
    # Create a copy of the event to avoid modifying the original
    processed_event = {}
    
    # Process each key-value pair to ensure JSON serializability
    for key, value in event.items():
        # Handle bytes objects by decoding them to UTF-8 strings
        if isinstance(value, bytes):
            try:
                processed_event[key] = value.decode('utf-8')
            except UnicodeDecodeError:
                # If it's not valid UTF-8, use base64 encoding as fallback
                import base64
                processed_event[key] = base64.b64encode(value).decode('ascii')
        else:
            processed_event[key] = value
    
    # Get time or use current time
    try:
        time = processed_event.get('time', datetime.now(timezone.utc).isoformat())
        if isinstance(time, str):
            # Convert ISO format to datetime if needed
            time = pd.to_datetime(time).isoformat().replace('+00:00', 'Z')
    except:
        time = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        
    # Extract numerical values for charting - with improved detection
    values = {}
    
    # Case 1: Handle nested values in 'data' field
    if 'data' in processed_event and isinstance(processed_event['data'], dict):
        for nested_key, nested_value in processed_event['data'].items():
            if isinstance(nested_value, (int, float)):
                values[nested_key] = nested_value
    
    # Case 2: Direct numerical values at the top level
    for key, value in processed_event.items():
        if isinstance(value, (int, float)) and key != 'time':
            values[key] = value
    
    # If no values found, log this for debugging
    if not values:
        logger.warning(f"No chartable values found in event: {processed_event}")
    
    return {
        'time': time,
        'values': values,
        'raw_data': processed_event
    }

def start_event_processor():
    """Start the event processor in a separate thread"""
    global event_processor_thread
    
    def run_processor():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(processor.receive_events())
        
    if event_processor_thread is None or not event_processor_thread.is_alive():
        event_processor_thread = Thread(target=run_processor, daemon=True)
        event_processor_thread.start()
        return True
    return False

@app.route('/')
def index():
    """Render the main dashboard page"""
    return render_template('index.html')

@app.route('/api/start', methods=['POST'])
def api_start():
    """API endpoint to start the event processor"""
    success, message = initialize_processor()
    if success:
        if start_event_processor():
            return jsonify({"status": "success", "message": "Event processor started"})
        return jsonify({"status": "success", "message": "Event processor was already running"})
    return jsonify({"status": "error", "message": message})

@app.route('/api/data/recent', methods=['GET'])
def api_recent_data():
    """API endpoint to get recent data"""
    if processor is None:
        return jsonify({"status": "error", "message": "Event processor not initialized"})
        
    # Get limit parameter or use default
    limit = request.args.get('limit', default=100, type=int)
    
    # Get recent events
    events = processor.get_recent_events(limit)
    
    # Format for the chart
    formatted_events = [prepare_chart_data(event) for event in events]
    
    return jsonify({
        "status": "success", 
        "data": formatted_events
    })

@app.errorhandler(404)
def page_not_found(e):
    """Handle 404 errors"""
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    """Handle 500 errors"""
    return render_template('500.html'), 500

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    logger.info(f"Client connected: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    logger.info(f"Client disconnected: {request.sid}")
    
if __name__ == '__main__':
    # Initialize processor at startup (not required, can be started via API)
    initialize_processor()
    
    # Start socketio server
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', 5000))
    
    logger.info(f"Starting server on {host}:{port}")
    socketio.run(app, host=host, port=port, debug=True)