# Azure Event Hub Real-time UI

A Python web application that provides a real-time UI with charts for visualizing Azure Event Hub data.

## Features

- Real-time visualization of Azure Event Hub data using interactive charts
- Support for multiple data series in a single chart
- Configurable time window and chart types
- Automatic scaling of Y-axis
- Real-time metrics display (message count, latest/min/max values)
- Raw data display for debugging

## Requirements

- Python 3.8+
- Azure Event Hub instance
- Event Hub connection string or credentials for Azure Identity authentication

## Installation

1. Clone this repository or download the source code.

2. Install the required packages:

```bash
pip install -r requirements.txt
```

3. Configure your Azure Event Hub connection by editing the `.env` file:

```
# Azure Event Hub Connection Settings
EVENTHUB_CONNECTION_STRING=your_connection_string_here
EVENTHUB_NAME=your_event_hub_name
EVENTHUB_CONSUMER_GROUP=$Default

# Optional Azure authentication using managed identity or service principal
# AZURE_TENANT_ID=
# AZURE_CLIENT_ID=
# AZURE_CLIENT_SECRET=

# If using Event Hub namespace and authentication directly
# EVENTHUB_NAMESPACE=your_namespace
# EVENTHUB_SAS_POLICY=
# EVENTHUB_SAS_KEY=
```

## Running the Application

Start the application:

```bash
python app.py
```

The application will be available at `http://localhost:5000`.

## Usage

1. Open the application in your web browser.
2. Click the "Connect to Event Hub" button to start receiving data.
3. The chart will display real-time data from your Event Hub.
4. Use the controls to adjust the chart settings:
   - Time window: Sets how much historical data to display (in seconds)
   - Chart type: Choose between line and bar charts
   - Auto-scale Y-axis: Automatically adjust the Y-axis scale based on the data

## Security Considerations

- The application uses Azure Identity DefaultAzureCredential which supports multiple authentication methods including Managed Identity, environment variables, and Visual Studio Code credentials.
- For production use, consider:
  - Using Azure Key Vault for storing secrets
  - Implementing proper authentication for the web UI
  - Deploying in a secure Azure environment

## Architecture

The application consists of these main components:

1. **Event Hub Processor**: Connects to Azure Event Hub and processes incoming events.
2. **Flask Web Application**: Serves the UI and handles API requests.
3. **Socket.IO**: Provides real-time updates between the server and client.
4. **Chart.js**: Renders the interactive charts in the browser.

## Data Format

The application expects Event Hub messages to be in JSON format. Numeric values will be automatically extracted for charting.

Example message format:
```json
{
  "time": "2025-05-01T12:34:56.789Z",
  "temperature": 22.5,
  "humidity": 45.2,
  "status": "normal"
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.