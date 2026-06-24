# PPPoE Configuration Worker

Kafka consumer that automatically configures PPPoE credentials on ONUs using GenieACS TR-069.

## Overview

This service listens to GenieACS boot events from Kafka, retrieves PPPoE user credentials from the device provisioning API, and automatically configures the ONU with the correct PPPoE username and password via GenieACS.

## Workflow

1. **Listen**: Consume messages from `genieacs_boot` Kafka topic
2. **Lookup**: Fetch PPPoE user data by ONU serial number from API
3. **Configure**: Set PPPoE credentials on ONU via GenieACS TR-069
4. **Update**: Store ACS device ID in the database
5. **Notify**: Send Gotify notifications and publish results to Kafka

## Architecture

```
GenieACS Boot Event → Kafka (genieacs_boot)
                         ↓
                  PPPoE Config Worker
                         ↓
        ┌────────────────┼────────────────────┐
        ↓                ↓                    ↓
   API Lookup      GenieACS Config    Update ACS Device ID
        ↓                ↓                    ↓
   User Data       Set PPPoE Params   PATCH /api/../{username}/onu
        ↓                ↓                    ↓
        └────────────────┴────────────────────┘
                         ↓
              Gotify Notifications
                         ↓
        Kafka (pppoe_registered or onu_no_pppoe)
```

## Kafka Topics

### Input Topic: `genieacs_boot`
Boot event from GenieACS with device information:
```json
{
  "event": "BOOT",
  "timestamp": "2025-11-03T09:19:29.109Z",
  "device_id": "E007C2-MH80-MHAR08F6C2D9",
  "manufacturer": "MHAR",
  "oui": "E007C2",
  "product_class": "MH80",
  "serial_number": "MHAR08F6C2D9",
  "hardware_version": "",
  "software_version": ""
}
```

### Success Topic: `pppoe_registered`
Published when PPPoE configuration succeeds:
```json
{
  "event": "BOOT",
  "device_id": "E007C2-MH80-MHAR08F6C2D9",
  "serial_number": "MHAR08F6C2D9",
  "pppoe_user": {
    "user_name": "MYB-779",
    "nas_ip_address": "10.42.3.28",
    "mikrotik_group": "premium"
  },
  "configuration_status": "success",
  "configured_username": "MYB-779",
  "acs_device_id": "E007C2-MH80-MHAR08F6C2D9",
  "acs_device_id_updated": true
}
```

### Error Topic: `onu_no_pppoe`
Published when PPPoE user not found or configuration fails:
```json
{
  "event": "BOOT",
  "device_id": "E007C2-MH80-MHAR08F6C2D9",
  "serial_number": "MHAR08F6C2D9",
  "error": "No PPPoE user found for serial number",
  "error_type": "no_pppoe_user"
}
```

## API Integration

### Fetch PPPoE User Data
```
GET http://localhost:8000/api/v1/pppoe/users/by-onu-serial-number/{serial_number}
```

Response:
```json
{
  "id": 7,
  "user_name": "MYB-779",
  "user_password": "123456",
  "nas_ip_address": "10.42.3.28",
  "mikrotik_group": "premium",
  "onu_serial_number": "MHAR08F6C2D9",
  "is_active": true
}
```

### Update ACS Device ID
```
PATCH http://localhost:8000/api/v1/pppoe/users/{username}/onu
```

Request:
```json
{
  "acs_device_id": "E007C2-MH80-MHAR08F6C2D9"
}
```

This updates the GenieACS device ID in the database for tracking and future reference.

## GenieACS Integration

Uses GenieACS Python library to set TR-069 parameters:
```python
acs.task_set_parameter_values(device_id, [
    ["InternetGatewayDevice.WANDevice.1.WANConnectionDevice.2.WANPPPConnection.1.Username", username],
    ["InternetGatewayDevice.WANDevice.1.WANConnectionDevice.2.WANPPPConnection.1.Password", password]
])
```

## Environment Variables

See `.env.example` for all configuration options:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `10.42.4.19:9092` | Kafka broker addresses |
| `KAFKA_INPUT_TOPIC` | `genieacs_boot` | Input topic for boot events |
| `KAFKA_SUCCESS_TOPIC` | `pppoe_registered` | Success output topic |
| `KAFKA_ERROR_TOPIC` | `onu_no_pppoe` | Error output topic |
| `API_BASE_URL` | `http://localhost:8000` | Device provisioning API URL |
| `GENIEACS_HOST` | `10.42.4.3` | GenieACS server address |
| `GENIEACS_USER` | `onu` | GenieACS username |
| `GENIEACS_PASSWORD` | `onu` | GenieACS password |
| `GOTIFY_URL` | `https://gotify.mcandres.com` | Gotify server URL |
| `GOTIFY_TOKEN` | `AfvvshNxqj6wk59` | Gotify app token |

## Setup & Run

### Local Development

1. **Install dependencies**:
```bash
pip install -r requirements.txt
```

2. **Configure environment**:
```bash
cp .env.example .env
# Edit .env with your settings
```

3. **Load environment and run**:
```bash
source load_env.sh
python main.py
```

### Docker Deployment

1. **Build and push to Docker Hub**:
```bash
./docker-build-push.sh
```

2. **Run with Docker Compose**:
```bash
docker-compose up -d
```

3. **View logs**:
```bash
docker-compose logs -f pppoe-config-worker
```

4. **Stop service**:
```bash
docker-compose down
```

## Notifications

Gotify notifications are sent for:

### Success (Priority 5)
```
✅ PPPoE Configured Successfully
Serial: MHAR08F6C2D9
Device: E007C2-MH80-MHAR08F6C2D9
Username: MYB-779
Group: premium
```

### No PPPoE User (Priority 8)
```
❌ PPPoE User Not Found
Serial: MHAR08F6C2D9
Device: E007C2-MH80-MHAR08F6C2D9
Error: No PPPoE user configured for this ONU
```

### Configuration Failed (Priority 8)
```
❌ PPPoE Configuration Failed
Serial: MHAR08F6C2D9
Device: E007C2-MH80-MHAR08F6C2D9
Error: Failed to set PPPoE parameters via GenieACS
```

## TR-069 Parameter Paths

Default paths (configurable via environment variables):
- **Username**: `InternetGatewayDevice.WANDevice.1.WANConnectionDevice.2.WANPPPConnection.1.Username`
- **Password**: `InternetGatewayDevice.WANDevice.1.WANConnectionDevice.2.WANPPPConnection.1.Password`

For different ONU models, adjust these paths in `.env`.

## Monitoring

Check worker status:
```bash
docker-compose ps
docker-compose logs pppoe-config-worker
```

Monitor Kafka topics:
```bash
# Check input topic
kafka-console-consumer --bootstrap-server 10.42.4.19:9092 --topic genieacs_boot

# Check success topic
kafka-console-consumer --bootstrap-server 10.42.4.19:9092 --topic pppoe_registered

# Check error topic
kafka-console-consumer --bootstrap-server 10.42.4.19:9092 --topic onu_no_pppoe
```

## Troubleshooting

### Worker not processing messages
- Check Kafka connectivity: verify bootstrap servers
- Verify topic exists: `kafka-topics --list --bootstrap-server 10.42.4.19:9092`
- Check consumer group status

### API requests failing
- Verify API_BASE_URL is correct
- Ensure API is accessible from container
- Check if PPPoE user exists for the serial number

### GenieACS configuration failing
- Verify GenieACS credentials
- Check if device is connected to GenieACS
- Verify TR-069 parameter paths match your ONU model

### No Gotify notifications
- Verify Gotify URL and token
- Check Gotify server accessibility
- Review worker logs for errors

## Docker Hub

Image: `marcandres888/pppoe-config-worker:latest`

Pull and run:
```bash
docker pull marcandres888/pppoe-config-worker:latest
docker run -d --name pppoe-config-worker \
  -e KAFKA_BOOTSTRAP_SERVERS=10.42.4.19:9092 \
  -e API_BASE_URL=http://host.docker.internal:8000 \
  -e GENIEACS_HOST=10.42.4.3 \
  marcandres888/pppoe-config-worker:latest
```

## License

Part of the Apollo Device Provisioner project.
