"""
GenieACS PPPoE Configuration Worker

Listens to genieacs_boot Kafka topic and automatically configures
PPPoE credentials on ONUs using GenieACS TR-069.
"""
import os
import json
import logging
import requests
from typing import Dict, Any, Optional
from kafka import KafkaConsumer, KafkaProducer
import genieacs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PPPoEConfigurationWorker:
    """Worker to configure PPPoE credentials on ONUs via GenieACS"""
    
    def __init__(self):
        # Kafka Configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '10.42.4.19:9092')
        self.kafka_input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'genieacs_boot')
        self.kafka_success_topic = os.getenv('KAFKA_SUCCESS_TOPIC', 'pppoe_registered')
        self.kafka_error_topic = os.getenv('KAFKA_ERROR_TOPIC', 'onu_no_pppoe')
        self.kafka_consumer_group = os.getenv('KAFKA_CONSUMER_GROUP', 'pppoe-config-worker')
        
        # API Configuration
        self.api_base_url = os.getenv('API_BASE_URL', 'http://localhost:8000')
        
        # GenieACS Configuration
        self.genieacs_host = os.getenv('GENIEACS_HOST', '10.42.4.3')
        self.genieacs_user = os.getenv('GENIEACS_USER', 'onu')
        self.genieacs_password = os.getenv('GENIEACS_PASSWORD', 'onu')
        self.genieacs_ssl = os.getenv('GENIEACS_SSL', 'false').lower() == 'true'
        
        # Gotify Configuration
        self.gotify_url = os.getenv('GOTIFY_URL', 'https://gotify.mcandres.com')
        self.gotify_token = os.getenv('GOTIFY_TOKEN', 'AfvvshNxqj6wk59')
        
        # TR-069 Parameter Paths (Default)
        self.pppoe_username_path_default = os.getenv('PPPOE_USERNAME_PATH', 
            'InternetGatewayDevice.WANDevice.1.WANConnectionDevice.2.WANPPPConnection.1.Username')
        self.pppoe_password_path_default = os.getenv('PPPOE_PASSWORD_PATH',
            'InternetGatewayDevice.WANDevice.1.WANConnectionDevice.2.WANPPPConnection.1.Password')
        
        # TR-069 Parameter Paths for ZTE F670L
        self.pppoe_username_path_zte_f670l = 'InternetGatewayDevice.WANDevice.1.WANConnectionDevice.1.WANPPPConnection.1.Username'
        self.pppoe_password_path_zte_f670l = 'InternetGatewayDevice.WANDevice.1.WANConnectionDevice.1.WANPPPConnection.1.Password'
        
        self.consumer = None
        self.producer = None
        self.genieacs_connection = None
        
        logger.info("PPPoE Configuration Worker initialized")
        logger.info(f"Kafka Bootstrap: {self.kafka_bootstrap_servers}")
        logger.info(f"Input Topic: {self.kafka_input_topic}")
        logger.info(f"API Base URL: {self.api_base_url}")
        logger.info(f"GenieACS Host: {self.genieacs_host}")
    
    def connect_kafka(self):
        """Initialize Kafka consumer and producer"""
        logger.info("Connecting to Kafka...")
        
        self.consumer = KafkaConsumer(
            self.kafka_input_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=self.kafka_consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        logger.info("✅ Kafka connected successfully")
    
    def connect_genieacs(self):
        """Initialize GenieACS connection"""
        logger.info("Connecting to GenieACS...")
        
        self.genieacs_connection = genieacs.Connection(
            self.genieacs_host,
            ssl=self.genieacs_ssl,
            auth=True,
            user=self.genieacs_user,
            passwd=self.genieacs_password
        )
        
        logger.info("✅ GenieACS connected successfully")
    
    def send_gotify_notification(self, title: str, message: str, priority: int = 5, extras: Optional[Dict] = None):
        """Send notification to Gotify"""
        try:
            url = f"{self.gotify_url}/message?token={self.gotify_token}"
            payload = {
                "title": title,
                "message": message,
                "priority": priority
            }
            if extras:
                payload["extras"] = extras
            
            response = requests.post(url, json=payload, timeout=5)
            response.raise_for_status()
            logger.info(f"✅ Gotify notification sent: {title}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to send Gotify notification: {e}")
    
    def get_pppoe_user_by_serial(self, serial_number: str) -> Optional[Dict[str, Any]]:
        """Fetch PPPoE user data by ONU serial number (tries primary then secondary)"""
        try:
            # Try primary serial number first
            url = f"{self.api_base_url}/api/v1/pppoe/users/by-onu-serial-number/{serial_number}"
            logger.info(f"Fetching PPPoE user for primary serial: {serial_number}")
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            user_data = response.json()
            logger.info(f"✅ Found PPPoE user by primary serial: {user_data.get('user_name')}")
            return user_data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"⚠️ No PPPoE user found for primary serial: {serial_number}")
                
                # Try secondary serial number
                try:
                    url = f"{self.api_base_url}/api/v1/pppoe/users/by-onu-secondary-serial-number/{serial_number}"
                    logger.info(f"Trying secondary serial: {serial_number}")
                    
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()
                    
                    user_data = response.json()
                    logger.info(f"✅ Found PPPoE user by secondary serial: {user_data.get('user_name')}")
                    return user_data
                    
                except requests.exceptions.HTTPError as e2:
                    if e2.response.status_code == 404:
                        logger.warning(f"❌ No PPPoE user found for secondary serial: {serial_number}")
                        return None
                    logger.error(f"HTTP error fetching PPPoE user by secondary serial: {e2}")
                    return None
                except Exception as e2:
                    logger.error(f"Error fetching PPPoE user by secondary serial: {e2}")
                    return None
            
            logger.error(f"HTTP error fetching PPPoE user: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching PPPoE user: {e}")
            return None
    
    def get_pppoe_parameter_paths(self, manufacturer: str, product_class: str) -> tuple:
        """Get PPPoE parameter paths based on device manufacturer and product class"""
        # Check for ZTE F670L
        if manufacturer and product_class:
            manufacturer_upper = manufacturer.upper()
            product_class_upper = product_class.upper()
            
            if manufacturer_upper == 'ZTE' and product_class_upper == 'F670L':
                logger.info(f"Using ZTE F670L parameter paths for {manufacturer} {product_class}")
                return (self.pppoe_username_path_zte_f670l, self.pppoe_password_path_zte_f670l)
        
        # Default paths for other devices
        logger.info(f"Using default parameter paths for {manufacturer} {product_class}")
        return (self.pppoe_username_path_default, self.pppoe_password_path_default)
    
    def configure_pppoe_on_onu(self, device_id: str, username: str, password: str, 
                              manufacturer: str = None, product_class: str = None) -> bool:
        """Configure PPPoE credentials on ONU via GenieACS"""
        try:
            logger.info(f"Configuring PPPoE on device: {device_id}")
            logger.info(f"Username: {username}")
            logger.info(f"Manufacturer: {manufacturer}, Product Class: {product_class}")
            
            # Get appropriate parameter paths based on device type
            username_path, password_path = self.get_pppoe_parameter_paths(manufacturer, product_class)
            
            logger.info(f"Username Path: {username_path}")
            logger.info(f"Password Path: {password_path}")
            
            # Set PPPoE parameters
            self.genieacs_connection.task_set_parameter_values(device_id, [
                [username_path, username],
                [password_path, password]
            ])
            
            logger.info(f"✅ PPPoE configuration task sent to GenieACS for {device_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to configure PPPoE on {device_id}: {e}")
            return False
    
    def update_acs_device_id(self, username: str, device_id: str) -> bool:
        """Update ACS device ID for PPPoE user"""
        try:
            url = f"{self.api_base_url}/api/v1/pppoe/users/{username}/onu"
            logger.info(f"Updating ACS device ID for user: {username}")
            
            payload = {"acs_device_id": device_id}
            response = requests.patch(url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"✅ Updated ACS device ID: {device_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update ACS device ID for {username}: {e}")
            return False
    
    def publish_to_kafka(self, topic: str, message: Dict[str, Any]):
        """Publish message to Kafka topic"""
        try:
            future = self.producer.send(topic, message)
            future.get(timeout=10)
            logger.info(f"✅ Published to topic '{topic}'")
        except Exception as e:
            logger.error(f"Failed to publish to Kafka topic '{topic}': {e}")
    
    def process_message(self, message: Dict[str, Any]):
        """Process a single GenieACS boot event"""
        try:
            logger.info("=" * 80)
            logger.info("📡 New GenieACS Boot Event")
            logger.info("=" * 80)
            
            # Extract data from message
            event = message.get('event')
            device_id = message.get('device_id')
            serial_number = message.get('serial_number')
            manufacturer = message.get('manufacturer')
            product_class = message.get('product_class')
            timestamp = message.get('timestamp')
            
            if not device_id or not serial_number:
                error_msg = "Missing required fields: device_id or serial_number"
                logger.error(f"❌ {error_msg}")
                return
            
            logger.info(f"📱 Device ID: {device_id}")
            logger.info(f"🔢 Serial Number: {serial_number}")
            logger.info(f"🏭 Manufacturer: {manufacturer}")
            logger.info(f"📦 Product Class: {product_class}")
            logger.info(f"⏰ Timestamp: {timestamp}")
            
            # Step 1: Get PPPoE user data by serial number
            pppoe_user = self.get_pppoe_user_by_serial(serial_number)
            
            if not pppoe_user:
                error_msg = f"No PPPoE user found for serial number: {serial_number}"
                logger.error(f"❌ {error_msg}")
                
                # Send error notification
                self.send_gotify_notification(
                    title="❌ PPPoE User Not Found",
                    message=f"Serial: {serial_number}\n"
                            f"Device: {device_id}\n"
                            f"Manufacturer: {manufacturer}\n"
                            f"Error: No PPPoE user configured for this ONU",
                    priority=8,
                    extras={
                        "serialNumber": serial_number,
                        "deviceId": device_id,
                        "status": "no_pppoe_user"
                    }
                )
                
                # Publish to error topic
                error_payload = {
                    **message,
                    "error": error_msg,
                    "error_type": "no_pppoe_user"
                }
                self.publish_to_kafka(self.kafka_error_topic, error_payload)
                return
            
            # Extract PPPoE credentials
            username = pppoe_user.get('user_name')
            password = pppoe_user.get('user_password')
            nas_ip = pppoe_user.get('nas_ip_address')
            mikrotik_group = pppoe_user.get('mikrotik_group')
            
            logger.info(f"👤 PPPoE Username: {username}")
            logger.info(f"📍 NAS IP: {nas_ip}")
            logger.info(f"👥 Mikrotik Group: {mikrotik_group}")
            
            # Step 2: Configure PPPoE on ONU via GenieACS
            success = self.configure_pppoe_on_onu(device_id, username, password, manufacturer, product_class)
            
            if not success:
                error_msg = f"Failed to configure PPPoE on device: {device_id}"
                logger.error(f"❌ {error_msg}")
                
                # Send error notification
                self.send_gotify_notification(
                    title="❌ PPPoE Configuration Failed",
                    message=f"Serial: {serial_number}\n"
                            f"Device: {device_id}\n"
                            f"Username: {username}\n"
                            f"Error: Failed to set PPPoE parameters via GenieACS",
                    priority=8,
                    extras={
                        "serialNumber": serial_number,
                        "deviceId": device_id,
                        "username": username,
                        "status": "configuration_failed"
                    }
                )
                
                # Publish to error topic
                error_payload = {
                    **message,
                    "pppoe_user": pppoe_user,
                    "error": error_msg,
                    "error_type": "genieacs_configuration_failed"
                }
                self.publish_to_kafka(self.kafka_error_topic, error_payload)
                return
            
            # Step 3: Update ACS device ID in database
            logger.info(f"Step 3: Updating ACS device ID in database")
            acs_update_success = self.update_acs_device_id(username, device_id)
            if not acs_update_success:
                logger.warning(f"⚠️ Failed to update ACS device ID, but PPPoE configuration succeeded")
            
            # Step 4: Publish success result
            success_payload = {
                **message,
                "pppoe_user": pppoe_user,
                "configuration_status": "success",
                "configured_username": username,
                "configured_at": timestamp,
                "acs_device_id": device_id,
                "acs_device_id_updated": acs_update_success
            }
            self.publish_to_kafka(self.kafka_success_topic, success_payload)
            
            # Send success notification
            acs_status = "✅ ACS Device ID updated" if acs_update_success else "⚠️ ACS Device ID update failed"
            self.send_gotify_notification(
                title="✅ PPPoE Configured Successfully",
                message=f"Serial: {serial_number}\n"
                        f"Device: {device_id}\n"
                        f"Username: {username}\n"
                        f"Group: {mikrotik_group}\n"
                        f"NAS IP: {nas_ip}\n"
                        f"ACS Device ID: {device_id}\n"
                        f"{acs_status}\n"
                        f"Status: PPPoE credentials configured via GenieACS",
                priority=5,
                extras={
                    "serialNumber": serial_number,
                    "deviceId": device_id,
                    "username": username,
                    "mikrotikGroup": mikrotik_group,
                    "status": "success"
                }
            )
            
            logger.info("=" * 80)
            logger.info("✅ PPPoE Configuration Process Completed Successfully")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.exception(e)
    
    def run(self):
        """Main worker loop"""
        logger.info("🚀 Starting PPPoE Configuration Worker...")
        
        try:
            # Initialize connections
            self.connect_kafka()
            self.connect_genieacs()
            
            logger.info(f"👂 Listening to topic: {self.kafka_input_topic}")
            logger.info("=" * 80)
            
            # Process messages
            for kafka_message in self.consumer:
                try:
                    message = kafka_message.value
                    self.process_message(message)
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
                    logger.exception(e)
                    
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down worker...")
        except Exception as e:
            logger.error(f"Fatal error in worker: {e}")
            logger.exception(e)
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()
            logger.info("👋 Worker stopped")


if __name__ == "__main__":
    worker = PPPoEConfigurationWorker()
    worker.run()
