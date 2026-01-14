import configparser
from confluent_kafka import Producer

# Read the configurations once when the module is imported
config = configparser.ConfigParser()
# Path is relative to the directory where python producer.py is run (generator/)
config.read("/opt/airflow/generator/configuration/configuration.ini") 
KAFKA_SETTINGS = config["KAFKA"]

def get_configs():
    """
    Returns the Confluent Cloud producer configuration dictionary.
    Pulls credentials from configuration.ini.
    """
    conf = {
        'bootstrap.servers': KAFKA_SETTINGS["bootstrap_servers"],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN', 
        'client.id': 'python-producer' # ESSENTIAL ADDITION
    }
    
    # Check for authentication method and apply credentials
    auth_method = KAFKA_SETTINGS.get("auth_method", "none")
    if auth_method == 'sasl_scram':
        # NOTE: Confluent Cloud typically uses PLAIN, not SCRAM, but we will use the keys
        # for authentication regardless of mechanism name, as long as PLAIN is set above.
        conf['sasl.username'] = KAFKA_SETTINGS["sasl_username"]
        conf['sasl.password'] = KAFKA_SETTINGS["sasl_password"]
        
    # --- DEBUG ADDITION ---
    # Create a safe copy to print without revealing password
    debug_conf = conf.copy()
    if 'sasl.password' in debug_conf:
        debug_conf['sasl.password'] = '*** OBFUSCATED ***'
        
    print(f"DEBUG: Kafka Config Prepared for Confluent Cloud: {debug_conf}")
    # --- END DEBUG ---

    return conf

def delivery_report(err, msg):
    """Reports successful or failed message delivery."""
    if err is not None:
        print(f"Message delivery failed: {err}")

def get_raw_config():
    return config
