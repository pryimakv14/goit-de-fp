from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
from configs import kafka_config
import json

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
)

athlete_event_results_topic = NewTopic(name="athlete_event_results", num_partitions=2, replication_factor=1)
aggregated_athlete_data_topic = NewTopic(name="aggregated_athlete_data", num_partitions=2, replication_factor=1)

admin_client.create_topics(new_topics=[athlete_event_results_topic, aggregated_athlete_data_topic], validate_only=False)

admin_client.close()

consumer = KafkaConsumer(
    "aggregated_athlete_data",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    group_id="alert_display_group",
    auto_offset_reset='earliest'
)

for message in consumer:
    alert = message.value
    print(f"Received message in athlete_event_results topic : {alert}")
