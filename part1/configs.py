kafka_config = {
    "bootstrap_servers": ['localhost:9092'],
    "security_protocol": 'PLAINTEXT',
    "sasl_mechanism": 'PLAIN',
}

mysql_config = {
    "host": "217.61.57.46",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp"
}

db_url_base = "jdbc:mysql://217.61.57.46:3306/"

spark_mysql_config = {
    **mysql_config,
    "ol_db_url": db_url_base + "olympic_dataset",
    "neo_db_url": db_url_base + "neo_data",
}
