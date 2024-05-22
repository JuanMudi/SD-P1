import argparse
import logging
import pickle
from sys import getsizeof
import threading
import time
from pymongo import MongoClient
import zmq


def initialize():
    # Logs configuration
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    global quality_system_connect_address
    quality_system_connect_address = "tcp://localhost:5580"

    global quality_system_socket
    quality_system_socket = zmq.Context().socket(zmq.REQ)

    global fog_layer_bind_address
    fog_layer_bind_address = "tcp://*:5581"

    global fog_layer_socket
    fog_layer_socket = zmq.Context().socket(zmq.REP)

    global database_connect_address
    database_connect_address = "mongodb+srv://admin:admin@sensorsdata.tmxfbkr.mongodb.net/?retryWrites=true&w=majority&appName=sensorsData"

    global db
    global client
    global alerts_collection
    global temperature_collection
    global humidity_collection
    global smoke_collection
    global message_counter
    global messages_size

    message_counter = 0
    messages_size = 0

    try:
        quality_system_socket.connect(quality_system_connect_address)
        fog_layer_socket.bind(fog_layer_bind_address)

        # Crear un cliente y conectarse al servidor
        client = MongoClient(database_connect_address)
        db = client["sensorsData"]
        alerts_collection = db["alerts"]
        temperature_collection = db["temperature"]
        humidity_collection = db["humidity"]
        smoke_collection = db["smoke"]

    except Exception as e:
        logging.error(f"Error creating sockets: {e}")


def processing_system_cloud():
    global message_counter
    global messages_size

    logging.info("Starting processing system in the cloud layer...")
    while True:
        try:

            message = fog_layer_socket.recv_json(flags=zmq.NOBLOCK)
            fog_layer_socket.send_json({"status": "received"})

            if message["message_type"] == "alert":
                message_counter += 2
                messages_size += getsizeof(message) * 2
                logging.info(f"Alerta recibida en la capa cloud: {message}")
                alerts_collection.insert_one(message)
                quality_system_socket.send_json(message)

            elif message["message_type"] == "measurement":
                message_counter += 1
                messages_size += getsizeof(message)
                logging.info(f"Data received in the cloud layer: {message}")

                data = message

                if data["sensor_type"] == "Temperature" and data["measurement"] != -1:
                    temperature_collection.insert_one(data)
                    logging.info(f"Data saved in MongoDB: {data}")

                elif data["sensor_type"] == "Humidity" and data["measurement"] != -1:
                    humidity_collection.insert_one(data)
                    logging.info(f"Data saved in MongoDB: {data}")

                elif data["sensor_type"] == "Humo":
                    smoke_collection.insert_one(data)
                    logging.info(f"Data saved in MongoDB: {data}")

            elif message["message_type"] == "request":
                if message["sensor_type"] == "Temperature":
                    data = temperature_collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(10)
                    logging.info(f"Data obtained from MongoDB: {data}")
                elif message["sensor_type"] == "Humidity":
                    data = humidity_collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(10)
                    logging.info(f"Data obtained from MongoDB: {data}")
                elif message["sensor_type"] == "Smoke":
                    data = smoke_collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(10)
                    logging.info(f"Data obtained from MongoDB: {data}")
                    
                fog_layer_socket.send_json(data)

        except zmq.Again as e:
            time.sleep(1)
        except zmq.ZMQError as e:
            logging.error(f"Error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    initialize()

    parser = argparse.ArgumentParser(description="Cloud processing system")
    parser.add_argument("--reset", type=str, required=False, choices=["True"], help="Reset the database")

    args = parser.parse_args()

    if args.reset == "True":
        client.drop_database("sensorsData")
        print("Database 'sensorsData' dropped successfully.")

    main_proxy_socket_thread = threading.Thread(target=processing_system_cloud)
    main_proxy_socket_thread.start()
    main_proxy_socket_thread.join()
