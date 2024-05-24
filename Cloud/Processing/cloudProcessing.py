import argparse
import logging
import pickle
from sys import getsizeof
import threading
import time
from pymongo import MongoClient
import zmq


# Humidity parameters
RANGO_MIN_HUMEDAD = 70.0
RANGO_MAX_HUMEDAD = 100.0

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
    global time_collection
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
        time_collection = db["time"]

    except Exception as e:
        logging.error(f"Error creating sockets: {e}")


def processing_system_cloud():
    global message_counter
    global messages_size

    logging.info("Starting processing system in the cloud layer...")
    try:
            while True:


                message = fog_layer_socket.recv_json()
                message_counter += 2
                messages_size += getsizeof(message) * 2
                aux = message.copy()
                if message["message_type"] == "alert":
                    alerts_collection.insert_one(aux)                     
                    logging.info(f"Alerta recibida en la capa cloud: {message}")
                    logging.info("Sendig alert to quality system")             
                    quality_system_socket.send_json(message)                      
                    message = quality_system_socket.recv_json()
                    logging.info(f"Quality system response: {message}")
                    fog_layer_socket.send_json({"status": "received"})

                elif message["message_type"] == "communication_time":
                    time_collection.insert_one(message)
                    fog_layer_socket.send_json({"status": "received"})

                elif message["message_type"] == "measurement":
                    message_counter += 1
                    messages_size += getsizeof(message)
                    logging.info(f"Data received in the cloud layer: {message}")
                    fog_layer_socket.send_json({"status": "received"})

                    data = message

                    if data["sensor_type"] == "Temperature" and data["measurement"] != -1:
                        temperature_collection.insert_one(data)
                        logging.info(f"Data saved in MongoDB: {data}")

                    elif data["sensor_type"] == "Humidity" and data["measurement"] != -1:
                        humidity_collection.insert_one(data)
                        logging.info(f"Data saved in MongoDB: {data}")

                    elif data["sensor_type"] == "Smoke":
                        smoke_collection.insert_one(data)
                        logging.info(f"Data saved in MongoDB: {data}")

                elif message["message_type"] == "request":
                    if message["sensor_type"] == "Temperature":
                        consulta = temperature_collection.find({}, {"_id": 0}).sort("time", -1).limit(10)
                    elif message["sensor_type"] == "Humidity":
                        consulta = humidity_collection.find({}, {"_id": 0}).sort("time", -1).limit(10)
                    elif message["sensor_type"] == "Smoke":
                        consulta = smoke_collection.find({}, {"_id": 0}).sort("time", -1).limit(10)
                    
                    logging.info(f"Data obtained from MongoDB: {consulta}")   

                    fog_layer_socket.send_json(list(consulta))

    except zmq.Again as e:
        time.sleep(1)
    except zmq.ZMQError as e:
        logging.error(f"Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def humidity_mensual_average():
    while True:
        try:
            data_cursor = humidity_collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(5)
            data = list(data_cursor)
            logging.info(f"Data obtained for HUMIDITY from MongoDB: {data}")
            size = len(data)

            if size > 0:
                total_measurement = 0

                for d in data:
                    logging.warning(f"Data: {d}")
                    total_measurement += d["measurement"]

                logging.info(f"Total measurement: {total_measurement} + {size}")
                promedio = total_measurement / size

                if RANGO_MIN_HUMEDAD <= promedio  and  RANGO_MAX_HUMEDAD >= promedio:
                    logging.CRITICAL(f"The humidity average is OK: {promedio}")
                else:
                    logging.CRITICAL(f"The humidity average is WRONG: {promedio}")
                    quality_system_socket.send_json({
                        "message_type": "alert",
                        "Average": f"{promedio}",
                        "status": "incorrecto",
                        "sensor_type": "Humidity",
                        "layer": "Cloud"
                    })
                    response = quality_system_socket.recv_json()
                    logging.info(f"Quality system response: {response}")
        
        except Exception as e:
            logging.error(f"Error calculating the monthly average of humidity: {e}")
        
        time.sleep(20)

def time_average():
    try:
        while True: 
            # Obtener todos los tiempos de comunicación
            tiempos = []
            for documento in time_collection.find({"message_type": "communication_time"}, {"_id": 0, "time": 1}):
                tiempos.append(documento["time"])
            
            # Calcular el promedio
            if tiempos:
                promedio = sum(tiempos) / len(tiempos)
            else:
                promedio = 0

            logging.info(f"Average communication time: {promedio}")
            quality_system_socket.send_json({"message_type": "alert", "Latency": promedio, "layer": "Cloud"})
            response = quality_system_socket.recv_json()
            logging.info(f"Quality system response: {response}")

            logging.info("Messages counter and message size")
            quality_system_socket.send_json({"message_type": "alert", "message_counter": message_counter, "messages_size": messages_size, "layer": "Cloud"})
            response = quality_system_socket.recv_json()
            logging.info(f"Quality system response: {response}")

            # Realizar la agregación para contar alertas por tipo de layer
            pipeline = [
                {"$group": {"_id": "$layer", "count": {"$sum": 1}}}
            ]
            
            resultados = alerts_collection.aggregate(pipeline)
            
            # Crear un diccionario para almacenar los resultados
            conteo_alertas = {"Cloud": 0, "Fog": 0, "Edge": 0}
            for resultado in resultados:
                layer = resultado["_id"]
                if layer in conteo_alertas:  # Asegúrate de que solo se cuentan los valores esperados
                    conteo_alertas[layer] = resultado["count"]

            quality_system_socket.send_json({"message_type": "alert", "conteo_alertas": conteo_alertas})
            message = quality_system_socket.recv_json()
            logging.info(f"Quality system response: {message}")

            time.sleep(20)
    except Exception as e:
        logging.error(f"Error calculating metrics: {e}")



    


if __name__ == "__main__":
    initialize()

    parser = argparse.ArgumentParser(description="Cloud processing system")
    parser.add_argument("--reset", type=str, required=False, choices=["True"], help="Reset the database")

    args = parser.parse_args()

    if args.reset == "True":
        client.drop_database("sensorsData")
        print("Database 'sensorsData' dropped successfully.")

    humidity_mensual_average_thread = threading.Thread(target=humidity_mensual_average)
    humidity_mensual_average_thread.start()

    main_proxy_socket_thread = threading.Thread(target=processing_system_cloud)
    main_proxy_socket_thread.start()

    time_average_thread = threading.Thread(target=time_average)
    time_average_thread.start()


    time_average_thread.join()
    main_proxy_socket_thread.join()
    humidity_mensual_average_thread.join()
