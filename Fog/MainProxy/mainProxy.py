import logging
import pickle
import threading
import time
import zmq

# Temperature parameters
RANGO_MIN_TEMPERATURA = 11.0
RANGO_MAX_TEMPERATURA = 29.4

# Humidity parameters
RANGO_MIN_HUMEDAD = 70.0
RANGO_MAX_HUMEDAD = 100.0

def initialize():
    global context, sensor_bind_socket, quality_system_socket, cloud_connect_socket, health_system_socket

    # Logs configuration
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    sensor_bind_address = "tcp://*:5555"
    health_system_connect_address = "tcp://10.138.208.85:5558"
    quality_system_connect_address = "tcp://10.51.51.183:5580"
    cloud_connect_address = "tcp://54.92.238.228:5581"

    context = zmq.Context()
    try:
        logging.info("Starting main proxy...")

        # Create the socket to connect to the sensors
        sensor_bind_socket = context.socket(zmq.PULL)
        sensor_bind_socket.bind(sensor_bind_address)

        quality_system_socket = context.socket(zmq.REQ)
        quality_system_socket.connect(quality_system_connect_address)

        cloud_connect_socket = context.socket(zmq.REQ)
        cloud_connect_socket.connect(cloud_connect_address)

        health_system_socket = context.socket(zmq.PUSH)
        health_system_socket.connect(health_system_connect_address)

    except Exception as e:
        logging.error(f"Error creating sockets: {e}")

def send_data(data):
    try:
        start_time = time.time()
        if data["sensor_type"] == "Temperature" and data["measurement"] != -1:
            cloud_connect_socket.send_json(data)
            cloud_connect_socket.recv_json()
            analyze_data(obtain_data("Temperature"), "Temperature")
        elif data["sensor_type"] == "Humidity" and data["measurement"] != -1:
            cloud_connect_socket.send_json(data)
            cloud_connect_socket.recv_json()
            analyze_data(obtain_data("Humidity"), "Humidity")
        elif data["sensor_type"] == "Smoke":
            cloud_connect_socket.send_json(data)
            cloud_connect_socket.recv_json()
            analyze_data(obtain_data("Smoke"), "Smoke")
        end_time = time.time()
        communication_time = end_time - start_time
        cloud_connect_socket.send_json({"message_type": "communication_time", "time": communication_time})
        cloud_connect_socket.recv_json()
        

    except Exception as e:
        logging.error(f"Error sending data: {e}")

def obtain_data(sensor):
    try:
        cloud_connect_socket.send_json({"message_type": "request", "sensor_type": sensor})
        data = cloud_connect_socket.recv_json()
        logging.info(f"Data obtained from cloud: {data}")
        return data
    except Exception as e:
        logging.error(f"Error obtaining data: {e}")
        return None

def analyze_data(data, sensor):
    try:
        documents_list = list(data)
        logging.info(f"Documents list: {data}")
        if sensor == "Temperature":
            promedio = sum(d["measurement"] for d in documents_list) / len(documents_list)
            if RANGO_MIN_TEMPERATURA <= promedio <= RANGO_MAX_TEMPERATURA:
                logging.info(f"The temperature average is OK: {promedio}")
            else:
                logging.info(f"The temperature average is WRONG: {promedio}")
                quality_system_socket.send_json({"message_type": "alert", "Average": promedio, "status": "incorrecto", "sensor_type": sensor})
                response = quality_system_socket.recv_json()
                logging.info(f"Quality system response: {response}")
                cloud_connect_socket.send_json({"message_type": "alert","sensor_type": sensor, "measurement": promedio, "status": "incorrecto", "layer" : "Fog" })
                response_cloud = cloud_connect_socket.recv_json()
                logging.info(f"Cloud response: {response_cloud}")
        elif sensor == "Humidity":
            promedio = sum(d["measurement"] for d in documents_list) / len(documents_list)
            if RANGO_MIN_HUMEDAD <= promedio <= RANGO_MAX_HUMEDAD:
                logging.info(f"The humidity average is OK: {promedio}")
            else:
                logging.info(f"The humidity average is WRONG: {promedio}")
                quality_system_socket.send_json({"message_type": "alert", "Average": promedio, "status": "incorrecto", "sensor_type": sensor})
                response = quality_system_socket.recv_json()
                logging.info(f"Quality system response: {response}")
                cloud_connect_socket.send_json({"message_type": "alert", "sensor_type": sensor, "measurement": promedio, "status": "incorrecto", "layer" : "Fog"})
                response_cloud = cloud_connect_socket.recv_json()
                logging.info(f"Cloud response: {response_cloud}")
    except Exception as e:
        logging.error(f"Error analyzing data: {e}")

def send_heartbeat():
    while True:
        try:
            health_system_socket.send_json({"heartbeat": "ping"})
        except zmq.ZMQError as e:
            logging.error(f"Failure sending the heartbeat: {e}")
        time.sleep(1)

def main():
    logging.info("Waiting for data...")
    while True:
        logging.info("Waiting for data...")
        try:
            message = sensor_bind_socket.recv_json()
            logging.info(f"Message: {message}")
            send_data(message)
        except zmq.Again:
            pass
        except Exception as e:
            logging.error(f"Error receiving data: {e}")

if __name__ == "__main__":
    initialize()
    
    main_thread = threading.Thread(target=main)
    main_thread.start()

    heartbeat_thread = threading.Thread(target=send_heartbeat)
    heartbeat_thread.start()

    try:
        main_thread.join()
        heartbeat_thread.join()
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt detected. Closing the ZeroMQ context.")
    finally:
        logging.info("Closing sockets...")
        sensor_bind_socket.close()
        quality_system_socket.close()
        cloud_connect_socket.close()
        health_system_socket.close()
        context.term()
