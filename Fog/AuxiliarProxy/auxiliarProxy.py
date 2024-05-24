import logging
import threading
import time
import zmq

# Temperature parameters
RANGO_MIN_TEMPERATURA = 11.0
RANGO_MAX_TEMPERATURA = 29.4

# Humidity parameters
RANGO_MIN_HUMEDAD = 70.0
RANGO_MAX_HUMEDAD = 100.0

main_proxy = True
count_time = 5

def __init__():
   

    #Logs configuration
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    global sensor_bind_address
    sensor_bind_address = "tcp://*:5555"

    global health_system_bind_address
    health_system_bind_address = "tcp://*:5558"

    global quality_system_connect_address
    quality_system_connect_address = "tcp://localhost:5580"

    global cloud_connect_address
    cloud_connect_address = "tcp://54.92.238.228:5581"

    global context
    context = zmq.Context()

    
    global sensor_bind_socket
    global quality_system_socket
    global cloud_connect_socket
    global health_system_socket



    try:
        logging.info("Creating connections")

        health_system_socket = context.socket(zmq.PULL)
        health_system_socket.bind(health_system_bind_address)
        logging.info("Finished creating connections")



    except Exception as e:
        logging.error(f"Error creating sockets: " + str(e))

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
def health_system():
    global main_proxy
    global count_time
    global control
    logging.info("Starting health system...")
    while True:
        try:
            message = health_system_socket.recv_json(flags=zmq.NOBLOCK)
            if message.get("heartbeat") == "ping":
                logging.info("♥ Latido recibido")
                count_time = 5  # Se recibió un latido, actualiza el estado
                main_proxy = True
                control = False
        except zmq.Again:
            main_proxy = False
            if(count_time > -1):
                count_time -= 1
            if(count_time == 0):
                main_thread = threading.Thread(target=main)
                main_thread.start()
            time.sleep(1)     
       
def main():
    global control
    control = False
    global sensor_bind_socket
    global quality_system_socket
    global cloud_connect_socket

    try:
        # Create the socket to connect to the sensors
        sensor_bind_socket = context.socket(zmq.PULL)
        sensor_bind_socket.bind(sensor_bind_address)

        quality_system_socket = context.socket(zmq.REQ)
        quality_system_socket.connect(quality_system_connect_address)

        cloud_connect_socket = context.socket(zmq.REQ)
        cloud_connect_socket.connect(cloud_connect_address)
    
    
        while not main_proxy and count_time <= 0:
            if(not control):
                logging.error("The main proxy is not available. Auxiliar proxy is taking the control.")
                control = True
            try:
                message = sensor_bind_socket.recv_json()
                logging.info(f"Message: {message}")
                send_data(message)

            except zmq.Again as e:
                pass
            except Exception as e:
                logging.error(f"Error: {e}")
    finally:
        logging.info("Closing sockets...")
        sensor_bind_socket.close()
        quality_system_socket.close()
        cloud_connect_socket.close()
   

if __name__ == "__main__":
    __init__()
    health_system_thread = threading.Thread(target=health_system)
    health_system_thread.start()

    
    try:
        health_system_thread.join()
        
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt detected. Closing the ZeroMQ context.")
    finally:
        logging.info("Closing sockets...")
        health_system_socket.close()
        context.term()


