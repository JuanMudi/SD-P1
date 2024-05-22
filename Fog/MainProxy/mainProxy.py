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


def __init__():

    #Logs configuration
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    global sensor_connect_address
    sensor_connect_address = "tcp://localhost:5555"

    global health_system_connect_address
    health_system_connect_address = "tcp://localhost:5558"

    global quality_system_connect_address
    quality_system_connect_address = "tcp://localhost:5580"

    global cloud_connect_address
    cloud_connect_address = "tcp://54.92.238.228:5581"

    global context
    context = zmq.Context()

    
    global sensor_connect_socket
    global quality_system_socket
    global cloud_connect_socket
    global health_system_socket



    try:
        logging.info("Starting main proxy...")

        # Create the socket to connect to the sensors
        sensor_connect_socket = context.socket(zmq.PULL)
        sensor_connect_socket.connect(sensor_connect_address)

        quality_system_socket = context.socket(zmq.REQ)
        quality_system_socket.connect(quality_system_connect_address)

        cloud_connect_socket = context.socket(zmq.REQ)
        cloud_connect_socket.connect(cloud_connect_address)

        health_system_socket = context.socket(zmq.PUSH)
        health_system_socket.connect(health_system_connect_address)


    except Exception as e:
        logging.error(f"Error creating sockets: " + str(e))

def send_data(data):
    if(data["sensor_type"]=="Temperature" and data["measurement"] != -1):
        cloud_connect_socket.send_json(data) 
        cloud_connect_socket.recv_json()        
    elif(data["sensor_type"]=="Humidity" and data["measurement"] != -1):
        cloud_connect_socket.send_json(data)   
        cloud_connect_socket.recv_json()        
    elif(data["sensor_type"]=="Smoke"):
        cloud_connect_socket.send_json(data)     
        cloud_connect_socket.recv_json()        
    


def obtain_data(sensor):
    try:
        cloud_connect_socket.send_json({"message_type": "request", "sensor_type": sensor})
        data = cloud_connect_socket.recv_serialized()      

        logging.info(f"Data obtained from cloud: {data}")
        return data
    except Exception as e:
        logging.error(f"Error obtaining data: {e}")
        return None


def analyze_data(data, sensor):
    try:
        documents_list = list(data)
        if(sensor=="Temperature"):
            promedio = 0.0
            for d in documents_list:
                promedio += d["measurement"]
            promedio = promedio/len(documents_list)

            if(promedio>=RANGO_MIN_TEMPERATURA and promedio<=RANGO_MAX_TEMPERATURA):
                logging.info(f"The temperature average is OK: {promedio}")
            else:
                # Send alert to the quality system in the Fog layer
                logging.info(f"The temperature average is WRONG: {promedio}")

                quality_system_socket.send_json({"message_type": "alert", "Average": promedio, "status": "incorrecto", "sensor_type": sensor})
                response = quality_system_socket.recv_json()


                # Send alert to the cloud
                cloud_connect_socket.send_json({"sensor_type": sensor, "measurement": promedio, "status": "incorrecto"})
                response_cloud = cloud_connect_socket.recv_json()


                logging.info(f"Quality system response: {response}")
                logging.info(f"Cloud response: {response_cloud}")

        elif(sensor=="Humidity"):
            promedio = 0.0
            for d in documents_list:
                promedio += d["measurement"]
            promedio = promedio/len(documents_list)

            if(promedio>=RANGO_MIN_HUMEDAD and promedio<=RANGO_MAX_HUMEDAD):
                logging.info(f"The humidity average is OK: {promedio}")
            else:
                # Send alert to the quality system in the Fog layer
                logging.info(f"The humidity average is WRONG: {promedio}")

                quality_system_socket.send_json({"message_type": "alert", "Average": promedio, "status": "incorrecto", "sensor_type": sensor})
                response = quality_system_socket.recv_json()


                # Send alert to the cloud
                cloud_connect_socket.send_json({"sensor_type": sensor, "measurement": promedio, "status": "incorrecto"})
                response_cloud = cloud_connect_socket.recv_json()


                logging.info(f"Quality system response: {response}")
                logging.info(f"Cloud response: {response_cloud}")
                    
    
    except Exception as e:
        logging.error(f"Error: {e}")

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
        try:
            message = sensor_connect_socket.recv_json(flags=zmq.NOBLOCK)
            logging.info(f"Message: {message}")
            send_data(message)

        except Exception as e:
            logging.error(f"Error receiving data: {e}")
   

if __name__ == "__main__":
    __init__()
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
        sensor_connect_socket.close()
        quality_system_socket.close()
        cloud_connect_socket.close()
        health_system_socket.close()
        context.term()


