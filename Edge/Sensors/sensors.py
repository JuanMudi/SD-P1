import argparse
from datetime import datetime
import json
import logging
import random
import threading
import time
import zmq

def __init__():

    #Logs configuration
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    global proxy_bind_address
    proxy_bind_address = "tcp://*:5555"

    global quality_system_connect_address
    quality_system_connect_address = "tcp://localhost:5556"

    global actuator_connect_address
    actuator_connect_address = "tcp://localhost:5557"

    #Context creation
    global context
    context = zmq.Context()

    #Socket creation
    global quality_system_socket
    global proxy_socket

    try:
        quality_system_socket = context.socket(zmq.REQ)
        quality_system_socket.connect(quality_system_connect_address)

        proxy_socket = context.socket(zmq.PUSH)
        proxy_socket.bind(proxy_bind_address)

    except Exception as e:
        logging.error(f"Error creating sockets: " + str(e))

# Function to load the configuration from a file
def load_config(file):
    with open(file, 'r') as f:
        return json.load(f)

# Simulate a smoke measurement
def take_smoke_measurement(config):

    # Take the configuration parameters
    p_verdadero = config["probabilidad_verdadero"]
    p_falso = config["probabilidad_falso"]
    
    # Generate a random value
    r = random.random()
    if r < p_verdadero:
        value = True
    elif r < p_verdadero + p_falso:
        value = False
    else:
        value = None
    
    return value

# Simulate a temperature or humidity measurement
def take_measurement(config):

    # Generate a random value
    r = random.random()

    if r < config["probabilidad_correctos"]:
        # Generate a correct measurement
        measurement = random.uniform(config["rango_min"], config["rango_max"])
    elif r < config["probabilidad_correctos"] + config["probabilidad_fuera_rango"]:
        # Generate a measurement out of range
        if random.random() < 0.5:
            measurement = random.uniform(config["rango_min"] - 10, config["rango_min"])
        else:
            measurement = random.uniform(config["rango_max"], config["rango_max"] + 10)
    else:
        # Generate an error
        measurement = -1
    
    return measurement



def sensor_thread(sensor_type, config, sensor_id):
    logging.info(f"Starting thread for {sensor_type} (ID: {sensor_id})...")

    while True:
        
        
        #Wait a random time before starting
        time.sleep(random.uniform(0, 5)) 
        
        # Take a measurement
        if sensor_type == "Smoke":
            measurement = take_smoke_measurement(config)
        else:
            measurement = take_measurement(config)
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Generate the message
        message = {
            "message_type": "measurement",
            "sensor_type": sensor_type,
            "measurement": measurement,
            "time": time_now,
            "route" : "Edge-Fog"
        }

        # Send the measurement to the proxy
        try:
            proxy_socket.send_json(message, zmq.NOBLOCK)
            logging.info(f"[{message["time"]}] {message["sensor_type"]}: {measurement}")  # Log the measurement
            
            if(sensor_type=="Smoke" and measurement==True):
                quality_system_socket.send_json({"sensor_type": sensor_type,"message_type": "alert", "measurement": measurement, "status": "incorrecto"})
                proxy_socket.send_json({"sensor_type": sensor_type, "message_type": "alert", "measurement": measurement, "status": "incorrecto"})

                response = quality_system_socket.recv_json()
                logging.info(f"Alert status: {response}")

        except zmq.error.Again:
            logging.error(f"Error sending message to proxy")


        # Wait for the next measurement
        time.sleep(config["frecuencia"])

def main(sensor_type, num_threads, config_files):
    logging.info("...Starting threads...")
    
    # Create threads
    threads = []
    for i in range(num_threads):
        config_file = config_files[i] if i < len(config_files) else config_files[-1]
        config = load_config(config_file)
        sensor_id = f"{sensor_type}_{i+1}"
        thread = threading.Thread(target=sensor_thread, args=(sensor_type, config, sensor_id))
        threads.append(thread)
        thread.start()
    
    # Wait all the threads to join
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulación de Sensores")
    parser.add_argument("--sensor_type", type=str, required=True, choices=["Temperature", "Humedity", "Smoke"], help="Type of sensor")
    parser.add_argument("--num_threads", type=int, default=1, help="Number of threads by sensor type")
    parser.add_argument("--config_files", type=str, nargs='+', required=True, help="Configuration files for sensors")

    args = parser.parse_args()

    __init__()
    main(args.sensor_type, args.num_threads, args.config_files)
    


        








