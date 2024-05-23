import zmq
import logging
import time

def init():
    # Logs configuration
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    global quality_system_socket
    quality_system_bind_address = "tcp://*:5580"

    context = zmq.Context()
    quality_system_socket = context.socket(zmq.REP)

    try:
        quality_system_socket.bind(quality_system_bind_address)
    except Exception as e:
        logging.error(f"Error creating sockets: {str(e)}")

def quality_system_cloud():
    init()

    logging.info("Starting quality system in the cloud layer...")
    while True:
        try:
            message = quality_system_socket.recv_json(flags=zmq.NOBLOCK)
            if message["message_type"] == "alert":
                logging.info(f"Alerta recibida en la capa cloud: {message}")
                quality_system_socket.send_json({"status": "recibido"})    
        except zmq.Again:
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error receiving alerts: {e}")

if __name__ == "__main__":
    quality_system_cloud()
