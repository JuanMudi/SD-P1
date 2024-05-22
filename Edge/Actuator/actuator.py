import logging
import zmq

def __init__():

    #Logs configuration
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    
    global actuator_bind_address
    actuator_bind_address = "tcp://*:5557"

    global context
    context = zmq.Context()

    global actuator_socket
    actuator_socket = context.socket(zmq.PULL)

    try:
        actuator_socket.bind(actuator_bind_address)
    except Exception as e:
        logging.error(f"Error creating sockets: " + str(e))

def main():

    __init__()

    while True:
        logging.info("Waiting for alerts...")
        try:
            message = actuator_socket.recv_json(flags=zmq.BLOCKY)
            logging.info(f"Received alert: {message}")
        except Exception as e:
            logging.error(f"Error receiving alerts " + str(e))


    
if __name__ == "__main__":
    main()