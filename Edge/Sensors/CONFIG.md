
# Levantar un Hilo por Sensor
python Edge/Sensors/sensors.py --sensor_type Temperature --num_threads 1 --config_files Edge/Sensors/ConfigFiles/config_temperatura.json

# Levantar Múltiples Hilos por Sensor con Configuraciones Diferentes
python sensors.py --sensor_type Temperature --num_threads 3 --config_files ConfigFiles/config_temperatura1.json ConfigFiles/config_temperatura2.json ConfigFiles/config_temperatura3.json

# Levantar Múltiples Hilos por Sensor con la Misma Configuración
python Edge/Sensors/sensors.py --sensor_type Temperature --num_threads 10 --config_files Edge/Sensors/ConfigFiles/config_temperatura.json
