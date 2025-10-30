# singletonclient.py
import socket
import json
import uuid
import sys
import argparse
import logging

def configurar_logger(verbose):
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

def cargar_input(path):
    try:
        with open(path, "r") as file:
            data = json.load(file)
        return data
    except Exception as e:
        logging.error(f"Error al leer archivo de entrada: {e}")
        sys.exit(1)

def guardar_output(data, path):
    try:
        with open(path, "w") as file:
            json.dump(data, file, indent=4)
        print(f" salida guardada en {path}")
        #print(json.dumps(data, indent=4))  #  muestra el JSON en consola también
    except Exception as e:
        logging.error(f"Error al guardar archivo de salida: {e}")
        sys.exit(1)

def send_request(request, host, port):
    logging.debug(f"Conectando a servidor {host}:{port}")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(json.dumps(request).encode("utf-8"))
        
        # Leer toda la respuesta del servidor hasta que cierre la conexión
        chunks = []
        while True:
            data = s.recv(4096)
            if not data:
                break
            chunks.append(data)
        
        response = b"".join(chunks).decode("utf-8")
        logging.debug(f"Respuesta completa recibida ({len(response)} bytes)")
        return json.loads(response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cliente para patrón Singleton/Proxy/Observer")
    parser.add_argument("-i", "--input", required=True, help="Archivo JSON de entrada") #entrada
    parser.add_argument("-o", "--output", required=False, help="Archivo JSON de salida") #salida
    parser.add_argument("-s", "--server", default="localhost", help="Host del servidor") #server
    parser.add_argument("-p", "--port", type=int, default=8080, help="Puerto del servidor") #puerto
    parser.add_argument("-v", "--verbose", action="store_true", help="Modo verbose") #comprobacion de aws
    
    args = parser.parse_args()
    configurar_logger(args.verbose)

    # Carga de datos de entrada
    request = cargar_input(args.input)
    
    # Agregar UUID del cliente automaticamente si no esta
    request["UUID"] = request.get("UUID", str(uuid.getnode()))

    # Validacion basica de campos obligatorios
    action = request.get("ACTION")
    if not action:
        logging.error("El campo 'ACTION' es obligatorio.")
        sys.exit(1)
    
    if action == "set":
        required_fields = ["id", "cp", "CUIT", "domicilio", "idreq", "idSeq", "localidad",
                           "provincia", "sede", "seqID", "telefono", "web"]
        missing = [f for f in required_fields if f not in request.get("DATA", {})]
        if missing:
            logging.error(f"Faltan campos en DATA para accion 'set': {missing}")
            sys.exit(1)

    # Enviar solicitud al servidor
    response = send_request(request, args.server, args.port)

    # Mostrar o guardar salida
    if args.output:
        guardar_output(response, args.output)
    else:
        print("Respuesta del servidor:")
        print(json.dumps(response, indent=4))
