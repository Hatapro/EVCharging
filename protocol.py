"""
Protocolo estándar de comunicación Socket
Formato: <STX><DATA><ETX><LRC>
"""

import json
from colorama import Fore, Style

STX = b"\x02"  # Comienzo de mensaje
ETX = b"\x03"  # Fin de mensaje
ACK = b"\x06"  # Confirmación
NACK = b"\x15"  # No confirmación


# Calcula LRC mediante XOR byte a byte
def calcular_lrc(data):
    lrc = 0
    for byte in data:
        lrc ^= byte
    return bytes([lrc])


# Empaqueta un mensaje en formato <STX><DATA><ETX><LRC>
def empaquetar_mensaje(data_dict):
    try:
        data_json = json.dumps(data_dict).encode("utf-8")
        mensaje = STX + data_json + ETX
        lrc = calcular_lrc(mensaje)
        return mensaje + lrc

    except Exception as e:
        print(f"{Fore.RED}Error empaquetando mensaje: {e}{Style.RESET_ALL}")
        return None


# Desempaqueta un mensaje en formato <STX><DATA><ETX><LRC>
def desempaquetar_mensaje(mensaje_bytes):
    try:
        if len(mensaje_bytes) < 3:
            print(f"{Fore.RED}Mensaje demasiado corto{Style.RESET_ALL}")
            return None

        if mensaje_bytes[0:1] != STX:
            print(f"{Fore.RED}STX no encontrado{Style.RESET_ALL}")
            return None

        etx_pos = mensaje_bytes.find(ETX)
        if etx_pos == -1:
            print(f"{Fore.RED}ETX no encontrado{Style.RESET_ALL}")
            return None

        data = mensaje_bytes[1:etx_pos]
        lrc_recibido = mensaje_bytes[-1]
        mensaje_sin_lrc = mensaje_bytes[:-1]
        lrc_calculado = calcular_lrc(mensaje_sin_lrc)[0]

        if lrc_recibido != lrc_calculado:
            print(
                f"{Fore.RED}Error LRC: recibido={lrc_recibido:02x}, calculado={lrc_calculado:02x}{Style.RESET_ALL}"
            )
            return None

        data_dict = json.loads(data.decode("utf-8"))
        return data_dict

    except json.JSONDecodeError as e:
        print(f"{Fore.RED}Error JSON: {e}{Style.RESET_ALL}")
        return None
    except Exception as e:
        print(f"{Fore.RED}Error desempaquetando: {e}{Style.RESET_ALL}")
        return None


# Envía un mensaje empaquetado según protocolo estándar y espera ACK/NACK
def enviar_mensaje(sock, data_dict):
    try:
        mensaje = empaquetar_mensaje(data_dict)
        if not mensaje:
            return False

        sock.sendall(mensaje)
        respuesta = sock.recv(1)

        if respuesta == ACK:
            return True
        elif respuesta == NACK:
            print(f"{Fore.YELLOW}NACK recibido{Style.RESET_ALL}")
            return False
        else:
            print(
                f"{Fore.YELLOW}Respuesta esperada: {respuesta.hex()}{Style.RESET_ALL}"
            )
            return False

    except Exception as e:
        print(f"{Fore.RED}Error enviando mensaje: {e}{Style.RESET_ALL}")
        return False


# Recibe un mensaje empaquetado según protocolo estándar y envía ACK/NACK
def recibir_mensaje(sock, timeout=None):
    original_timeout = None
    try:
        if timeout is not None:
            original_timeout = sock.gettimeout()
            sock.settimeout(timeout)

        buffer = b""
        while True:
            byte = sock.recv(1)
            if not byte:
                return None

            if byte == STX:
                buffer = STX
                break

        while True:
            byte = sock.recv(1)
            if not byte:
                return None

            buffer += byte

            if byte == ETX:
                lrc = sock.recv(1)
                if not lrc:
                    return None
                buffer += lrc
                break

        data = desempaquetar_mensaje(buffer)

        if data:
            sock.sendall(ACK)
            return data
        else:
            sock.sendall(NACK)
            return None

    except Exception as e:
        print(f"{Fore.RED}Error recibiendo mensaje: {e}{Style.RESET_ALL}")
        try:
            sock.sendall(NACK)
        except Exception:
            pass
        return None

    finally:
        if original_timeout is not None and timeout is not None:
            try:
                sock.settimeout(original_timeout)
            except Exception:
                pass


# Envía mensaje SIN esperar ACK/NACK
def enviar_mensaje_simple(sock, data_dict):
    try:
        mensaje = empaquetar_mensaje(data_dict)
        if not mensaje:
            return False

        sock.sendall(mensaje)
        return True

    except Exception as e:
        print(f"{Fore.RED}Error enviando mensaje simple: {e}{Style.RESET_ALL}")
        return False


# Recibe mensaje SIN enviar ACK/NACK
def recibir_mensaje_simple(sock, timeout=None):
    original_timeout = None
    try:
        if timeout is not None:
            original_timeout = sock.gettimeout()
            sock.settimeout(timeout)

        buffer = b""
        while True:
            byte = sock.recv(1)
            if not byte:
                return None

            if byte == STX:
                buffer = STX
                break

        while True:
            byte = sock.recv(1)
            if not byte:
                return None

            buffer += byte

            if byte == ETX:
                lrc = sock.recv(1)
                if not lrc:
                    return None
                buffer += lrc
                break

        return desempaquetar_mensaje(buffer)

    except Exception as e:
        print(f"{Fore.RED}Error recibiendo mensaje simple: {e}{Style.RESET_ALL}")
        return None

    finally:
        if original_timeout is not None and timeout is not None:
            try:
                sock.settimeout(original_timeout)
            except Exception:
                pass
