# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 09:12:36 2020

@author: Martin
"""

import serial
import socket
import time
import os
import datetime as dt
import struct
from pathlib import Path

from . import bbdd
from .bbdd import lee_config


# %%
###########################################################################################################
####
####        EXTRACCIÓN DE VARIABLES GLOBALES DEL FICHERO DE CONFIGURACIÓN
####
###########################################################################################################

'''
try: 
    with open(str(Path(module_path, 'estacion_config.yaml')),'r') as config_file:
        config = yaml.load(config_file, Loader = yaml.FullLoader) #Se utiliza el FullLoader para evitar un mensaje de advertencia, ver https://msg.pyyaml.org/load para mas información
                                                                     #No se utiliza el BasicLoader debido a que interpreta todo como strings, con FullLoader los valores numéricos los intrepreta como int o float

except yaml.YAMLError:
    print ("Error in configuration file.\n")
'''    
module_path = os.path.dirname(__file__)
PATH_CONFIG_PYGEONICA = str(Path(module_path, 'pygeonica_config.yaml'))

config = lee_config('Estacion', PATH_CONFIG_PYGEONICA)

# Variables globales del módulo
Estaciones = {}
for estacion in config['Estaciones']:
    Estaciones.update({estacion['Num'] : estacion['IP']})
    
BYTEORDER = config['BYTEORDER']
PASS = config['PASS']
NUMERO_USUARIO = config['NUMERO_USUARIO']
PORT = config['PORT']
TIEMPO_RTS_ACTIVO = config['TIEMPO_RTS_ACTIVO']
TIEMPO_ESPERA_DATOS = config['TIEMPO_ESPERA_DATOS']

#%%
###########################################################################################################
####
####        FUNCIONES INTERNAS DEL PROTOCOLO GEONICA
####
###########################################################################################################


def _cabecera(numero_estacion):  #La cabecera de todos los mensajes recibidos por el sistema de medición
    """
    Info
    ---------
    Función que genera la cabecera de las tramas recibidas por geonica.
    Útil a la hora de comprobar la correcta recepción de la trama.

    Parameters
    ----------
    numero_estacion : int
        El número identificativo de la estación.

    Returns
    -------
    CABECERA : bytes

    """
    
    DLE = bytes(chr(16), encoding='ascii')                  #Data Link Escape
    SYN = bytes(chr(22), encoding='ascii')                  #Syncronos Idle
    SOH = bytes(chr(1), encoding='ascii')                   #Start of Heading
    E = numero_estacion.to_bytes(2, byteorder=BYTEORDER)    #Numero de la estacion de la que se reciben los datos
    U = NUMERO_USUARIO.to_bytes(2, byteorder=BYTEORDER)     #Numero del usuario que ha solicitado los datos
    #C = b'\x00'                                            #Número de comando que se ha solicitado
    
    CABECERA = DLE + SYN + DLE + SOH + E + U #+ C
    return CABECERA


def _comprobar_recepcion(trama_bytes, numero_estacion): 
    """
    Info
    ----------
    Se comprueba la trama recibida. Devolviendo un booleano o el código del error. 
    
    Parameters
    ----------
    trama_bytes : bytes
        Trama recibida por la estación.
    numero_estacion : int
        El número identificativo de la estación.

    Returns
    -------
         True: Recepcion correcta
         False: Recepcion de bytes incorrecta
         Int: Indica el codigo del error producido (Mirar página 9 del Protocolo de comunicaciones de Geonica Meteodata 3000)
   

    """
    
    trama = bytearray(trama_bytes)
    bytes_recibidos = len(trama)
    estado = bool()
    
    if bytes_recibidos == 13:                                           #Respuesta indicando sincronizacion completada o error en la comunicación
        if trama[:8] == _cabecera(numero_estacion):          #Comporbación de que la cabecera recibida es la correcta
            if (trama[11] == 4):                                                #Bits indicando el fin de la transmisión, sincronización completada
                estado = True                                                         #Se devuelve un booleano indicando sincronización completada
            elif (trama[11] == 21):                                             #Error en la sincronización
                return int.from_bytes(trama[10], byteorder=BYTEORDER)               #Se devuelve el indicador del estado del error            
    elif bytes_recibidos == 193:            #Respuesta indicando las mediciones pedidas
        if trama[:8] == _cabecera(numero_estacion):          #Comporbación de que la cabecera recibida es la correcta
                estado = True
    else:
        estado = False                                                    #Estado de error
        
    return estado


def _visulizar_trama(trama_bytes):
    """
    Info
    ----------
    FUNCION INTERNA PARA PERMITIR VISULIZAR LA TRAMA.
    NO ES UNA FUNCÍON QUE SE UTILICE EN EL MÓDULO, SIMPLEMENTE
    SE HA PROGRAMADO PARA LA REALIZACIÓN DE PRUEBAS PRUEBAS.
    
    Este método decodifica la trama recibida de la estación, el caso expuesto a continución se produce cuando se
    solicitan los valores intanstáneos de la estación. En el caso de que se soliciten otro tipo de valores, 
    los bytes del 117(trama_bytes[116]) al 188(trama_bytes[187]) no contienen ninguna información relevante.
    
    Parameters
    ----------
    trama_bytes : bytes
        Trama recibida por la estación.

    Returns
    -------
    trama : list
        Lista de números con los bytes recibidos por la estación.

    """
    
    trama = []
    #CABECERA
    trama.append(trama_bytes[0])                                                        #Data Link Escape
    trama.append(trama_bytes[1])                                                        #Syncronos Idle
    trama.append(trama_bytes[2])                                                        #Data Link Escape
    trama.append(trama_bytes[3])                                                        #Start of Heading
    trama.append(int.from_bytes(trama_bytes[4:6], byteorder = BYTEORDER))               #Número de
    trama.append(int.from_bytes(trama_bytes[6:8], byteorder = BYTEORDER))               #   estación
    trama.append(trama_bytes[8])                                                        #Comando solicitado
    trama.append(int.from_bytes(trama_bytes[9:11], byteorder = BYTEORDER))              #Longitud de bytes de datos a entregar
    trama.append(trama_bytes[11])                                                       #Número de canales configurados
    trama.append(trama_bytes[12])                                                       #Año...
    trama.append(trama_bytes[13])                                                       #Mes...
    trama.append(trama_bytes[14])                                                       #Día...
    trama.append(trama_bytes[15])                                                       #Hora...
    trama.append(trama_bytes[16])                                                       #Minuto...
    trama.append(trama_bytes[17])                                                       #Segundo de la estación
    trama.append(trama_bytes[18])                                                       #Data Link Escape
    trama.append(trama_bytes[19])                                                       #Start of Text
    trama = trama + _decodificar_medidas(trama_bytes)                                  #Datos recibidos de los canales, y codificados en formato flotante IEEE 754 32bit(4 bytes por dato)
    
    lista1 = []                                                                         
    for i in range(48 - 1):                                                             #Número de muestra correspondiente desde el incio del perido
        inicio = 116 + i                                                                #de cálculo
        lista1.append(int.from_bytes(trama_bytes[inicio:(inicio + 2)], byteorder = BYTEORDER))
    trama.append(lista1)
    
    lista2 = []
    for i in range(24 - 1):                                                             #Indicador del estado del canal:
        lista2.append(trama_bytes[164 + i])                                             # 0:Normal 1:Alarma por umbral superior 2:Alarma por umbral inferior
    trama.append(lista2)
        
    trama.append(trama_bytes[188])                                                      #Data Link Escape
    trama.append(trama_bytes[189])                                                      #Enf of Text
    trama.append(bytearray(trama_bytes[190:192]))                                       #Checksum, equivale al XOR de los bytes pares e impares de datos, por separado; para más info ver página 11 protocolo de comunicaciones geonica
    trama.append(trama_bytes[192])                                                      #Enquiring
    
    return trama


def _genera_trama(numero_estacion, comando):
    """
    Info
    ----------
    Genera la trama que se vaa enviar a la estación.

    Parameters
    ----------
    numero_estacion : int
        El número identificativo de la estación.
    comando : int
        Indica el tipo de medidas que se quieren leer:
            1 (Medidas instantáneas)
            12 (Valores tendentes)
            13 (Medidas almacenadas en última posición)
            14 (Medio)
            15 (Acumulado)
            16 (Integrado)
            17 (Máximo)
            18 (Mínimo)
            19 (Desviación estándar)
            20 (Incremento)
            21 (Estado alarma)
            22 (Operación OR de todos los valores)

    Returns
    -------
    trama : bytes
        Trama a enviar.
    """
    
    DLE = bytes(chr(16), encoding='ascii')
    SYN = bytes(chr(22), encoding='ascii')
    E = numero_estacion.to_bytes(2, byteorder=BYTEORDER)
    comando_comm = bytes(chr(comando), encoding='ascii')
    U = NUMERO_USUARIO.to_bytes(2, byteorder=BYTEORDER)
    X = 14 * b'\x00'
    ctrl = b'\xFF' +  b'\xFF'# Verificación de la configuración (CRC16, standard ITU-TSS) 0xFFFF evita verificación
    pasw = bytes(PASS, encoding='ascii')
    ENQ = bytes(chr(5), encoding='ascii')
    
    trama = DLE + SYN + E + comando_comm + U + X + ctrl + pasw + ENQ

    return trama
    
    
def _genera_trama_sincronizar(numero_estacion, hora):
    """
    Info
    ----------
    Genera la trama para sincronizar la hora de la estación.

    Parameters
    ----------
    numero_estacion : int
        El número identificativo de la estación.
    hora : datetime.datetime
        Fecha y hora que se quiere enviar a la estación

    Returns
    -------
    trama : bytes
        Trama a enviar.
    """
    
    DLE = bytes(chr(16), encoding='ascii')
    SYN = bytes(chr(22), encoding='ascii')
    E = numero_estacion.to_bytes(2, byteorder=BYTEORDER)
    comando_sinc = bytes(chr(0), encoding='ascii') # 0: codigo sync hora
    U = NUMERO_USUARIO.to_bytes(2, byteorder=BYTEORDER)
    A = (hora.year - 2000).to_bytes(1, byteorder=BYTEORDER)        
    M = hora.month.to_bytes(1, byteorder=BYTEORDER)
    D = hora.day.to_bytes(1, byteorder=BYTEORDER)
    d = hora.isoweekday().to_bytes(1, byteorder=BYTEORDER)
    H = hora.hour.to_bytes(1, byteorder=BYTEORDER)
    m = hora.minute.to_bytes(1, byteorder=BYTEORDER)
    s = hora.second.to_bytes(1, byteorder=BYTEORDER)
    X = 7 * b'\x00'
    ctrl = b'\xFF' +  b'\xFF'# Verificación de la configuración (CRC16, standard ITU-TSS) 0xFFFF evita verificación
    pasw = bytes(PASS, encoding='ascii')
    ENQ = bytes(chr(5), encoding='ascii')
    
    trama = DLE + SYN + E + comando_sinc + U + A + M + D + d + H + m + s + X + ctrl + pasw + ENQ
        
    return trama


def _decodificar_medidas(trama_bytes):
    """
    Info
    ----------
    Decodifica la trama recibida por la estación, y devuelve las medidas en floats.

    Parameters
    ----------
    trama_bytes : bytes
        Trama recibida por la estación.

    Returns
    -------
    valor : list de floats
        Medidas de los canales.
    """
    
    trama = bytearray(trama_bytes)
    medidas = []
    canales_configurados = trama_bytes[11]                    #Bytes indicando el numero de canales configurados
    
    for i in range(canales_configurados):                          
        byte_comienzo_muestra = 20 + (4 * i)                                  #Comienzo de los bytes de datos
        byte_fin_muestra = byte_comienzo_muestra + 4                         #Longitud de cada muestra de 4bytes
        medidas.append(trama[byte_comienzo_muestra:byte_fin_muestra])   #Se añade a la lista de medidas la medida del siguiente canal   
    
    #Se pasa de la codificacion IEEE32bit a float
    valor = []
    for medida in medidas:                                #Se atraviese el array 
        valor.append(struct.unpack('>f', medida)[0])           #Por cada canal configurado, se transforma a float la medicion, actualmente codificado en IEEE 754 32bit
    
    return valor


def _decodificar_FechayHora(trama_bytes):
    """
    Info
    ----------
    Se encarga de docdificar la trama recibida y de obtener la fecha y hora de la estación.

    Parameters
    ----------
    trama_bytes : bytes
        Trama recibida por la estación.

    Returns
    -------
    date : datetime.datetime
        Fecha y hora de la estación.

    """
    #class datetime.datetime(year, month, day, hour=0, minute=0, second=0, microsecond=0, tzinfo=None, *, fold=0)  #Constructor de la clase datetime
    date = dt.datetime(trama_bytes[12] + 2000, trama_bytes[13], trama_bytes[14], trama_bytes[15], trama_bytes[16], trama_bytes[17])
    
    '''
    La trama contiene la siguiente información:
    
    date.day = trama_bytes[14]
    date.month = trama_bytes[13]
    date.year = trama_bytes[12] + 2000                  #Se le suma 2000, ya que la estación solo se almacena la centena en la que nos encontramos
    
    date.hour = trama_bytes[15]
    date.minute = trama_bytes[16]
    date.second = trama_bytes[17]
    '''
    
    return date

#%%
###########################################################################################################
####
####        FUNCIONES INTERNAS DE COMUNICACIÓN
####
###########################################################################################################


def _socket(dir_socket, trama, num_bytes):
    """
    Info
    ----------
    Esta función se encarga de abrir y configurar el socket, establecer la conexión con la estación, 
    enviar la trama deseada y recibir la respuesta de la estación.

    Parameters
    ----------
    dir_socket : string
        La dirrección IP de la estación
    trama : bytearray
        La trama que se desea enviar
        
    Returns
    -------
    lectura : bytearray
        La lectura de la estación en bruto

    """
    
    #Se crear el scoket y se conceta con la estación
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as err:
        print('Error en la creación del socket.\n %s' %(err))
        return -1
    try:
        sock.connect(dir_socket)
    except socket.error as err:
        print('Error en la conexión del socket.\n %s' %(err))
        return -1
    
    #Se envía la trama a la estación
    sock.sendall(trama)
    
    #Se espera hasta que se reciban el numero de bytes deseados
    try:
        time.sleep(2 * TIEMPO_ESPERA_DATOS)
        sock.settimeout(5 * TIEMPO_ESPERA_DATOS)
        lectura = sock.recv(num_bytes)
    except:
        print('Tiempo de espera de datos sobrepasado.\n')
        return -1
    try:
        sock.close()
    except:
        print('Error al cerrar el socket.\n')
    
    #Se devuelve la lectura obtenida
    return lectura


def _serial(dir_serial, trama):
    """
    De forma similar a _socket(), pero mediante el puerto de comunicación Serie.

    Parameters
    ----------
    dir_serial : string
        La dirrección del puerto serie por el 
        cual se va a producir la comunicación con la estación

    Returns
    -------
    lectura : bytearray
        La lectura de la estación en bruto

    """
    #Se confiura y abre el puerto serie
    try:
        ser = serial.Serial(
                    port=dir_serial,
                    baudrate=57600,
                    parity=serial.PARITY_NONE,
                    stopbits=serial.STOPBITS_ONE,
                    bytesize=serial.EIGHTBITS
                    )
    except:
        print('Error en la aperura del puerto serie.\n')
        return -1
    
    # Debe activarse la linea RTS 1seg. antes del envio para que el dispositivo se prepare, si esta en modo ahorro,
    ## Mantener nivel alto durante 100ms y descactivar es suficiente  Referencia: Protocolo de comunicaciones Geonica Meteodata 3000 Página 3 Apartado 2.a.ii
    ser.rts = True
    time.sleep(TIEMPO_RTS_ACTIVO)
    ser.rts = False
    
    #Se escribe en el buffer de salida la trama deseada y se espera un tiempo para que la estación responda
    ser.write(trama)      
    time.sleep(TIEMPO_ESPERA_DATOS)
    
    #Se lee el buffer de entrada donde debería estar la informacion recibida
    lectura = ser.read_all()
    ser.close()
    
    #Se devuelve la lectura obtenida
    return lectura


#%%
###########################################################################################################
####
####        INTERFAZ DE USUARIO
####
###########################################################################################################


def lee_canales(num_estacion, modo_comm='socket', dir_socket=None, dir_serie=None, modo=1):
    """
    Info
    ----------
    Como su nombre indica, lee los canales de una estación.

    Parameters
    ----------
    num_estacion : int
    modo_comm : str, opcional
        Por defecto es 'socket'.
    dir_socket : str, opcional
        Por defecto es None. Se obtiene del fichero de configuración.
    dir_serie : str, opcional
        Por defecto es None.
    modo : int, opcional
        Indica el tipo de medidas que se quieren leer:
            12 (Valores tendentes)
            13 (Instantáneo)
            14 (Medio)
            15 (Acumulado)
            16 (Integrado)
            17 (Máximo)
            18 (Mínimo)
            19 (Desviación estándar)
            20 (Incremento)
            21 (Estado alarma)
            22 (Operación OR de todos los valores)
        Por defecto es 1(Medidas instantáneas).

    Returns
    -------
    fecha : datetime
	medidas : dict (str : (float + str))
        Se devuelve un diccionario con los nombres de los canales como key(clave) y 
        que contiene una lista con los valores de las medidas obtenidas, 
        junto con correspondietes unidades.

    """
    
    #Se define la trama que se va a enviar, en función de la información deseada
    if (modo == 1) | ((modo >= 12) & (modo <= 22)):
        trama = _genera_trama(num_estacion, modo)
    else:
        print('Error en el modo seleccionado.\n')
        return -1
    
    
    #Se comprueba que la estación pertenece a las estaciones existentes
    if not num_estacion in Estaciones:
        print('Error en la selección de la estación, número de estación incorrecto.\n')
        return -1
    
    #Se compruba el modo de comunicación
    if modo_comm.lower() == 'socket':
        if dir_socket == None:
            dir_socket = Estaciones[num_estacion]
        #Se comprueba que dir_socket es válido
        if type(dir_socket) == str:
            #Se comprueba que la dirrecion tiene un formato adecuado
            for num in dir_socket.split('.'):
                if (int(num) < 0) | (int(num) > 255):
                    print('Error en el formato de la dirrección IP.\n')
                    return -1
            
            num_bytes = 193 #Según el protocolo de geonica, la trama recibida por la estacion es de 193 bytes (Esto no se cumple si se solicita sincronización de hora)
            #Una vez hechas las comprbaciones, comienza la comunicación
            #Se intenta realizar la comunicación hasta un número máximo de intentos(5)
            lectura = []
            intentos = 0
            while len(lectura) != num_bytes:
                lectura = _socket((dir_socket, PORT), trama, num_bytes)
                intentos += 1
                if (intentos > 5) | (lectura == -1):
                    break
            
            #Lectura errónea
            if lectura == -1:
                return -1
            
        else:
            print('Por favor, indique una dirreción IP válida.\n')
            return -1
        
    elif modo_comm.lower() == 'serial':
        #Se comprueba que dir_serie es válido
        if not(dir_serie == None) & (type(dir_socket) == str):
            #Se compruba que la dirrecion tiene un formato adecuado
            with str.upper().split('M') as puerto:
                cond_tipo = (puerto[0].isalpha() & puerto[1].isdigit()) 
                cond_formato = len(puerto) == 2
                if (not cond_tipo) | (not cond_formato):
                    print('Error en el formato de la dirrección de puerto serie.\n')
                    return -1
            
            #Una vez hechas las comprbaciones, comienza la comunicación
            lectura = _serial(dir_serie,trama)
            
            #Lectura errónea
            if lectura == -1:
                return -1
            
        else:
            print('Por favor, indique una dirreción de puerto serie.\n')
            return -1
    else:
        print('Error en la selección del modo de comunicación, modo no válido.\n')
        return -1
    
    #Tratamiento de la lectura de la estación
    
    #Se comprueba si la transmisión ha sido correcta
    estado_recepcion = _comprobar_recepcion(lectura, num_estacion)
    
    '''
    En caso de que se produzca un error, se devuelve el número del error
    Si el estado de la recepcion es correcto se devuelve un True
    En cualquier otro caso, p.ej. el número de bytes recibidos no es el esperado, el valor devuelto es un False
    ''' 
    if estado_recepcion != True:
        print("Error en la comunicacion con la estación.\n")
        return estado_recepcion
    
    #Si hay algo que leer...
    if lectura:
        #Obtencion de la fecha de la estación
        fecha = _decodificar_FechayHora(lectura)
        print('La fecha de la estación es: ')
        print(fecha)
        
        #Obtencion de las medidas instantáneas
        medidas = _decodificar_medidas(lectura)
        # print('Las medidas obtenidas son:\n')
        # print(medidas)
        
    else:
        print("Error en la recepción.\n")
        return -1
    
    canales = bbdd.get_channels_config(num_estacion)['Abreviatura'].tolist()
    
    #Se crea un lista con las unidades de las variables
    unidades = []
    for medida in canales:
        param = bbdd.get_parameters().set_index('Abreviatura')['Unidad']
        unidades.append(param.loc[medida])
    
    #Se crea un diccionario cuya clave es el nombre del canal y contiene la medida correspondiente a dicho canal
    med =[list(x) for x in zip(medidas,unidades)]
    res = dict(zip(canales, med))
    
    #Al finalizar la comunicación, se devuelve la fecha y las medidas obtenidas, junto con sus unidades
    return fecha, res


def sincroniza_hora(num_estacion, hora, modo_comm='socket', dir_socket=None, dir_serie=None):
    """
    Info
    ----------
    Función encargada de la sincronización de la fecha/hora de la estación.

    Parameters
    ----------
    num_estacion : str
    hora : dt.datetime
    modo_comm : str, opcional
        Por defecto es 'socket'.
    dir_socket : str, opcional
        Por defecto es None. Se obtiene del fichero de configuración.
    dir_serie : str, opcional
        Por defecto es None.
        
    Returns
    -------
    esatdo_recepcion:
        Devuelve True si se ha sincronizado la hora o
        el número del error recibido.

    """
    
    #Se define la trama que se va a enviar, en función de la información deseada
    trama = _genera_trama_sincronizar(num_estacion, hora)
    
    #Se comprueba que la estación pertenece a las estaciones existentes
    if not num_estacion in Estaciones:
        print('Error en la selección de la estación, número de estación incorrecto.\n')
        return -1
    
    #Se compruba el modo de comunicación
    if modo_comm.lower() == 'socket':
        if dir_socket == None:
            dir_socket = Estaciones[num_estacion]
        #Se comprueba que dir_socket es válido
        if type(dir_socket) == str:
            #Se comprueba que la dirrecion tiene un formato adecuado
            for num in dir_socket.split('.'):
                if (int(num) < 0) | (int(num) > 255):
                    print('Error en el formato de la dirrección IP.\n')
                    return -1     
                
            num_bytes = 13 #Según el protocolo de geonica, la trama recibida por la estacion es de 13 bytes
            #Una vez hechas las comprbaciones, comienza la comunicación
            #Se intenta realizar la comunicación hasta un número máximo de intentos(5)
            lectura = []
            intentos = 0
            while len(lectura) != num_bytes:
                lectura = _socket((dir_socket, PORT), trama, num_bytes)
                intentos += 1
                if (intentos > 5) | (lectura == -1):
                    break
            
            #Lectura errónea
            if lectura == -1:
                return -1
            
        else:
            print('Por favor, indique una dirreción IP válida.\n')
            return -1
        
    elif modo_comm.lower() == 'serial':
        #Se comprueba que dir_serie es válido
        if not(dir_serie == None) & (type(dir_socket) == str):
            #Se compruba que la dirrecion tiene un formato adecuado
            with str.upper().split('M') as puerto:
                cond_tipo = (puerto[0].isalpha() & puerto[1].isdigit()) 
                cond_formato = len(puerto) == 2
                if (not cond_tipo) | (not cond_formato):
                    print('Error en el formato de la dirrección de puerto serie.\n')
                    return -1
            
            
            #Una vez hechas las comprbaciones, comienza la comunicación
            lectura = _serial(dir_serie,trama)
            
            #Lectura errónea
            if lectura == -1:
                return -1
            
        else:
            print('Por favor, indique una dirreción de puerto serie.\n')
            return -1
    else:
        print('Error en la selección del modo de comunicación, modo no válido.\n')
        return -1
    
    #Se compurba que la sincronización ha sido correcta
    estado_recepcion = _comprobar_recepcion(lectura, num_estacion)
    if estado_recepcion == True:
        print('Fecha sincronizada.\n')
        return True
    else:   
        print("Error en la comunicacion con la estación.\n")
        print(estado_recepcion)
        return False