# -*- coding: utf-8 -*-
"""

@author: Martin
"""
import pyodbc
import pandas as pd
import numpy as np
import datetime as dt
import yaml
import csv
import os
import pytz
from pathlib import Path

# En el servidor SQL hay que habilitar el puerto TCP del servidor y abrirlo en el firewall
# https://docs.microsoft.com/es-es/sql/relational-databases/lesson-2-connecting-from-another-computer?view=sql-server-ver15

module_path = os.path.dirname(__file__)
PATH_CONFIG_PYGEONICA = str(Path(module_path, 'pygeonica_config.yaml'))
PATH_CONFIG_SENSORES = str(Path(module_path, 'sensores_config.yaml'))

# %%
###########################################################################################################
####
####        EXTRACCIÓN DE VARIABLES GLOBALES DEL FICHERO DE CONFIGURACIÓN
####
###########################################################################################################

def lee_config(dato, path):
    '''
    Lee del archuvo de configuración los datos que desee el usuario.

    Parameters
    ----------
    dato : str
        Opciones: Estaciones, Sensores, Servidor, BBDD
    path : str, optional
        Ruta al archivo de configuración. 
        Por defecto es el archivo sensores_config.yaml que se encuentra
        en la misma carpeta que el script.

    Returns
    -------
    Dict
        Diccionario con la configuración. Varía según la información solicitada.
    '''
     
    try: 
        with open(path,'r', encoding='utf8') as config_file:
            config_yaml = yaml.load(config_file, Loader = yaml.FullLoader) #Se utiliza el FullLoader para evitar un mensaje de advertencia, ver https://msg.pyyaml.org/load para mas información
                                                                    #No se utiliza el BasicLoader debido a que interpreta todo como strings, con FullLoader los valores numéricos los intrepreta como int o float
        return config_yaml[dato]
    except yaml.YAMLError:
        print ("Error en el fichero de configuración")
    except:
        print("Error en la lectura del fichero")
    
servidor = lee_config('Servidor', PATH_CONFIG_PYGEONICA)
bbdd = lee_config('BBDD', PATH_CONFIG_PYGEONICA)
file = lee_config('File', PATH_CONFIG_PYGEONICA)
dict_renombrar = lee_config('Dict_Rename', PATH_CONFIG_PYGEONICA)

# Variables globales del módulo
SERVER_ADDRESS = servidor['IP']  
PORT = str(servidor['Puerto'])              
DDBB_name = bbdd['Database']
database = bbdd['Nombre']
username = bbdd['Usuario']
password = bbdd['Contrasena']

DEFAULT_NAME = file['Nombre']
DEFAULT_PATH = file['Path']
    

# pyodbc.drivers() # lista de drivers disponibles
# ['SQL Server',
#  'SQL Server Native Client 11.0',
#  'ODBC Driver 11 for SQL Server']

# Instalar driver (si no está ya al instalar GEONICA SUITE 4K)
# https://www.microsoft.com/en-us/download/details.aspx?id=36434

#%%
###########################################################################################################
####
####        FUNCIONES INTERNAS DEL MÓDULO BBDD
####
###########################################################################################################

def _request_ddbb(server_address = SERVER_ADDRESS):       #request común a todas las funciones                                         
    """
    Info
    ----------
    Funcion encargada de generar el string correspondiente al
    request que es necesario cada vez que se incia a comunicacion con la base de datos.
    
    Parameters
    ----------
    server_address : str, opcional
        Formato: 255.255.255.255
        El valor por defecto es la IP del servidor Meteo.

    Returns
    -------
    str

    """
   
    request = (                                                         
            'DRIVER={ODBC Driver 11 for SQL Server};'                   ##Se seleccion el driver a utilizar
            # la ; hace que falle si hay más campos
            f'SERVER={server_address},{PORT}\{DDBB_name};'          ##Dirrección IP del servidor dónde se encuentre la base de datos 
            f'DATABASE={database};'                                     ##Nombre de la base de datos en la que se encuentran los datos
            f'UID={username};'                                          ##Usuario con el que se accede a la base de datos
            f'PWD={password}'                                           ##Contreseña de acceso a la base de datos
    )
    return request;

#%%
###########################################################################################################
####
####        INTERFAZ DE USUARIO
####
###########################################################################################################

def get_data_raw(numero_estacion, fecha_ini, fecha_fin = dt.date.today().strftime('%Y-%m-%d %H:%M')):
    """
    Info
    ----------
    Se obtienen los datos en bruto de una estacion deseada, 
        estos datos incluyen todos las funciones que estén configuradas en la estación.

    Parameters
    ----------
    numero_estacion : int
        El número identificativo de la estación.
    fecha_ini : str de datatime.date
        La fecha de incio del periodo deseado.
    fecha_fin : str de datatime.date, opcional
        Fecha final del periodo. Por defecto es la fecha de hoy.

    Returns
    -------
    data_raw : pandas.DataFrame
        DataFrame con todos los datos de la base de datos en el periodo
        deseado.

    """

    request = _request_ddbb()
    
    query_data = (
            "SELECT * FROM Datos "
            "WHERE NumEstacion = " + str(numero_estacion) + " AND "
            "Fecha >= '" + fecha_ini + "' AND "
            "Fecha < '" + fecha_fin + "'"
    ) #Se solicitan las medidas, junto son su correspondiende NumParámetro, de un periodo determinado
    
    #Se construye el DataFrame con los valores pedidos a la base de datos
    data_raw = pd.read_sql(query_data, pyodbc.connect(request))
    
    return data_raw


def get_parameters():
    """
    Info
    -------
    Mediante esta función se obtienen las funciones que están disponibles en la estacion,
    junto con su número de parametro y unidad.    
    
    Returns
    -------
    data_parameters : pandas.DateFrame
        DataFrame formada por: NumParametro, Nombre, Abreviatura y Unidad

    """
    
    request = _request_ddbb()
    
    query_parameters = (
            'SELECT NumParametro, Nombre, Abreviatura, Unidad FROM Parametros_spanish '
    )
    
    data_parameters = (
            pd.read_sql(query_parameters, pyodbc.connect(request))
    )
    
    #parametros = data_parameters.set_index('NumParametro')
    return data_parameters
    

def get_channels_config(numero_estacion):
    """
    Info
    ----------
    Devuelve una lista de los canales configurasdos en la estación indicada,
    estos canales están ordenados en el mismo orden en el que los devuelve la estación
    cuando se le solicita (mediante puerto Serie, conexión IP, etc. ) los datos de los canales.

    Parameters
    ----------
    numero_estacion : int
        El número identificativo de la estación.

    Returns
    -------
    canales : pandas.DataFrame
        DataFrame con los canales configrados, formato: NumFuncion, Abreviatura y NumParametro

    """

    request = _request_ddbb()
    
    query_channels_config = (
            'SELECT Canales.NumFuncion, Canales.Canal, Parametros_spanish.Abreviatura, Parametros_spanish.NumParametro '
            'FROM Canales '
            'INNER JOIN Parametros_spanish ON Canales.NumParametro = Parametros_spanish.NumParametro '
            'INNER JOIN Funciones ON Funciones.NumFuncion = Canales.NumFuncion '
            'WHERE NumEstacion = ' + str(numero_estacion)
    )
    
    data_channels_config = (
            pd.read_sql(query_channels_config, pyodbc.connect(request))
     )
    
    # Se obtiene la correspondecia Nombre de funcion -> número de función de la base de datos
    numFunciones = get_functions().reset_index().set_index('Nombre')
    
    # Diccionario en el que se indica que tipo de medida se desea alacenar por cada canal
    tipo_medidas = lee_config('Tipo_Lectura_Canales', PATH_CONFIG_PYGEONICA)[numero_estacion]
   
    data_channels_config.set_index('Abreviatura', inplace=True)

    # Se modifica el NunFuncion de cada medida para obtener las medidas deseadas por cada canal
    # Se recorre la lista de canales...
    for dato in data_channels_config.index:
        # Hasta que se encuentre el canal que se desee modificar...
        if dato in tipo_medidas.keys():
            # Si la medida deseada se ha configurado en la estación...
            numFuncion = int(numFunciones.loc[tipo_medidas[dato]])
            # Si sólo hay una medida configurada...
            if type(data_channels_config.loc[dato, 'NumFuncion']) == np.int64:
                if numFuncion == data_channels_config.loc[dato, 'NumFuncion']:
                    # Se modifica el dataframe, para que se guarden solo los valores deseados
                    data_channels_config.loc[dato, 'NumFuncion'] = numFuncion
            else: # En el caso de que se hayan configurado varios tipos de medidas...
                if numFuncion in data_channels_config.loc[dato, 'NumFuncion'].tolist():
                    # Se modifica el dataframe, para que se guarden solo los valores deseados
                    vals = [numFuncion] * len(data_channels_config.loc[dato, 'NumFuncion'])
                    data_channels_config.loc[dato, 'NumFuncion'] = vals
     
    data_channels_config.reset_index(inplace=True)
            
    # Se ordenan los canales por el número del 'Canal' que le corresponde en a base de datos
    data_channels_config.set_index('Canal', inplace = True)
    data_channels_config.sort_values(by = 'Canal', inplace= True)
    # Se borran los duplicados en la columna de 'Abreviatura', y se quita la columna 'Canal' que no aporta información
    # útil, ya que son valores propios de la base de datos.
    canales = data_channels_config.drop_duplicates(subset='Abreviatura').reset_index().drop(columns='Canal')
    
    return canales

def get_functions():
    """
    Info
    -------
    Devuelve una lista con el número correspondiente a la función.

    Returns
    -------
    funciones : pandas.DateFrame
        DataFrame con NumFuncion y Nombre (de la función)

    """
    
    request = _request_ddbb()
    
    query_functions = (
            'SELECT NumFuncion, Nombre FROM dbo.Funciones_MI '
            'WHERE Ididioma = 1034' #Se solicita en nombre de las funciones en español; 2057, para inglés
    )   
    
    funciones = (
            pd.read_sql(query_functions, pyodbc.connect(request))
    )
    
    # Se establece 'NumFuncion' como índice
    funciones.set_index('NumFuncion', inplace = True)
    return funciones


def lee_dia_geonica_ddbb(dia, numero_estacion, lista_campos=None):
    """
    Info
    ----------
    Se devuelven los datos de un día que se encuentran en la estación.

    Parameters
    ----------
    dia : datetime.date
        Día del que se quieren extraer los datos.
    numero_estacion : int
        Número identificativo de la estación.
    lista_campos : list, optional
        lista con campos a obtener de la BBDD. 
        Por defecto son todos los canales configurados en la estación.

    Returns
    -------
    pandas.DataFrame
        DataFrame con todos los datos de la estación, con la fecha y hora como índice.

    """

    #Si el usuario no especifica ninguna lista de campos deseados, por defecto se devuelven todos los canales
    # disponibles de la estación
    formato_fecha = 'yyyy/mm/dd hh:mm'
    if lista_campos == None:
        lista_campos = get_channels_config(numero_estacion)['Abreviatura'].tolist()
    # Se añade la fecha como columna, en el caso de que no esté incluida ya
    if not formato_fecha in lista_campos:
        lista_campos.insert(0, formato_fecha)
    
    dia_datetime = dt.datetime.combine(dia, dt.datetime.min.time())
    formato_tiempo = '%Y-%m-%d %H:%M'
    
    # Como se lee hora UTC y la civil necesita de valores del día anterior, se leen minutos del día anterior
    # El dataset tiene 1 día + 2 horas, que luego al convertir a hora civil se tomarán solo los minutos del día en cuestión
    fecha_ini = (dia_datetime - dt.timedelta(hours=2)).strftime(formato_tiempo)
    fecha_fin = (dia_datetime + dt.timedelta(hours=24)).strftime(formato_tiempo)
                                                    
    # https://docs.microsoft.com/es-es/sql/relational-databases/lesson-2-connecting-from-another-computer?view=sql-server-ver15
    
    data = get_data_raw(numero_estacion, fecha_ini, fecha_fin)
    
    # Se procesa data solo si hay contenido
    if len(data) != 0:
        # dict_estacion tiene como indice el numero_estacion y contenido 'nombre_parametro_ficheros' y 'mtype'
        # nombre, mtype
        #   'mtype'     Measurement type: Enter an integer to determine which
        #               information on each one minute interval to return.  Options are.
        #               0       1   2   3   4   5
        #               Ins.    Med Acu Int Max Min
               
        dict_estacion = get_channels_config(numero_estacion).set_index('NumParametro');
           
        def tipo_medida(d):
            try:
                return dict_estacion.loc[d]['NumFuncion']
            except:
                pass        
            
        # Selecciona las filas que tienen NumFuncion == el tipo de medida dado el NumParametro
        data = data[data['NumFuncion'] ==
                    data['NumParametro'].apply(tipo_medida)]

        # Conversion de parametros en filas a columnas del Dataframe
        data = data.pivot_table(index='Fecha', columns=[
                                'NumParametro'], values='Valor')

        # Si los valores son medias (mtype==1), sería el valor de hace 30 seg. Por lo tanto se toma el que realmente le corresponde.
        # samplea cada 30seg, se interpola para que haya valor, se desplazan los valores 30seg para cuadrar y se reajusta de nuevo con el indice original.
        
        def adapta(columna):
            try:
                #if dict_estacion[columna.name][1] == 1: Se sustituye por:
                if dict_estacion.loc[columna.name]['NumFuncion'] == 1:
                    return columna.resample('30S').interpolate(method='linear').shift(periods=-30, freq='S').reindex(data.index)
                else:
                    return columna
            except:
                pass

        data = data.apply(adapta, axis=0)
        # El ultimo valor si se ha ajustado, se queda en NaN. Se arregla tomando el penultimo + diff
        data.iloc[-1] = data.iloc[-2] + (data.diff().mean())

        # Cambia codigo NumParametro de BBDD a su nombre de fichero
        data_channels = get_parameters().set_index('NumParametro') #Se obtienen los números de los parámetros...
        data.rename(columns = data_channels['Abreviatura'], inplace=True) #... y se sustituye el NumParametro por el Nombre
        
    # cambia index a hora civil
    data.index = (data.index.tz_localize(pytz.utc).
                  tz_convert(pytz.timezone('Europe/Madrid')).
                  tz_localize(None))
    
    # filtra y se queda solo con los minutos del dia en cuestion, una vez ya se han convertido a hora civil
    data = data[str(dia)]
    
    # Si data está vacio, se crea con valores NaN
    indice_fecha = pd.Index(pd.date_range(
        start=dia, end=dt.datetime.combine(dia, dt.time(23, 59)), freq='1T'))
    if len(data) == 0:
        data = pd.DataFrame(index=indice_fecha, columns=lista_campos)

    # En caso de que el indice esté incompleto, se reindexa para que añada nuevos con valores NaN
    if len(data) != len(indice_fecha):
        data = data.reindex(index=indice_fecha)

    # En caso de que el columns esté incompleto, se reindexa para que añada nuevos con valores NaN

    # lista_campos_corta = lista_campos.copy()
    # lista_campos_corta.remove('yyyy/mm/dd hh:mm')
    # lista_campos_corta.remove('yyyy/mm/dd')
    # if set(lista_campos_corta).issuperset(data.columns):
    #     data = data.reindex(columns=lista_campos)
    if lista_campos != data.columns.tolist():
        data = data.reindex(columns=lista_campos)
    
            
    
    # # Separa y crea en 2 columnas fecha y hora
    # # tambien valdría data.index.strftime('%Y/%m/%d')
    # data['yyyy/mm/dd'] = [d.strftime('%Y/%m/%d') for d in data.index]
    # data['hh:mm'] = [d.strftime('%H:%M') for d in data.index]
    data['yyyy/mm/dd hh:mm'] = [d.strftime('%Y/%m/%d %H:%M') for d in data.index]

    return data


def genera_fichero_meteo(dia_inicial, dia_final=None, nombre_fichero=None, path_fichero=DEFAULT_PATH):
    """
    Info
    ----------
    Genera un fichero .txt por cada día del periodo solicitado.
    
    Parameters
    ----------
    dia_inicial : str(AAAA-MM-DD) o datetime-like
    dia_final : str(AAAA-MM-DD) o datetime-like, opcional
        Por defecto es el día antetior a la ejecución del código.
    nombre_fichero : string, opcional
        Por defecto es: meteo. Al nombre del fichero se añade la fecha del mismo.
        Con lo que, por defecto, se generará un fichero: meteoAAAA_MM_DD.txt
    path : string, opcional
        Formato adecuado: 'C:/mi_usuario/mis_documentos/mi_carpeta/'
        Rellenar en el caso de que el usuario
        del script no sea el servidor.

    Returns
    -------
    bool
        True: Fichero creado correctamente
        False: Error en la función

    """
    
    #Si no se indica fecha del final del peridod deseado,
    #se reciben los datos hasta el día anterior
    if dia_final == None:
        dia_final = dt.date.today() - dt.timedelta(days=1)

    #En caso de no indicarse ningun nombre de fichero:
    if nombre_fichero == None:
        nombre_fichero = DEFAULT_NAME
    
    
    # Generación fichero llamando a función lee_dia_geonica_ddbb(dia, lista_campos)
    
    # Se llama tantas veces a la funcion lee_dia_geonica_ddbb() como días haya en el perido indicado
    for d in pd.date_range(start=dia_inicial, end=dia_final):
        dia = d.date()
        
        # Listas con las estacinones en funcionamiento
        estaciones = lee_config('Estaciones Operativas', PATH_CONFIG_PYGEONICA)
        
        i = 1 # Variable auxiliar
        data = pd.DataFrame()
        # Se obtiene las variabes que no se quieren incluir en el fichero generado
        vars_excluidas = lee_config('Vars_Excluidas', PATH_CONFIG_PYGEONICA)
        fecha = 'yyyy/mm/dd hh:mm'
        # Lee BBDD y obtiene datos del dia por cada estación, y se añaden al DataFrame completo
        for estacion in estaciones:
            if i == 1:
                data = lee_dia_geonica_ddbb(dia, estacion)
                # Se eliminan las medidas que no se quieren almacenar
                for var in vars_excluidas:
                    if var in data.columns:
                        data.drop(columns=var, inplace=True)
            else:
                data_estacion = lee_dia_geonica_ddbb(dia, estacion)
                # Se eliminan las medidas que no se quieren almacenar, junto con la fecha, debido a que está se repite en cada estación
                data_estacion.drop(columns = fecha, inplace=True)
                for var in vars_excluidas:
                    if var in data_estacion.columns:
                        data_estacion.drop(columns=var, inplace=True)
                #Para que no se produzcan errorres, se asigna el sufijo "_i"(>=2) a los parámetros que coinciden en alguna estación
                data = data.join(data_estacion, rsuffix=('_' + str(i)))
            i += 1
            
        #Como la fecha y la hora son columnas compartidas, e idénticas, se elimina los duplicados y canales innecesarios.
        # data.drop(columns={'yyyy/mm/dd hh:mm_2', 'VRef Ext.', 'Bateria', 'Bateria_2', 'Est.Geo3K', 'Est.Geo3K_2'}, inplace=True)
        
        data.rename(columns=dict_renombrar, inplace=True)
        
        # Crear fichero .txt
        # Escribe la cabecera. Pandas utiliza el index standard de tipo datenum y solo
        # crea una columna y no dos como se usa normalmente con estos ficheros, por lo
        # que se escribe antes manualmente
        formato_fecha = '%Y_%m_%d'
        nombre_fichero_texto = path_fichero + nombre_fichero + \
            dia.strftime(formato_fecha) + '.txt'
        
        with open(nombre_fichero_texto, 'w', newline='') as f:
            a = csv.writer(f, delimiter='\t')
            a.writerow(data.columns)
        
        # Escribe los datos seleccionados en cols en modo append sin cabecera ni index
        data.to_csv(nombre_fichero_texto, columns=data.columns, mode='a', sep='\t',
                    float_format='%.3f', header=False, index=False, na_rep='NaN')
        
        print('Ha escrito fichero ' + nombre_fichero_texto)
        
        # Grafica
        '''
        plt.figure(figsize=(8, 6))
        plt.title('DNI+isotpyes - ' + nombre_fichero +
                  dia.strftime(formato_fecha))
        plt.grid(which='minor')
        plt.ylabel('Irradiance $\mathregular{[W·m^{-2}]}$')
        data.Top.plot(legend=True)
        data.Mid.plot(legend=True)
        data.Bot.plot(legend=True)
        data.Rad_Dir.plot(legend=True)
        plt.ylim([0, 1100])
        
        nombre_fichero_imagen = path_fichero + 'img/' + \
            nombre_fichero + dia.strftime(formato_fecha) + '.png'
        plt.savefig(nombre_fichero_imagen)
        
        print('Ha escrito fichero ' + nombre_fichero_imagen)
        '''
    
    return True


def comprueba_canales_fichero_config():
    '''
    Función que se encargada de la comprobación del fichero YAML en el que se encuantra la información
    de los sensores configurados en las estaciones.
    Esta comprobación se hace con la información obtenida de la BBDD de Geonica.

    Returns
    -------
    pertenencia_OK, orden_OK   (bool, bool)
        pertenencia_OK: Todos los sensores en el YAML están configurados en sus correspondientes estaciones 
        orden_OK: El orden de los canales lógicos configurados en el YAML coinciden con la información obtenida
        por la BBDD

    '''

    # Se definene las funciones que se van a utilizar:
    # Lectura de los canales lógico, ordenados, de las estaciones:
    def lee_canales_bbdd():
        '''
        Returns
        -------
        canales : dict
            Diccionario que contiene los canales lógicos configurados por cada estación.
            Por cada estación, identificada por su número, hay una lista que contiene los nombres
            de los canales lógicos, ordenados, que se encuentran en la estación.
        '''
        
        canales = {}
        
        # Se obtienen las estaciones operativas
        estaciones = lee_config('Estaciones Operativas', PATH_CONFIG_PYGEONICA)
            
        # Se obtienen los canles configurados por cada estación 
        for estacion in estaciones:
            info_canales = get_channels_config(estacion)['Abreviatura'].tolist()
            canales[estacion] = info_canales
        
        return canales
    
    
    ### Lectura de los sensores del fichero de configuración
    def lee_canales_fichero_config():
        '''
        Returns
        -------
        info : dict
            Diccionarioque contiene la información sobre los canales, lógicos y físicos, obtenidos
            del archivo YAML.
            El diccionario de cada estación está ordenado por el número de canal lógico.En cada estación,
            hay un diccionario por cada sensor. 
            El diciconario de cada sensor está formado por el nombre del canal lógico(su Abreviatura en la bbdd) y 
            el número de canal fisico del sensor.
        '''
        
        # Se obtiene la lista con los sensores disponibles
        lista_sensores = lee_config('Sensores', PATH_CONFIG_SENSORES)
        
        # Se inicializa el diccionario que contendrá la información de los sensores concetados 
        # a cada estación
        info = {i:{} for i in lee_config('Estaciones Operativas', PATH_CONFIG_PYGEONICA)}
        
        # Se recorre la lista de los sensores
        for sensor in lista_sensores:
            # Si el sensores está colocado en alguna estación...
            if sensor['NumEstacion'] != None:
                # Hay sensores que necesitan dos canales de medida...
                if type(sensor['NumCanal_Logico']) == str:
                    # Variable auxiliar necesaria para conocer que canal estamos añadiendo a la lista
                    i = 0
                    # Se crea una lista con los número de los canales lógicos utilizdos por el sensor
                    canales_logicos = sensor['NumCanal_Logico'].split(', ')
                    # En caso de que el sensor no esté conectado a ningún canal físico, 
                    # p.ej. sensor de viento concetado al puerto serie, se crea un a lista con Valores None,
                    #  indicando que este canal lógico no corresponde a ningún canal físico
                    if sensor['NumCanal_Fisico'] != None:
                        canales_fisicos = sensor['NumCanal_Fisico'].split(', ')
                    else:
                        canales_fisicos = []
                        for j in range(len(canales_logicos)):
                            canales_fisicos.append(None)
                    # Se crea una lista con los nombres de los parámetros asociados a cada canal lógico
                    nombres = sensor['Nombre_Parametro'].split(', ')
                    # Se recorre la lista de canales lógicos, y se añade al diccionario de la correspondiente estación
                    #  la información del canal lógico
                    for canal_logico in canales_logicos:
                        canal_logico = int(canal_logico)
                        can_fis = canales_fisicos[i]
                        nom_param = nombres[i]
                        
                         # Se añade la información al diccionario:
                        if not canal_logico in info[sensor['NumEstacion']]:
                            info[sensor['NumEstacion']][canal_logico] = {'Canal_Fisico': can_fis, 'Nombre_Parametro': nom_param}
                        else:
                            print('Los canales lógicos de dos sensores coinciden, comprobar el archivo de configuración.')
                            print('El canal lógico Nº ' + str(canal_logico) + ' tiene configurado dos sensores: ' + info[sensor['NumEstacion']][canal_logico]['Nombre_Parametro'] + ' y ' + nom_param + '.')
                        
                        # Finalmente se aumenta el valor de la variable auxiliar,
                        # indicando que canal lógico añadir a continuación
                        i += 1
                
                # En caso de que el sesor esté ocupando solo un canal lógico...
                else:
                    canal_logico = sensor['NumCanal_Logico']
                    can_fis = sensor['NumCanal_Fisico']
                    nom_param = sensor['Nombre_Parametro']
                    
                    # Se añade la información al diccionario:
                    if not canal_logico in info[sensor['NumEstacion']]:
                        info[sensor['NumEstacion']][canal_logico] = {'Canal_Fisico': can_fis, 'Nombre_Parametro': nom_param}
                    else:
                        print('Los canales lógicos de dos sensores coinciden, comprobar el archivo de configuración.')
                        print('El canal lógico Nº ' + str(canal_logico) + ' de la estación ' + str(sensor['NumEstacion']) + ' tiene configurado dos sensores: ' + info[sensor['NumEstacion']][canal_logico]['Nombre_Parametro'] + ' y ' + nom_param + '.')
                        print('El sensor ' + nom_param + ' no se va añadir a la lista de sensores. Si quieres mantener este sensor en la lista, corrija el archivo de configuración.')
        return info


    # Se inicializan as variables boleanas que indican si la información
    # del fichero YAML coinciden
    pertenencia_OK = True
    orden_OK = True
    
    # Se obtiene la información de los sensores que se va a comparar
    canales_estacion = lee_canales_bbdd()
    canales_sensores = lee_canales_fichero_config()

    # Se recorre la lista de estaciones...
    for estacion in canales_estacion.keys():
        # Se recorre los canales lógicos por cada estación...
        for canal_logico in canales_sensores[estacion].keys():
            # Se obtiene los valores que se van a comparar
            canal_logico = int(canal_logico)
            nom_param = canales_sensores[estacion][canal_logico]['Nombre_Parametro']
            # Si el nombre del canal no está en la lista de canales de la estación,
            # se indica al usuario que hay un error en el archivo de configuración
            if not nom_param in canales_estacion[estacion]:
                print('El canal ' + nom_param + ' no está en la lista de canales obtenida por la base de datos de la estación ' + str(estacion) + '. Comprobar el archivo de configuración.')
                pertenencia_OK = False
                break
            # El nombre del canal corresponde a la estación...
            else:
                # Se comprueba que el canal lógico es igual al obtenido a obtenido por la base de datos
                if canales_estacion[estacion][canal_logico - 1] != nom_param:
                    print('El canal ' + nom_param + ' no corresponde con el número de canal lógico obtenido en la estación ' + str(estacion) + '. Comprobar el archivo de configuración.')
                    orden_OK = False
                    break
    
    # En el caso de que no se haya producido ningún error, se infroma al usuario de ello...
    if orden_OK & pertenencia_OK:
        print('La configuración de los senores del archivo es igual a la obtenida por la base de datos.')
    
    return pertenencia_OK, orden_OK  