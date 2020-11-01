# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 15:42:46 2016

@author: ruben
"""
from io import StringIO
import os
import datetime as dt
import time as time_t

import pandas as pd
import numpy as np
import pytz

# from funciones.util_solar import change_datetimeindex
# from funciones.util import interpola2

UNIDAD_COMPARTIDA = 'X:'
UNIDAD_ESTACIONES = 'Z:'

PATH_VATIMETRO_INTREPID = UNIDAD_COMPARTIDA + '/Proyectos/NGCPV/INTREPID array/medidas/vatimetro/'
PATH_VATIMETRO_FADRIQUE = UNIDAD_COMPARTIDA + '/Proyectos/NGCPV/planta Villa de Don Fadrique/medidas/vatimetro/'

PATH_INVERSOR_FADRIQUE = UNIDAD_COMPARTIDA + '/Proyectos/NGCPV/planta Villa de Don Fadrique/medidas/inversor/'

PATH_ESTACION_GEONICA = UNIDAD_ESTACIONES + '/geonica/'
PATH_ESTACION_HELIOS = UNIDAD_ESTACIONES + '/Estacion_Helios/'
PATH_ESTACION_METEO = UNIDAD_ESTACIONES + '/Datos Meteo IES/'

PATH_ESTACION_CAMPANYA = UNIDAD_COMPARTIDA + '/Proyectos/NGCPV/INTREPID array/medidas/campanya/'
PATH_ESTACION_FADRIQUE = UNIDAD_COMPARTIDA + '/Proyectos/NGCPV/planta Villa de Don Fadrique/medidas/meteo Fadrique/'

def persist_timeseries_to_file(filename_cache=None):
    """
    Persits a Pandas DataFrame object returned by a function into cache file
    using a decorator, so it decorates the function that returns the
    pd.DataFrame.

    The function receives some extra parameters to be used by the
    decorator (and to make it explicit it is advised to add them in the
    definition of the function even if they are not used in the non-cached
    version). This approach allows to modify them in each instance of the
    function:
    - enable_cache=False : actually enables the cache (allows to choose it)
    - path_cache=None : path where the cache file is saved. If None, it takes
                        the current path
    - update_cache=False : It forces to update the cache file, even if there
                            are data in it
    Also time : pd.DatetimeIndex that is the index of the pd.DataFrame should
    be the name of the parameter in the original function

    Parameters
    ----------
    filename_cache : String, default None
        Name of cache file

    Returns
    -------
    decorator : function
        Function that will persist data into cache file
    """
    from functools import wraps

    if filename_cache is None:
        raise ValueError('A cache-file name is required.')

    persistence_type = filename_cache.split('.')[1]

    def decorator(original_func):
        """
        Decorator function
        """
        # The main intended use for @wraps() is to wrap the decorated function
        # and return the wrapper.
        # If the wrapper function is not updated, the metadata of the returned
        # function will reflect the wrapper definition rather than the original
        # function definition, which is typically less than helpful.
        @wraps(original_func)
        def new_func(time, enable_cache=False, path_cache=None,
                     update_cache=False, verbose_cache=False, *args, **kwargs):
            """
            Decorated function
            """
            if not enable_cache:
                return original_func(time, *args, **kwargs)

            if path_cache is None:
                path_cache = os.path.abspath('')
            if not os.path.exists(path_cache):
                os.makedirs(path_cache)

            path_file_cache = os.path.join(path_cache, filename_cache)
            if verbose_cache:
                print('> Path cache:', path_file_cache)

            try:
                if persistence_type == 'csv':
                    cache = pd.read_csv(path_file_cache, index_col=0, parse_dates=True)
                elif persistence_type == 'pickle':
                    cache = pd.read_pickle(path_file_cache)
                elif persistence_type == 'json':
                    cache = pd.read_json(path_file_cache)
                else:
                    raise ValueError('Unknown type of persistence', persistence_type)

                if verbose_cache:
                    print('> Reading cache...')

            except (IOError, ValueError):
                if verbose_cache:
                    print('> Cache empty')
                cache = pd.DataFrame()
            
            if not update_cache:
                if time.isin(cache.index).all():
                    data = cache.loc[time]
                    if verbose_cache:
                        print('> Cache contains requested data')
    
                else:
                    if verbose_cache:
                        print('> Reading data source...')
                    data = original_func(time, **kwargs)
                    if not data.empty:
                        if persistence_type == 'csv':
                            pd.concat([data, cache], join='inner').to_csv(path_file_cache)
                        elif persistence_type == 'pickle':
                            pd.concat([data, cache], join='inner').to_pickle(path_file_cache)
                        elif persistence_type == 'json':
                            pd.concat([data, cache], join='inner').to_json(path_file_cache)
                        else:
                            raise ValueError('Unknown type of persistence', persistence_type)
                        
                        if verbose_cache:
                            print('> Updating cache with requested data...')
                    else:
                        if verbose_cache:
                            print('> Cache not updated because requested data is empty')
            else:
                data = original_func(time, **kwargs)
                if persistence_type == 'csv':
                    data.to_csv(path_file_cache)
                elif persistence_type == 'pickle':
                    data.to_pickle(path_file_cache)
                elif persistence_type == 'json':
                    data.to_json(path_file_cache)

                if verbose_cache:
                    print('> Saving data in cache...')

            return data
        return new_func
    return decorator

@persist_timeseries_to_file(filename_cache='cache_geonica.csv')
def lee_geonica(time, path_estacion=None, enable_cache=False, path_cache=None, update_cache=False):
    return lee_estacion(time, tipo_estacion='geonica', path_estacion=path_estacion)

@persist_timeseries_to_file(filename_cache='cache_helios.csv')
def lee_helios(time, path_estacion=None, enable_cache=False, path_cache=None, update_cache=False):
    return lee_estacion(time, tipo_estacion='helios', path_estacion=path_estacion)

@persist_timeseries_to_file(filename_cache='cache_meteo.csv')
def lee_meteo(time, path_estacion=None, enable_cache=False, path_cache=None, update_cache=False):
    return lee_estacion(time, tipo_estacion='meteo', path_estacion=path_estacion)

@persist_timeseries_to_file(filename_cache='cache_campanya.csv')
def lee_campanya(time, path_estacion=None, enable_cache=False, path_cache=None, update_cache=False):
    return lee_estacion(time, tipo_estacion='campanya', path_estacion=path_estacion)

@persist_timeseries_to_file(filename_cache='cache_fadrique.csv')
def lee_fadrique(time, path_estacion=None, enable_cache=False, path_cache=None, update_cache=False):
    return lee_estacion(time, tipo_estacion='fadrique', path_estacion=path_estacion)

def lee_estacion(time, tipo_estacion=None, path_estacion=None, muestra_tiempo_lectura=False):
    # copiado el 24/02/16 de funciones_solares.py
    """
    Obtiene datos de la estación para los momentos solicitados

    Parameters
    ----------
    time : pandas.Index
        Lista de momentos a leer
    path_estacion : string (default)
        Ruta de donde están los ficheros de la estación

    Returns
    -------
    todo : pandas.DataFrame
        Los valores que corresponden a 'time'

    See Also
    --------

    Examples
    --------
    >>> import pandas as pd

    >>> time = pd.date_range(start='2014/01/01', end='2014/01/31', freq='1T')
    >>> leido = lee_estacion(time, tipo_estacion='geonica')
    """

    if tipo_estacion == 'geonica':
        if path_estacion is None:
            path_estacion = PATH_ESTACION_GEONICA
    elif tipo_estacion == 'helios':
        if path_estacion is None:
            path_estacion = PATH_ESTACION_HELIOS
    elif tipo_estacion == 'meteo':
        if path_estacion is None:
            path_estacion = PATH_ESTACION_METEO
    elif tipo_estacion == 'campanya':
        if path_estacion is None:
            path_estacion = PATH_ESTACION_CAMPANYA
    elif tipo_estacion == 'fadrique':
        if path_estacion is None:
            path_estacion = PATH_ESTACION_FADRIQUE
    elif tipo_estacion is None:
        raise ValueError("Elige un tipo de estación: tipo_estacion='geonica', 'campanya', 'fadrique'")
  
    lista_fechas_time = np.unique(time.date)
    if tipo_estacion == 'geonica':
        ayer = time.date[0] - dt.timedelta(days=1)
        lista_fechas_time = np.append(lista_fechas_time, ayer)  #añade a la lista de fechas el dia anterior, para cuando se obtiene tiempo UTC haya datas que obtener
    
    # print(lista_fechas_time)
    
    def parserdatetime(date_string): # date, time strings from file - returns datetime composed as combination of datetime.date() and datetime.time()
        return dt.datetime.strptime(date_string, "%Y/%m/%d %H:%M")
    
    todo = pd.DataFrame([])
    todo.index.name = 'datetime'
    
    start_time = time_t.time()
    for fecha in lista_fechas_time:
        if fecha.year == dt.date.today().year:
            path = path_estacion
        elif tipo_estacion == 'geonica':
            path = path_estacion + str(fecha.year) + '/'
        elif tipo_estacion == 'helios':
            path = path_estacion + 'Data' + str(fecha.year) + '/'
        elif tipo_estacion == 'meteo':
            path = path_estacion + str(fecha.year) + '/'
        else:
            path = path_estacion
        
        if tipo_estacion == 'helios':
            file = path + 'data' + dt.datetime.strftime(fecha, '%Y_%m_%d') + '.txt'
        else:
            file = path + tipo_estacion + dt.datetime.strftime(fecha, '%Y_%m_%d') + '.txt'
        
        try:
            dia = pd.read_csv(file, date_parser=parserdatetime, parse_dates=[[0, 1]], index_col=0, delimiter='\t')#, usecols=variables) # ignora usecols para evitar pd_issue#14792

        except (IOError, TypeError):
            print('No se encuentra el fichero: ', file)
            dia = pd.DataFrame(index=pd.date_range(start=fecha, end=dt.datetime.combine(fecha, dt.time(23, 59)), freq='1T'))   #cuando no hay fichero, se llena de valores vacios
        
        except pd.errors.EmptyDataError:
            print('Archivo de datos vacío: ', file)

        dia.index.name = 'datetime'
        
        todo = pd.concat([todo, dia]).sort_index()
    
    if tipo_estacion == 'geonica':
        todo.index = (todo.index.tz_localize(pytz.utc).
                      tz_convert(pytz.timezone('Europe/Madrid')).
                      tz_localize(None))
        # todo = change_datetimeindex(todo, mode='utc->civil')

    todo = todo[~todo.index.duplicated()].reindex(time)
    
    if muestra_tiempo_lectura:
        print("Tiempo leyendo estación (s): {:.1f}".format(time_t.time() - start_time))

    return todo

# @persist_timeseries_to_file(filename_cache='cache_forecastio.csv')
# def lee_forecastio(time, latitud=40.417, longitud=-3.704, api_key='c5ba1f1497c4d44d5c665da0cf5dfed2', enable_cache=False, path_cache=None, update_cache=False):
#     """
#     http://blog.forecast.io/the-forecast-data-api/
#     https://developer.forecast.io/docs/v2
    
#     https://github.com/ZeevG/python-forecast.io
    
#     Please note that we only store the best data we have for a given location and time: in the past, this will usually be observations from weather stations (though we may fall back to forecasted data if we don't have any observations); in the future, data will be more accurate the closer to the present moment you request.
#     """

#     import forecastio
        
#     def lee_momento_forecastio(momento, latitud, longitud, variables=['precipIntensity', 'temperature', 'humidity', 'cloudCover', 'pressure', 'visibility', 'dewPoint']):
                
#         data_weather = forecastio.load_forecast(api_key, lat=latitud, lng=longitud, time=momento)
        
#         datos = {variable:[] for variable in variables}
        
#         momentos = []
#         for data_hora in data_weather.hourly().data:
#             momentos.append(data_hora.time + dt.timedelta(hours=data_weather.offset())) # The current timezone offset in hours from GMT

#             for variable in variables:
#                 if variable in data_hora.d:
#                     datos[variable].append(data_hora.d[variable])
#                 else: # si no tiene campo de la variable se pone un 'None'
#                     datos[variable].append(None)
                
#         return pd.DataFrame(datos, index=momentos)

#     datos = pd.DataFrame()
#     for dia in np.unique(time.date):
#         momento = dt.datetime.combine(dia, dt.time())
#         datos_dia = lee_momento_forecastio(momento, latitud=latitud, longitud=longitud)
#         datos = datos.append(datos_dia)
    
#     # Interpola en caso de que el 'time' (o el equivalente 'time_UTC') tenga una resolución mayor al minuto (resolución estación)
#     datos = interpola2(datos, time)
        
#     return datos

def genera_fichero_csv_datos_vatimetro(fichero_datos_vati_lcr, planta=None, ruta_lmg_control='C:/Program Files (x86)/LMG-CONTROL/', fichero_inacabado=False):
    """
    Genera el fichero .csv dado un fichero .lcr usando el programa del vatímetro LMG Control
     * Toma como path por defecto el de donde esta el script
     * Lee .lcr del servidor si no existe o fuerza_descarga=True
     * Sobreescribe si el fichero ya está creado
     
     NOTA: usa LMG-Control v2.36
            - Requiere instalar 'pywinauto' mediante 'pip install pywinauto'

    Parameters
    ----------


    Returns
    -------


    See Also
    --------

    Examples
    --------
    >>> 
    """
    import pywinauto
    from pywinauto.application import Application
    
    print('No tocar el teclado y el ratón durante la ejecución!')
    
    if planta == 'intrepid':
        path_local = PATH_VATIMETRO_INTREPID
    elif planta == 'fadrique':
        path_local = PATH_VATIMETRO_FADRIQUE
    else:
        raise Exception('Hay que indicar un tipo de planta correto: "intrepid", "fadrique"')
    
    ruta_fichero_lcr = os.path.join(os.path.normpath(path_local), fichero_datos_vati_lcr)
    ruta_fichero_csv = os.path.join(os.path.normpath(path_local), fichero_datos_vati_lcr[:-3]+'csv')
    
    app = Application().start(ruta_lmg_control + 'lmgcontrol.exe' +' '+ '"'+ruta_fichero_lcr+'"')# windows requiere que esté entre "" por los espacios en los nombres
    
    # fichero_inacadado porque aun está leyendo    
    if fichero_inacabado:
        # dialogo de fichero inacabado, hace click en 'Sí'
        window1 = app.Dialog
        window1.Wait('ready', timeout=30)
        window1['&Sí'].Click()
    
    # entra en menu 'File' y va a Exportar
    wxwindowclassnr = app.wxWindowClassNR
    wxwindowclassnr.Wait('ready')
    wxwindowclassnr.MenuItem('File->Export recording').Click()
    
    # Va a menu Guardar
    window2 = app['Export']
    window2.Wait('ready')
    window2['Export'].Click()
    
    # escribe ruta y nombre fichero .csv
    window3 = app.Dialog
    window3.Wait('ready')
    combobox = window3['4']
    combobox.TypeKeys(ruta_fichero_csv, with_spaces=True)
    
    # clicka 'Guardar'
    window4 = app.Dialog
    window4.Wait('ready')
    window4.Button.Click()
    
    # Si aparece el diálogo de 'Confirmar Guardar como' porque el fichero ya existe, lo sobreescribe
    if len(pywinauto.findwindows.find_windows(title='Confirmar Guardar como')) != 0:
        window_conf = app.Dialog
        window_conf.Wait('ready')
        window_conf['&Sí'].Click()
    
    # Espera a que aparezca el botón 'Close'
    while True:
        try:
            window5 = app.Dialog
            window5.Wait('ready')
            window5['Close'].Click()
            break
        except Exception:
            print('Aun no ha aparecido el botón de "Close"')
    
    # Mata el proceso
    app.Kill_()
    
    print('Fichero .csv generado')

def lee_datos_vatimetro(lista_sesiones, planta=None, path=None):
    """
    Lee sesiones de datos de vatimetro de los ficheros .csv 
     
     NOTA: usa LMG-Control v2.36

    Parameters
    ----------


    Returns
    -------


    See Also
    --------

    Examples
    --------
    >>> 
    """
    
    if planta == 'intrepid':
        if path is None:
            path_local = PATH_VATIMETRO_INTREPID
    elif planta == 'fadrique':
        if path is None:
            path_local = PATH_VATIMETRO_FADRIQUE
    else:
        raise ValueError('Hay que indicar un tipo de planta correcto: "intrepid", "fadrique"')
    
    def lee_fichero_datos_vatimetro(fichero_datos_vati_csv):
    
        ruta_fichero_local = os.path.join(path_local, fichero_datos_vati_csv)
        
        variables = ['DATE_MS', 'TIME_MS', 'P1/W', 'P2/W', 'P3/W', 'P4/W', 'UAC1/V', 'UDC4/V', 'IAC1/A', 'IDC4/A']
        
        datos_vati = pd.read_csv(ruta_fichero_local, parse_dates=[['DATE_MS','TIME_MS']], index_col='DATE_MS_TIME_MS', header=3, usecols=variables)
        
        datos_vati.rename(columns={'P1/W':'Pac1', 'P2/W':'Pac2', 'P3/W':'Pac3', 'P4/W':'Pdc', 'UAC1/V':'Vac', 'UDC4/V':'Vdc', 'IAC1/A':'Iac', 'IDC4/A':'Idc'}, inplace=True)
        
        datos_vati = datos_vati.resample('1Min').mean()
        
        if planta == 'intrepid':
            datos_vati.Pac1 *= -1     #la pinza esta al reves en intrepido!
            datos_vati.Pac3 *= -1
        
        return datos_vati
        
    datos_sistema = pd.DataFrame()
    
    for sesion in lista_sesiones:
        fichero_datos_vati_csv = '{} {}.csv'.format(sesion, planta)
            
        datos_sesion = lee_fichero_datos_vatimetro(fichero_datos_vati_csv)
        
        datos_sesion['sesion'] = sesion
        
        datos_sistema = datos_sistema.append(datos_sesion)

    return datos_sistema

def lee_datos_inversor_fadrique(fichero_excel, path_inversor_fadrique=PATH_INVERSOR_FADRIQUE):
    
# Convierte fichero inversor .xlsx a .csv
    hoja_pot_ac = pd.io.excel.read_excel(path_inversor_fadrique + fichero_excel, skiprows=5, sheetname='Leistung', index_col=0)
    hoja_volt_dc = pd.io.excel.read_excel(path_inversor_fadrique + fichero_excel, skiprows=5, sheetname='DC Spannung', index_col=0)

    hoja_pot_ac.rename(columns={'Fronius CL 60.0_1':'Pac'}, inplace=True)
    hoja_volt_dc.rename(columns={'Fronius CL 60.0_1':'Vdc'}, inplace=True)
    
    datos_inversor = pd.concat([hoja_pot_ac.Pac, hoja_volt_dc.Vdc], axis=1)
    datos_inversor.index.rename('datetime', inplace=True)
    
    return datos_inversor

def lee_de_servidor(fichero_datos, path_local=None, path_servidor='', direccion_servidor='http://138.4.46.85:8001/'):
    import requests
    
    if path_local is None:
        path_local = os.path.abspath('buffer')
        if not os.path.exists(path_local): os.makedirs(path_local)
    
    ruta_fichero_local = os.path.join(path_local, fichero_datos)
    ruta_fichero_sevidor = os.path.join(path_servidor, fichero_datos)
    
    print('Intentando obtener el fichero ' + ruta_fichero_sevidor + ' del servidor...')
    # llamada a servidor
    respuesta = requests.get(direccion_servidor + path_servidor + '/' + fichero_datos)
            
    if respuesta.status_code == 404:
        raise SystemExit('No encuentra el fichero ' + ruta_fichero_sevidor)

    # escritura en disco de fichero .lcr
    with open(ruta_fichero_local, 'wb') as f:
        print('Escibiendo el fichero en ' + ruta_fichero_local)
        f.write(respuesta.content)
        
def lee_fichero_summary_azotea(fichero_summary_azotea, path_local=None, path_servidor='', fuerza_descarga=False):
    
    if path_local is None:
        path_local = os.path.abspath('buffer')
        if not os.path.exists(path_local): os.makedirs(path_local)

    ruta_fichero_local = os.path.join(path_local, fichero_summary_azotea)
    
    if not os.path.isfile(ruta_fichero_local) or fuerza_descarga == True:
        lee_de_servidor(fichero_summary_azotea, path_local=path_local, path_servidor=path_servidor, direccion_servidor='http://138.4.46.85:8001/')
    
    with open(ruta_fichero_local) as f:
        linea = f.readline()    
        while(linea.strip()): # evita lineas en blanco
            linea = f.readline()
    
        cabecera = f.readline() # lee cabecera
    
    flujo_fichero = StringIO()
    # Recorre el fichero e ignora las líneas de cabecera superfluas
    with open(ruta_fichero_local) as f:
        # añade cabecera antes de eliminar todas las que quedan
        flujo_fichero.write(cabecera)
        
        # comprueba si la linea es de tipo cabecera (con nombres) y la quita
        for linea in f:
            if not linea.startswith('Nombre'):
                flujo_fichero.write(linea)
    
    flujo_fichero.seek(0) # "rewind" to the beginning of the StringIO object
    
    # Lee flujo de fichero sin las lineas de cabeceras duplicadas
    datos_azotea = pd.read_csv(flujo_fichero, sep='\t', encoding='latin_1', dayfirst=True, parse_dates=[['Fecha','Hora']], index_col='Fecha_Hora', decimal=',')
    
    datos_azotea.rename(columns={' NÂº Modulo':'NModulo'}, inplace=True)
    
    return datos_azotea

def lee_config(nombre_fichero, nombre_seccion='parameters'):
    """
    Devuelve valores indicados en fichero con formato tipo de fichero de configuración '.ini'

    Parameters
    ----------
    'nombre_fichero' : string
        Nombre fichero
    'nombre_seccion' : string, default='parameters'
        Nombre de sección '[nombre]' en fichero. Es un campo obligatorio en 'configparser'

    Returns
    -------
    datos_config : pandas.Series
        Objeto con campos que contienen los parámetro y sus correspondiente valores
        * Los parámetros en el fichero tienen que ser SOLO valores numéricos

    See Also
    --------

    Examples
    --------
    >>> param_vdf = lee_config('param_vdf.txt')
    """
    import configparser
    
    config = configparser.ConfigParser(inline_comment_prefixes='#')
    config.optionxform = str # All option names are passed through the optionxform() method. Its default implementation converts option names to lower case. You can disable this behaviour by replacing the RawConfigParser.optionxform() function
    
    config.read(nombre_fichero)
    
    datos_config = pd.Series(dict(config[nombre_seccion]), dtype='float')
    
    return datos_config
