# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 15:42:46 2016

@author: ruben
"""
import os
import datetime as dt
import time as time_t

import pandas as pd
import numpy as np
import pytz

UNIDAD_ESTACIONES = 'Z:'

PATH_ESTACION_GEONICA = UNIDAD_ESTACIONES + '/geonica/'
PATH_ESTACION_HELIOS = UNIDAD_ESTACIONES + '/Estacion_Helios/'
PATH_ESTACION_METEO = UNIDAD_ESTACIONES + '/Datos Meteo IES/'
PATH_ESTACION_CAMPANYA = ''
PATH_ESTACION_FADRIQUE = ''

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
                            data.to_csv(path_file_cache)
                        elif persistence_type == 'pickle':
                            data.to_pickle(path_file_cache)
                        elif persistence_type == 'json':
                            data.to_json(path_file_cache)
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
    >>> leido = lee_estacion(time, tipo_estacion='meteo')
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
        raise ValueError("Elige un tipo de estación: tipo_estacion='meteo', 'geonica', 'campanya', 'fadrique'")
  
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
            if tipo_estacion == 'meteo':
                dia = pd.read_csv(file, parse_dates=[0], index_col=0, delimiter='\t')#, usecols=variables) # ignora usecols para evitar pd_issue#14792
            else:
                dia = pd.read_csv(file, date_parser=parserdatetime, parse_dates=[[0, 1]], index_col=0, delimiter='\t')#, usecols=variables) # ignora usecols para evitar pd_issue#14792

        except (IOError):
            print('No se encuentra el fichero: ', file)
            dia = pd.DataFrame(index=pd.date_range(start=fecha, end=dt.datetime.combine(fecha, dt.time(23, 59)), freq='1T'))   #cuando no hay fichero, se llena de valores vacios
        
        except TypeError:
            print('Fichero con datos mal formados: ', file)
            dia = pd.DataFrame(index=pd.date_range(start=fecha, end=dt.datetime.combine(fecha, dt.time(23, 59)), freq='1T'))   #cuando no hay fichero, se llena de valores vacios
            
        except pd.errors.EmptyDataError:
            print('Fichero de datos vacío: ', file)

        dia.index.name = 'datetime'
        
        todo = pd.concat([todo, dia]).sort_index()
    
    if tipo_estacion == 'geonica':
        todo.index = (todo.index.tz_localize(pytz.utc).
                      tz_convert(pytz.timezone('Europe/Madrid')).
                      tz_localize(None))
        # todo = change_datetimeindex(todo, mode='utc->civil')

    todo = todo[~todo.index.duplicated()].reindex(time.round(freq='T'))
    todo.index = time
    
    if muestra_tiempo_lectura:
        print("Tiempo leyendo estación (s): {:.1f}".format(time_t.time() - start_time))

    return todo
