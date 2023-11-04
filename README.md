# lectura_equipos

Para instalarlo como paquete:
`pip install --upgrade --force-reinstall --no-deps git+https://github.com/isi-ies-group/lectura_equipos.git`

Ejemplo de uso:
```
import pandas as pd

time = pd.date_range(start='2014/01/01', end='2014/01/31', freq='1T')
leido = lee_estacion(time, tipo_estacion='meteo') # devuelve un df
```
###### © Universidad Politécnica de Madrid, 2019-2020
