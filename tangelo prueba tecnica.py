from datetime import timedelta
from sqlalchemy import create_engine

import pandas as pd
import requests
import hashlib
import time


regions = ['africa','americas','asia','europe','oceania']
Datos = {'Region':[],'City Name':[],'Language':[],'Time':[]}
df_tangelo = pd.DataFrame(Datos)


for i in regions:
    url = f'https://restcountries.com/v3.1/region/{i}?fields=name,languages,continents'
    start_time = time.monotonic()
    data_region = requests.get(url)
    if data_region.status_code == 200:
        data_region = data_region.json()
        for e in data_region:
            language_encrypt = None
            for value in e['languages'].values():
                language_encrypt=hashlib.sha1(value.encode()).hexdigest()
            end_time = time.monotonic()
            timeDuration= timedelta(seconds=end_time - start_time).microseconds / 1000
            region_name = {'Region':e['continents'][0],'City Name':e['name']['common'],'Language':language_encrypt,'Time':timeDuration}
            df_tangelo = df_tangelo.append(region_name, ignore_index=True)

timeProcessing = df_tangelo['Time']

#Funciones de Panda para la contabilización de datos por columna
print( 'El tiempo total de la creación del dataframe fueron %s milisegundos ' %timeProcessing.sum() )
print( 'El tiempo promedio de la creación del dataframe fueron %s milisegundos' %timeProcessing.mean() )
print( 'El tiempo minimo de la creación del dataframe fueron %s milisegundos' %timeProcessing.min() )
print( 'El tiempo maximo de la creación del dataframe fueron %s milisegundos' % timeProcessing.max() )

#### Descomentar si se quiere ver el DATAFRAME  ###
print( df_tangelo )

# Creation for the exportation of  DATA in a Database of type SQLITE.
engine = create_engine('sqlite://',echo=False)
df_tangelo.to_sql('country',con=engine)


#### Descomentar si se quiere ver la consulta de prueba SQL ####
test_of_BD= engine.execute('SELECT * FROM country').fetchall()
#print(test_of_BD)

#DATAFRAME TO JSON
json_file = df_tangelo.to_json(orient= 'index')

##### DESCOMENTAR SI SE QUIERE GUARDAR UN NUEVO JSON, DIFERENTE AL QUE SE ADJUNTA EN EL REPORSITORIO ######
#with open ('my_data.json', 'w') as f:
 #   f.write (json_file)