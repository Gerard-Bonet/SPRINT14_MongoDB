#!/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np
import pandas as pd
from pandas import read_csv
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import r2_score
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor
import seaborn as sns
import math
import statistics as stat
from sklearn.neural_network import MLPRegressor

import pymongo
from bson.son import SON


# # NIVEL 1 Y 2

# - Ejercicios:
# 
# **Crea una base de datos NoSQL** utilizando MongoDB. Añádele algunos datos de ejemplo que te permitan comprobar que eres capaz de procesar la información de manera básica.
# 
# 
# **Conecta** la base de datos NoSQL **a Python** utilizando por ejemplo pymongo.
# 
# 
# Carga algunas consultas sencillas a un **Pandas Dataframe**.

# -----------------------------------------------------------------------------------------------------------------------------

# He creado una base de datos en MongoDB a la que he llamado Sprint14, a modo de prueba de conocimeinto y manejo de Mongo. Las tres tablas no tienen relación entre ellas.  
# 
# En la base de datos hay tres colecciones: *Estrellas, venta y fútbol*. 
# 
# Las colecciones *estrellas* y *venta* han sido hechas por consola de mongodb, mientras que la de *fútbol* ha sido creada importando un CSV mediante mongoinmport. 
# 
# Procederemos a conectar con la base de datos, explorar las colecciones, a la vez que las exportamos en Datasets. 

# In[3]:


myclient = pymongo.MongoClient("mongodb://localhost:27017/")# accedo a las bases de datso


# - Miramos las bases de datos que hay y nos conectamos a *Sprint14*

# In[4]:


print(myclient.list_database_names())# miramos las bases de datos que hay 


# In[5]:


db = myclient["sprint14"]# nos conectamos a Sprint14 


# - **Miramos las colecciones**:

# In[6]:


print(db.list_collection_names())


# In[7]:


cursor= db.estrellas.find({})
for i in cursor:
    print(i)


# In[8]:


cursor= db.venta.find({}).limit(5)
for i in cursor:
    print(i)


# In[9]:


cursor= db.futbol.find({})
# al ser una colección muy grande directamente la pasamos a DF.
df=pd.DataFrame(list(cursor))
df.head()


# In[10]:


df.shape# miramos las dimensiones


# - De esta última tabla buscamos aquellos jugadores que son zurdos. 

# In[12]:


cursor= db.futbol.find({"preferred_foot": "left"})
# al ser una colección muy grande directamente la pasamos a DF.
df=pd.DataFrame(list(cursor))
df


# - **Búsquedas con filtros y pipelines**
# 
# Procederemos a hacer algunas búsquedas con filtros( por ejemplo, buscar aquellos documentos con el atributo Xi< 0) y pipeline para agrupar.  

# - Buscamos aquella estrella en la tabla estrellas que contenga la palabara "agujero negro" mediante un filtro de expresión regular

# In[22]:


cursor= db.estrellas.find( { "tipo": { "$regex": "negro" } } ) 
print(list(cursor))


# - También podemos buscar la misma estrella buscando la estrella con Null en color, y sólo miramos la columnas estrella y tipo. Sólo mostramos dos columnas

# In[37]:


cursor= db.estrellas.find( { "color": None},{"estrella":1, "_id":0, "tipo":1}) 
print(list(cursor))


# - En la tabla de futbolistas, buscamos aquellos jugadores con la columna  "Potencial" mayor a 87 y  **seleccionamos sólo unas pocas columnas** que queremos ver...

# In[35]:


cursor= db.futbol.find({"potential": {"$gt":75}},{"player_fifa_api_id":1, "_id":0,"potential":1,"preferred_foot":1 })

df=pd.DataFrame(list(cursor))
df.head()


# En los dos últimos casos hemos puesto explícitamente **"_id":0** para que no apareciese _id en la búsqueda   

# Lo siguiente que haremos es un **GROUP BY** en la tabla de *ventas*. Agruparemos por IDtrabajador, y sumaremos su resultados 

# In[55]:


cursor= db.venta.aggregate([{"$group": {"_id":"$TRABAJADOR_Idtrabajador", "total":{"$sum":"$precio"} }}])

df=pd.DataFrame(cursor)
df


# In[ ]:




