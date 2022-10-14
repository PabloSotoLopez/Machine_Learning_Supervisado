#!/usr/bin/env python
# coding: utf-8


#Para el correcto  funcionamiento instalarse las siguientes librerias.

# Importacion paquete conexión BBDD sqlite.
import sqlite3
# Importaciones para manejo de fechas y horas.
from datetime import datetime, date, time, timedelta
import calendar
# Importación para manejo de tabalas.
import pandas as pd
import numpy as np
# Paquete para facilitar conexión con Reddit API.
import praw


# Fijamos parametros globales para pasar formatos int de Pandas a Sqlite correctamente y formato fecha global.
sqlite3.register_adapter(np.int64, lambda val: int(val))
sqlite3.register_adapter(np.int32, lambda val: int(val))
formato_fecha="%Y-%m-%d %H:%M:%S"


# Proceso de autentificación en la API, creamos una instancia de la clase Reedit del paquete praw,
# procediendo a la identificación y conexión.

    
reddit_read_only = praw.Reddit(client_id="3V8zSzmPjYST5LqQidVDow",    # client id
                     client_secret="OrBjS1QBoGwLk08f0U1sRvCZbfs1mA",  #client secret
                     user_agent="my user agent")                      #user agent name

    
   




def leer_topic(topic):
    '''Función que interactua con la tabla TOPIC. Consulta sí la cadena de busqueda ya se ha realizado con
    anterioridad. Sí es asi devuelve el id_t, sí no existe interta un registro, con la cadena.
    El id_t de la nueva consulta se lo asigna SQLite por definición tabal TOPIC.
    
    Parametro: 
    topic: Lista con los hashtag a buscar.
    Salida:    
    topic_table: Padas dataframe, reproduce TOPIC actualizado con indice en hashtag.
    
    '''
    
        
    # Creamos un objeto conexión y cursor, consultamos todos los hashtag existentes en TOPIC
    # y generamos lista.
    conex=sqlite3.connect('reddit.db')
    cursor=conex.cursor()
    # Instrucción necesaria para evitar problemas con las tuplas que devuelve SQLITE3.
    cursor.row_factory=lambda cursor,row:row[0]
    
    # Consultamos todos los hashtag existentes en TOPIC a lista python.
    topic_existentes=cursor.execute('SELECT hashtag FROM TOPIC').fetchall()
    
    # Consultamos todos los id_t existentes en TOPIC a lista.
    topic_id_existentes=cursor.execute("SELECT id_t FROM TOPIC").fetchall()
    
    # Revisamos cada elemento de la lista pasada como parametro.
    for item in topic:
     
        # Sí el elemeto no está en la lista, lo añadimos a la BBDD.TOPIC.
        sql=f"INSERT INTO TOPIC(hashtag) VALUES('{item}')"
        if item not in topic_existentes:
            cursor.execute(sql)
            conex.commit()
 
    # Volvemos a realizar la lectura de id_t y hasttag por sí se añadió algún elemento nuevo.
    topic_id=cursor.execute("SELECT id_t FROM TOPIC").fetchall()
    topic_existentes=cursor.execute('SELECT hashtag FROM TOPIC').fetchall()
    # Cerramos la conexión con la BBDD.
    conex.close()
    # Generamos un dataframe que utilizaremos como entradas de otras funciones.
    topic_table = pd.DataFrame(list(zip(topic_id,topic_existentes)), 
                               columns = ['topic_id','hashtag']).set_index('hashtag')
    

    return topic_table





def ultima_consulta(base_datos,tabla='MAIN'):
    '''Función que devuelve en formato datetime la fecha y hora de la última consulta. Está codificada, pero no se 
    utiliza en el proeeso de una única consulta. Será de utilidad, sí procedemos a realizar consultas
    automatizadas.
    Parametros: Base de datos y tabala a consultar.
    Sálida: Datetime con el tiempo de la última consulta realizada.
    
    '''
    
    # Proceso de conexión a BBDD ya explicado en leer_topic.
    conex=sqlite3.connect(base_datos)
    cursor=conex.cursor()
    cursor.row_factory=lambda cursor,row:row[0]
    
    # Ejecutamos una consulta, que nos permite obtener ts_propio del último id_m incorporado a MAIN.
    sql=f'SELECT ts_propio FROM {tabla} ORDER BY id_m DESC LIMIT 1'
    last_consulta=cursor.execute(sql).fetchone()
    
    # Intentamos transformarlo en datetime, sí no podemos es que MAIN está vacio.
    try:
        last_consulta=datetime.strptime(last_consulta, formato_fecha)
    except:
        print('Noy hay consultas previas a la hora actual')
    
    # Cerramos conexión BBDD.
    conex.close()
    
    return last_consulta
    
    





def consulta_to_pandas(consulta_individual):
    '''Función ETL, lee de Reddit, transforma los datos y extrae en formato Pandas Dataframe 
    preparado para tratar en posteriores procesos.
    
    Entrada: Lista de un elemento, con el término a buscar en Reddit api.
    Salida: Pandas dataframe ordenado que en nuestro caso, permitirá actualizar BBDD, o realizar cualquier otra
    manipulación.    
    '''
    
    # Con la función leer_topic, revisaremos si el hashtag ya está en TOPIC, o sí hay que incorporarlo.
    frame_consultas=leer_topic(consulta_individual)
   
    # Del dataframe salidad de leer_topic obtendremos el id_t del elemento en la posición 0 de la lista
    # parametro de la función. Siempre es una lista de tamaño 1, por coherencia con otras funciones, se utiliza
    # formato.
    id_consulta=frame_consultas.loc[consulta_individual[0]].at['topic_id']
    
    # Creamos una instancia de la clase subreddit de praw, y aplicamos el método search, al expecificar
    # all como parametro, hace una abstracción y busca en todos los subreddit, nivel mas alto de busqueda.
    # Busca el término parametro de la función, dados los params, los últimos ( new) y con el límite.
    # fijado como parametro global del ptograma.
    
    params = {'sort':'new','limit':limite_mensajes}
    all = reddit_read_only.subreddit("all").search(consulta_individual,**params)
    
    # De MAIN tenemos que conocer el id_m mas alto, el siguiente mensaje se incorporará con id_m+1.
    # Conectamos con MAIN, guardamos el más alto, comprobamos que es entero ( sí no lo fuese, MAIN vacio, 
    # fiajmos 1).
    
    max_id_m_sql='SELECT id_m FROM MAIN ORDER BY id_m DESC LIMIT 1'
    conex=sqlite3.connect(base_datos)
    cursor=conex.cursor()
    cursor.row_factory=lambda cursor,row:row[0]
    max_id_m_sql=cursor.execute(max_id_m_sql).fetchone()
    
    if not isinstance(max_id_m_sql, int):
        next_id_m=int(1)
    else:
        next_id_m=int(max_id_m_sql)+1
    conex.close()
    # Leemos la fecha-hora actual.
    hora_consulta=datetime.now().strftime(formato_fecha)
    
  
    
    # Instanciamos una lista vacia para ir preparando toda la información de cada post.
    # Y Ciclamos en cada elemento del objeto all de praw. Cada elemento del objeto es un
    # mensaje (submsion) de reddit.
    # Los objetos sumision.... provienene de all, objeto praw.subreddit, la fecha_consulta e id_m 
    # de esta función, y el resto son variables globales de este programa.
    # Se va incrementando id_m en cada cuelta.
    
    # NOTA: Concatemos subreddit+display_name+title+selftext,por coherencia con otras rrss.
    posts=[]      
    for submission in all:
        
        posts.append([
                next_id_m,
                usuario_consulta,
                submission.id,
                submission.subreddit.display_name+submission.title+submission.selftext,
                hora_consulta,
                submission.created_utc,
                red_social,
                submission.author.name,
                submission.subreddit.display_name,
                submission.num_comments,
                submission.score,
                submission.upvote_ratio,
                id_consulta])
        next_id_m += 1

        
    # Tranformamos la lista obtenida en un dataframe.
    posts = pd.DataFrame(posts,columns=[
                'id_m',
                'user_cons',
                'id_rrss',
                'subreddit+title+text',
                'time_consulta',
                 'created_utc',
                'red social',
                'author',
                'subreddit',
                'num_comments',
                'score',
                'upvote_ratio',
                'id_consulta'

            ])
    
    # Añadimos una columan que transforma timestamp en string con formato.
    posts['date'] = pd.to_datetime(posts['created_utc'], unit='s').astype(str)
    
    # Ordenamos por hora del mensaje descendente. Segurmaente no haga falta, pero lo mantenemos.
    posts=posts.sort_values('date',ascending=False)
    
    # Comprobamos sí post no está vacio, de estar vacio estos procesos generarían un error.
    if len(posts)>0:
        # Calculamos los likes y dislikes, la API de reddit da puntuación y ratio de positivos.
        posts['downs'] =posts.apply(lambda row: row.score- (int(row.score*row.upvote_ratio)), axis=1)
        posts['ups'] =posts.apply(lambda row: (row.score+ row.downs), axis=1)
        #Por definición nuestro número de interaccioens es like + dislike + nem comentarios.
        posts['num_interacciones']=posts['ups']+posts['downs']+posts['num_comments']
    else:
        posts['downs'] =0
        posts['ups'] =0
        posts['num_interacciones']=0
        

    # Retornamos el dataframe posts.
    return posts



def insert_comment(posts):
    """
    Función que trata el dataframe obtenido de consulta_to_pandas e incorpora a las tablas de la BBDD,
    en concreto a MAIN, MAIN_TOPIC Y MAIN_INTERACCION, TODOS LOS MENSAJES QUE NO EXISTAN PREVIAMENTE
    EN MAIN.
    Sí un mensaje ya está en MAIN, lo descartará. Utilizará la clave única generada por reddit 
    para comprobarlo.
    A partir de este  dataframe posts filtrado, lo iemos manipuladno y generando
    dataframes intermedios con los campos exactos a incorporar en cada una de las tablas BBDD,
    los tendremos que transformar en tuplas de python para facilitar el intercambio de información con
    sqlite.
    Entrada: Pandas dataframe de la función consulta_to_pandas. Consultas a BBDD.
    Salidas: Mensajes de progreso y resumen de consultas y tiempo tardaado. Incorporación registros a tablas BBDD.
    P

    """
    # Contecamos a la base de datos
    conex=sqlite3.connect(base_datos)
    cursor=conex.cursor()
    cursor.row_factory=lambda cursor,row:row[0]
    
    # Consultamos todos los id_mensaje de la tabla MAIN. El campo es el id del mensaje de reddit.
    id_mensajes_main=cursor.execute('SELECT id_mensaje FROM MAIN').fetchall()
    
    # Excluimos de los post a añadir todos los que su id ya esté en MAIN.
    # Nota filtrado daaframe: Filtramos los elementos de post cuyo id_rrss no esté en la
    # lista id_mensajes_main que acabamos de generar consultando a la tabla MAIN.
    post_not_in_main =posts[~posts.id_rrss.isin(id_mensajes_main)]
    registros_anadir_main=len(post_not_in_main.index)
   
    
    # Trasnformamos en tupla las columnas necesarias para añadir en MAIN de dataframe filtrado.
    # Es el formato mas cómodo para evitar problemas en los INSERT de sqlite.
    records_main = tuple(post_not_in_main[['id_m','user_cons','subreddit+title+text',
                                'time_consulta','date','red social','author','num_interacciones','id_rrss']].to_records(index=False))
    
    
    # Hacemos el mismo proceso anterior para los registros a incorporar en la tabla BBDD MAIN_TOPIC.
    records_main_topic=tuple(post_not_in_main[['id_consulta','id_m']].to_records(index=False))
    
    
    # Para la tabla BBDD MAIN_INTERACCION, necesitamos transformar como está ordenada la información en nuestro
    # dataframe, de un formato id_mensaje,ups... en una linea a un formato en el que cada mendaje aparecerá
    # en tres registros junto con su clave in interaccion de BBDD INTERACCION y el número de ellas.
    # Como aclaración se trata de hacer un pandas.stack, acompañando de reonmbar columnas e
    # ir acompañandolo de fijar y abandonar indices queconsigan nuestro proposito.
    
    main_interaccion_pandas=post_not_in_main[['id_m','ups','downs','num_comments']]
    main_interaccion_pandas=main_interaccion_pandas.rename(
                                    columns={'id_m':'id_m','ups':1,'downs':2,'num_comments':3
                                            }).set_index('id_m').stack().reset_index().rename(columns=
                                    {'level_1':'tipo',0:'num'})
    
    # Transformamos en tupla el dataframe a incorporar en tabla BBDD MAIN_INTERACCION.
    records_main_interaccion=tuple(main_interaccion_pandas.to_records(index=False))
    
    
    # Con todo preparado, pasamos al proceso de incorporación de los registros a nuestra BBDD.
    
    # Proceso de insercción en la tabla Main.
    sql = ''' INSERT INTO MAIN(id_m,login,comentario,ts_propio,ts_tweet,rrss,usuario,n_interacciones,id_mensaje)
              VALUES(?,?,?,?,?,?,?,?,?) '''
    
    # Ciclamos por cada elemento de la tupla MAIN.
    for comentario in records_main:
        # Intentamos añadir el comentario a MAIN, sí no puede, generamos excpeción,
        # Es una medida preventiva, de bloqueo de BBDD, por manipulación fuera del programa,
        # nunca sobra.
        try:        
            cursor.execute(sql, comentario)
        except:
            print('1 error en insercción en MAIN')
    # Terminamos con un commit.
    conex.commit()
    
    # Pasamos a la tabla BBDD MAIN_TOPIC
    sql1 = ''' INSERT INTO MAIN_TOPIC(id_topic,id_main)
              VALUES(?,?) '''
    for elemento in records_main_topic:
         # Intentamos añadir el comentario a MAIN, sí ya existe por definición de la tabla,
        # es que ya existe y no lo añade.
        try:        
            cursor.execute(sql1, elemento)
        except:
            print('1 error en insercción Main_topic')
    conex.commit()
    
    # Termiamos con la tabla BBDD MAIN_INTERACCION.    
    sql2 = ''' INSERT INTO MAIN_INTERACCION(id_m,id_i,n_interaccion)
              VALUES(?,?,?) '''
    for elemento in records_main_interaccion:
         # Intentamos añadir el comentario a MAIN, sí ya existe por definición de la tabla,
        # es que ya existe y no lo añade.
        try:        
            cursor.execute(sql2, elemento)
        except:
            print('1 error en insercción Main_interaccion')
    conex.commit()
    
    # Cerramos la conexión con la BBDD.
    conex.close()
   
    # Retornamos un mensaje de cortesia con el numero de reistros incorporados a MAIN.
    return print(f'Se han añadido {registros_anadir_main} mensajes a Main. Proceso terminado')    





def actualizar_todas_consultas(consulta):
    '''
    Función de entrada a todo el proceso de actualización/incorporación. Dada una lista de python no vacia
    '''
    
    inicio = datetime.now()
    id_consulta=leer_topic(consulta)
    a_realizar=list(id_consulta.index)
    contador=0
    for item in a_realizar:
        print(f"En progreso consulta/actualización ... {item}.")
        insert_comment(consulta_to_pandas([item]))
        contador+=1
    fin = datetime.now()
    tiempo_ejecuccion=fin-inicio
    tiempo_ejecuccion=tiempo_ejecuccion-timedelta(microseconds=tiempo_ejecuccion.microseconds)
    return print(f" {contador} consultas/actualizaciones realizada con éxito en {tiempo_ejecuccion}")
        


# PARAMETROS GENERALES DE LA BUSQUEDA.
red_social='reddit'
base_datos='reddit.db'
usuario_consulta='Pablo Soto'

# Si no pones nada actualizará las consultas existentes en tabla BBDD TOPIC
consulta=['san martin']
limite_mensajes=1000



if __name__ == "__main__":
    actualizar_todas_consultas(consulta)
  







