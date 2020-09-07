#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

from subprocess import Popen, PIPE
import datetime, calendar


def deliverTable(HiveNameTable, nameFile, dirDownload, dirDelivery, dirLocalDeliverables):
    # Example of execution
    # deliverTable(hiveCartoName,nameFileCarto,dirDownload,dirDelivery,dirLocalDeliverables)
    # hiveCartoName = Nombre de la tabla en Hive
    # nameFile = nombre del archivo que se quiere crear (sin extensión csv)
    # dirDownload = directorio de descargas, por defecto, será
    # dirDelivery = directorio de entrega, por defecto, será
    # dirLocalDeliverables = directorio local de entrega
    # La diferencia entre delivery y download es que download es sólo el directorio donde se descargan los archivos
    # delivery contiene los archivos preparados para entregar. De esta manera, separamos download de delivery.

    # Read table from Hive
    print('[' + str(datetime.datetime.now()) + ' INFO] Leyendo la tabla HIVE: ' + HiveNameTable)

    tablaHive = (spark.read.table(HiveNameTable))

    # Write to HDFS
    print('[' + str(datetime.datetime.now()) + ' INFO] Escribiendo en HDFS el archivo: ' + nameFile)
    (tablaHive
     .write
     .mode('overwrite')
     .csv(nameFile, header=True, sep='|'))

    # Delete the folder previously (just in case there has been an old version)
    p = (Popen(['rm',
                '-rf',
                dirDownload + nameFile], stdin=PIPE, stdout=PIPE, stderr=PIPE))

    output, err = p.communicate()
    if err == '':
        print('[' + str(datetime.datetime.now()) + ' INFO] Directorio ' + dirDownload + nameFile + ' borrado con éxito')
    else:
        print('[' + str(
            datetime.datetime.now()) + ' ERR] ha fallado el borrado del directorio ' + dirDownload + nameFile)

    # Download the *.csv files to local
    p = (Popen(['hadoop',
                'fs',
                '-copyToLocal',
                nameFile,
                dirDownload + '.'], stdin=PIPE, stdout=PIPE, stderr=PIPE))

    output, err = p.communicate()
    if err == '':
        print('[' + str(
            datetime.datetime.now()) + ' INFO] Archivo ' + nameFile + ' descargado con éxito al directorio ' + dirDownload)
    else:
        print('[' + str(
            datetime.datetime.now()) + ' ERR] ha fallado la descarga del archivo ' + nameFile + ' al directorio ' + dirDownload)

    # Merge the files
    p = (Popen(' '.join(['awk',
                         "'FNR==1 && NR!=1{next;}{print}'",
                         dirDownload + nameFile + '/*.csv',
                         '>',
                         # Hay que poner shell=True cuando se ejecuta una string
                         dirDelivery + nameFile + '.csv']), shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE))

    output, err = p.communicate()
    if err == '':
        print('[' + str(
            datetime.datetime.now()) + ' INFO] Archivo ' + nameFile + '.csv creado con éxito en el directorio ' + dirDelivery)
    else:
        print('[' + str(
            datetime.datetime.now()) + ' ERR] ha fallado la creación del archivo ' + nameFile + '.csv en el directorio ' + dirDelivery)

    executeCommandInLocal = \
        (' '.join(['scp',
                   'adesant3@milan-discovery-edge-387:' + dirDelivery + nameFile + '.csv',
                   dirLocalDeliverables
                   ])).replace('\\\\', '\\')

    print('[' + str(datetime.datetime.now()) + ' INFO] Si quieres descargar el archivo a tu local, puedes hacerlo ejecutando el siguiente comando \
           desde tu local: ' + executeCommandInLocal)

    print('[' + str(datetime.datetime.now()) + ' INFO] Proceso terminado con éxito!')