# tp-2015-1c-breaking-c

## Crear un archivo falso en /tmp/archivoFalso.txt
Dentro del proceso Nodo se deben descomentar las variables globales bufFalso y bufAMediasFalso, se deben descomentar por completo las funciones crearBloqueFalso(), crearBloqueAMediasFalso() y crearArchivoFalso(). Dentro del main llamar a la funcion crearArchivoFalso() y correrlo.
Si salió bien, se encontrará el archivo falso creado en "/tmp/archivoFalso.txt" con un tamaño de 50MB. (se puede configurar el tamaño en multiplos de 10MB)
Importante: Una vez creado el archivo, volver a comentar las dos variables globales y las tres funciones por completo.

## Filesystem: Archivos y Directorios
Falta persistir lo que sería tabla de directorios y de archivos.



## conexión Job-MaRTA
Se configuro a MaRTA como server escuchando en el puerto 5000 y Job cliente, para probarlo cerciorarse de que este configurada la IP de MaRTA en el archivo .conf de Job y en el parametro MI_IP de MaRTA 

## common libraries
Para bajarlas cloné el directorio fuera del directorio del proyecto breaking C e hice sudo make install. Eso me creó un directorio llamado commons en /usr/lib además de un .so

Una vez hecho esto, se pueden agregar en los .c y en los .h cualquiera de las librerías que vienen incluídas en las common libraries ya que están en el sistema y el compilador por defecto las busca en ese directorio.

Entonces lo que hice fue incluir en Filesystem.c la línea:
 #include <commons/log.h>

Pueden probar compilar Filesystem.c antes de instalar las commons para ver que da el error y luego del install volver a compilarlo y con eso se dan cuenta de que toma las librerías correctamente.
