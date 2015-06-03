# tp-2015-1c-breaking-c

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
