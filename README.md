# tp-2015-1c-breaking-c

## common libraries
Para bajarlas cloné el directorio fuera del directorio del proyecto breaking C e hice sudo make install. Eso me creó un directorio llamado commons en /usr/lib además de un .so

Una vez hecho esto, se pueden agregar en los .c y en los .h cualquiera de las librerías que vienen incluídas en las common libraries ya que están en el sistema y el compilador por defecto las busca en ese directorio.

Entonces lo que hice fue incluir en Filesystem.c la línea:
 #include <commons/log.h>

Pueden probar compilar Filesystem.c antes de instalar las commons para ver que da el error y luego del install volver a compilarlo y con eso se dan cuenta de que toma las librerías correctamente.

## linkear -lcommons
Para poder compilar el proceso Job, que utiliza la commons de Log, fue necesario agregar el parametro -lcommons al linker (para el build). Antes de esto, la compilación devolvía error por falta de referencia a la función, por ejemplo, a log_create.

Para agregar -lcommons: click derecho al proyecto, properties -> C/C++ Build -> settings -> GCC C Linker -> Expert settings: -> Command line parameters -> ir al final y agregar -lcommons

Se realizo para los 4 procesos (MaRTA, Nodo, Job y FileSystem)
