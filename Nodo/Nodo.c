#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>

//Declaración de funciones
void configurar();

//Declaración de variables
t_config* configurador;

int main(){
	configurar();
	return 0;
}

void configurar(){
	configurador= config_create("resources/nodoConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
		printf("ARCHIVO_BIN=%s\n",config_get_string_value(configurador,"ARCHIVO_BIN")); //para probar que realmente tomó al archivo de configuración
	}

