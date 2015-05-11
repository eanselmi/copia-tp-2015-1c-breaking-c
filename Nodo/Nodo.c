#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>
#include <sys/mman.h>
#include <fcntl.h>

#define BLOCK_SIZE 20971520


//Declaración de funciones
void configurar();
char* mapearFileDeDatos();
void pruebammap();

//Declaración de variables
t_config* configurador;

int main(){
	char* fileDeDatos;
	configurar();
	fileDeDatos= mapearFileDeDatos(); //va a mapear el archivo de datos a memoria, y asignarle al puntero fileDeDatos la direccion donde arranca el file
	pruebammap(fileDeDatos); //solo para probar si mmap() funciona, esta funcion puede ser eliminada despues
	return 0;
}

void configurar(){
	configurador= config_create("resources/nodoConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
		printf("ARCHIVO_BIN=%s\n",config_get_string_value(configurador,"ARCHIVO_BIN")); //para probar que realmente tomó al archivo de configuración
	}

char* mapearFileDeDatos(){
	char* fileDatos;
	int fileDescriptor = open((config_get_string_value(configurador,"ARCHIVO_BIN")),O_RDWR); //Abro el archivo de datos
	struct stat estadoDelFile; //declaro una estructura que guarda el estado de un archivo
	fstat(fileDescriptor,&estadoDelFile); //guardo el estado del archivo de datos en la estructura
	printf("size:%d",estadoDelFile.st_size); // corroboro que imprima el size
	fileDatos=mmap(0,estadoDelFile.st_size,PROT_WRITE,MAP_SHARED,fileDescriptor,0);//leyendo explicación
	//Explicacion mmap--> http://www.devshed.com/c/a/braindump/the-mmap-system-call-in-linux/
	//Ejemplo: https://www.cs.purdue.edu/homes/fahmy/cs503/mmap.txt
	//Open() : http://en.wikipedia.org/wiki/Open_(system_call)
	//fstat() : http://linux.die.net/man/2/fstat
	return fileDatos;
}

void pruebammap(char* fileDeDatos){
	fileDeDatos[(BLOCK_SIZE-1)*0]="Hola";//escribiria en el bloque 0

	return;
}
