#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>
#include <sys/mman.h>
#include <fcntl.h>

#define BLOCK_SIZE 20971520


//Declaración de funciones
void configurar();
char* mapearFileDeDatos();
//void pruebammap();
void leerDeMmap();

//Declaración de variables
t_config* configurador;
int sizeFileDatos;

int main(){
	char* fileDeDatos;
	configurar();
	fileDeDatos= mapearFileDeDatos(); //va a mapear el archivo de datos a memoria, y asignarle al puntero fileDeDatos la direccion donde arranca el file
	escribirEnArchivo(fileDeDatos); //Escribe en la direccion de memoria que apunta fileDeDatos
	leerDeArchivo(fileDeDatos); //Lee desde la direccion de memoria que apunta fileDeDatos
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
	sizeFileDatos=estadoDelFile.st_size;
	printf("size:%d\n",sizeFileDatos); // corroboro que imprima el size
	fileDatos=mmap(0,sizeFileDatos,(PROT_WRITE|PROT_READ|PROT_EXEC),MAP_SHARED,fileDescriptor,0);//leyendo explicación
	//Explicacion mmap--> http://www.devshed.com/c/a/braindump/the-mmap-system-call-in-linux/
	//Ejemplo: https://www.cs.purdue.edu/homes/fahmy/cs503/mmap.txt
	//Open() : http://en.wikipedia.org/wiki/Open_(system_call)
	//fstat() : http://linux.die.net/man/2/fstat
	close(fileDescriptor); //Cierro el archivo
	return fileDatos;
}

void escribirEnArchivo(char* fileDeDatos){
	printf("Hola\n");
	char *datos;
	datos=malloc(BLOCK_SIZE);
	*datos=1000110111111110101010101;
	memcpy(fileDeDatos,datos,BLOCK_SIZE); //Copia el valor de BLOCK_SIZE bytes desde la direccion de memoria apuntada por datos a la direccion de memoria apuntada por fileDeDatos
	return;
}


void leerDeArchivo(char* fileDeDatos){
	char* datosLeidos;
	datosLeidos=malloc(BLOCK_SIZE);
	memcpy(datosLeidos,fileDeDatos,BLOCK_SIZE); //Copia el valor de BLOCK_SIZE bytes desde la direccion de memoria apuntada por fileDeDatos a la direccion de memoria apuntada por datosLeidos
	//puts(*datosLeidos);
}
