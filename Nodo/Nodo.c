#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <commons/log.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>

#define BLOCK_SIZE 20971520
#define BUF_SIZE 50

//Declaración de funciones
char* mapearFileDeDatos();
//void pruebammap(); la comente porque como no esta desarrollada me da mensajes que no me gustan

//void leerDeMmap(); la comente porque como no esta desarrollada me da mensajes que no me gustan

//Declaración de variables Globales
t_config* configurador;
t_log* logger;
int sizeFileDatos;

int main(int argc , char *argv[]){
	char* fileDeDatos;
	configurador= config_create("resources/nodoConfig.conf"); //se asigna el archivo de configuración especificado en la ruta

	//-------------------------- Cuerpo ppal del programa ---------------------------------------

	//------------ Variables locales a la funcion main --------------------
	int sockfd;
	unsigned char identificacion[BUF_SIZE]; //para el mensaje que envie al conectarse para identificarse, puede cambiar
	struct sockaddr_in filesystem;
	logger = log_create("./nodoLog.log", "Nodo", true, LOG_LEVEL_INFO);
	memset(&filesystem, 0, sizeof(filesystem));
	//---------------------------------------------------------------------

	//Estructura para conexion con FS
	filesystem.sin_family = AF_INET;
	filesystem.sin_addr.s_addr = inet_addr(config_get_string_value(configurador,"IP_FS"));
	filesystem.sin_port = htons(config_get_int_value(configurador,"PUERTO_FS"));
	//-------------------------------

	if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror ("socket");
		log_info(logger,"FALLO la creacion del socket");
		exit (-1);
	}
	if (connect(sockfd, (struct sockaddr *)&filesystem,sizeof(struct sockaddr)) == -1) {
		perror ("connect");
		log_info(logger,"FALLO la conexion con el FS");
		exit (-1);
	}
	log_info(logger,"Se conectó al FS IP: %s, en el puerto: %d",config_get_string_value(configurador,"IP_FS"),config_get_int_value(configurador,"PUERTO_FS"));
	// aca revisaria si el nodo es nuevo o si es un nodo que se esta reconectando y dependiendo el caso, envia un mensaje y otro

	if (1){ //algo que verifique si el nodo es nuevo o si esta reconectando, pongo 1 para que haga algo
			//si el if da verdadero por nodo nuevo
			strcpy(identificacion,"nuevo");
			if((send(sockfd,identificacion,sizeof(identificacion),0))==-1) {
					perror("send");
					log_info(logger,"FALLO el envio del saludo al FS");
					exit(-1);
			}
		}
		else {
			//si el if da da falso por nodo existente que se esta reconectando
			strcpy(identificacion,"reconectado");
			if((send(sockfd,identificacion,sizeof(identificacion),0))==-1) {
					perror("send");
					log_info(logger,"FALLO el envio del saludo al FS");
					exit(-1);
			}
		}

	fileDeDatos= mapearFileDeDatos(); //va a mapear el archivo de datos a memoria, y asignarle al puntero fileDeDatos la direccion donde arranca el file
	escribirEnArchivo(fileDeDatos); //Escribe en la direccion de memoria que apunta fileDeDatos
	leerDeArchivo(fileDeDatos); //Lee desde la direccion de memoria que apunta fileDeDatos
	log_destroy(logger);
	return 0;
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
