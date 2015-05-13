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
#include <commons/string.h>

#define BLOCK_SIZE 10 //block size 10 bytes para hacer las pruebas, luego será 20971520
#define BUF_SIZE 50

//Declaración de funciones
char* mapearFileDeDatos();
void setBloque(int bloque,char* datos);
char* getBloque(int bloque);

//Declaración de variables Globales
t_config* configurador;
t_log* logger;
char* fileDeDatos;
int sizeFileDatos;

int main(int argc , char *argv[]){
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

	if (string_equals_ignore_case(config_get_string_value(configurador,"NODO_NUEVO"),"SI")){ //verifica si el nodo es nuevo
			strcpy(identificacion,"nuevo");
			if((send(sockfd,identificacion,sizeof(identificacion),0))==-1) {
					perror("send");
					log_info(logger,"FALLO el envio del saludo al FS");
					exit(-1);
			}
			//aca se debería modificar el valor del archivo de configuracion a NO ... ¿como? mail a Dios
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

	/*
	 *La siguiente función va a mapear el archivo de datos que esta especificado en el archivo conf
	 * a memoria, y asignarle al puntero fileDeDatos la direccion donde arranca el file. Utilizando mmap()
	 */

	fileDeDatos=mapearFileDeDatos();

	/*Generacion de datos para probar el funcionamiento de la funcion setBloque*/
		char* datosAEscribir;
		datosAEscribir=malloc(BLOCK_SIZE);
		memcpy(datosAEscribir,"Hola hola",BLOCK_SIZE);
		int bloqueAEscribir=2;
	//

	setBloque(bloqueAEscribir,datosAEscribir); // Grabará los datos enviados en el bloque solicitado

	/*Generación de datos para probar la funcion getBloque*/

		char* datosLeidos;
		datosLeidos=malloc(BLOCK_SIZE);
		int bloqueALeer=1;
	//

	datosLeidos=getBloque(bloqueALeer); // Devolverá el contenido del bloque solicitado
	log_destroy(logger);
	return 0;
}

char* mapearFileDeDatos(){
	char* fileDatos;

	/*
	 * Abro el archivo de datos. Éste archivo se crea localmente en la máquina que ejecutará el proceso Nodo
	 * y luego se configura el nombre en el archivo de configuracion(ARCHIVO_BIN).
	 * Una manera sencilla de crearlo es truncate -s "tamaño" nombrearchivo.bin
	 * Por ejemplo el que use para las pruebas: truncate -s 50 datos.bin --> crea un file de 50 bytes
	 */


	int fileDescriptor = open((config_get_string_value(configurador,"ARCHIVO_BIN")),O_RDWR);
	struct stat estadoDelFile; //declaro una estructura que guarda el estado de un archivo
	fstat(fileDescriptor,&estadoDelFile); //guardo el estado del archivo de datos en la estructura
	sizeFileDatos=estadoDelFile.st_size; // guardo el tamaño (necesario para el mmap)

	/*se mapea a memoria,fileDatos apuntará a una direccion en donde empieza el archivo, con permisos de
	lectura escritura y ejecucion, los cambios en las direcciones de memoria a donde apunta se verán reflejados
	 en el archivo*/

	fileDatos=mmap(0,sizeFileDatos,(PROT_WRITE|PROT_READ|PROT_EXEC),MAP_SHARED,fileDescriptor,0);
	close(fileDescriptor); //Cierro el archivo
	return fileDatos;
}

void setBloque(int numBloque,char* datosAEscribir){
	/*
	* El puntero ubicacionEnElFile, se va a posicionar en el bloque que se desea escribir el archivo
	* datosAEscribir, recibido por parametro, tiene los datos que quiero escribir
	* Con el memcpy a ubicacionEnElFile, escribo en ese bloque
	*/

	char *ubicacionEnElFile;
	ubicacionEnElFile=malloc(BLOCK_SIZE);
	ubicacionEnElFile=fileDeDatos+(BLOCK_SIZE*(numBloque-1));
	memcpy(ubicacionEnElFile,datosAEscribir,BLOCK_SIZE); //Copia el valor de BLOCK_SIZE bytes desde la direccion de memoria apuntada por datos a la direccion de memoria apuntada por fileDeDatos
	return;
}

char* getBloque(int numBloque){
	/*
	* El puntero ubicacionEnElFile, se va a posicionar en el bloque de donde deseo leer los datos
	* El puntero datosLeidos, tendrá los datos que lei, y será devuelto por la funcion
	* Con el memcpy a datosLeidos, copio ese bloque
	*/

	char* datosLeidos;
	char *ubicacionEnElFile;
	datosLeidos=malloc(BLOCK_SIZE);
	ubicacionEnElFile=malloc(BLOCK_SIZE);
	ubicacionEnElFile=fileDeDatos+(BLOCK_SIZE*(numBloque-1));
	memcpy(datosLeidos,ubicacionEnElFile,BLOCK_SIZE); //Copia el valor de BLOCK_SIZE bytes desde la direccion de memoria apuntada por fileDeDatos a la direccion de memoria apuntada por datosLeidos
	return datosLeidos;
}
