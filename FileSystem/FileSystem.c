#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <commons/log.h>
#include <commons/config.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <commons/collections/list.h>
#include "FS_MDFS.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#define BUF_SIZE 50
#define BLOCK_SIZE 20971520


//Declaración de Funciones
int Menu();
void DibujarMenu();

//Declaración de variables globales
t_config * configurador;
t_log* logger;
fd_set master; // conjunto maestro de descriptores de fichero
fd_set read_fds; // conjunto temporal de descriptores de fichero para select()

int main(int argc , char *argv[]){
	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	struct sockaddr_in filesystem; // dirección del servidor
	struct sockaddr_in nodo; // dirección del cliente
	int fdmax; // número máximo de descriptores de fichero
	int listener; // descriptor de socket a la escucha
	int newfd; // descriptor de socket de nueva conexión aceptada
	unsigned char identificacion[BUF_SIZE]; // buffer para datos del cliente
	int yes=1; // para setsockopt() SO_REUSEADDR, más abajo
	int i, j;
	int nodos_iniciales=0;
	int control_nodos=0;
	char ** lista_ips;
	configurador= config_create("resources/fsConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
	lista_ips = config_get_array_value (configurador,"LISTA_NODOS");

	//....................................................................................

	if ((listener = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("socket");
		log_info(logger,"FALLO la creacion del socket");
		exit(-1);
	}
	// obviar el mensaje "address already in use" (la dirección ya se está usando)
	if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes,sizeof(int)) == -1) {
		perror("setsockopt");
		log_info(logger,"FALLO la ejecucion del setsockopt");
		exit(-1);
	}
	// enlazar
	filesystem.sin_family = AF_INET;
	filesystem.sin_addr.s_addr = INADDR_ANY;
	filesystem.sin_port = htons(config_get_int_value(configurador,"PUERTO_LISTEN"));
	memset(&(filesystem.sin_zero), '\0', 8);
	if (bind(listener, (struct sockaddr *)&filesystem, sizeof(filesystem)) == -1) {
		perror("bind");
		log_info(logger,"FALLO el Bind");
		exit(-1);
	}
	// escuchar
	if (listen(listener, 10) == -1) {
		perror("listen");
		log_info(logger,"FALLO el Listen");
		exit(1);
	}
	// añadir listener al conjunto maestro
	FD_SET(listener, &master);
	// seguir la pista del descriptor de fichero mayor
	fdmax = listener; // por ahora es éste el ultimo socket

	while (nodos_iniciales != strlen(lista_ips)){
		if ((newfd = accept(listener, (struct sockaddr*)&nodo,&nodo)) == -1) {
			perror ("accept");
			log_info(logger,"FALLO el ACCEPT");
		   	exit (-1);
		}
		for (i=1;i<=strlen(lista_ips);i++){
			if (strcmp(inet_ntoa(nodo.sin_addr),lista_ips[i]))
			//if (inet_ntoa(nodo.sin_addr)==config_get_array_value[i])
				nodos_iniciales++;
		}
		if (nodos_iniciales == control_nodos)
			free(newfd);
		else {
			control_nodos++;
			FD_SET(newfd, &master);
			fdmax = newfd;
			free (newfd);
		}
	}

	//Cuando sale de este ciclo el proceso FileSystem ya se encuentra en condiciones de iniciar sus tareas





	//................................................................................

	Menu();
	log_destroy(logger);
	return 0;
}

//Consola Menu
void DibujarMenu(void){
	printf("################################################################\n");
	printf("# Ingrese una opción para continuar:                           #\n");
	printf("# 1) Formatear el MDFS                                         #\n");
	printf("# 2) Eliminar, Renombrar o Mover archivos                      #\n");
	printf("# 3) Crear, Eliminar, Renombrar o Mover directorios            #\n");
	printf("# 4) Copiar un archivo local al MDFS                           #\n");
	printf("# 5) Copiar un archivo del MDFS al filesystem local            #\n");
	printf("# 6) Solicitar el MD5 de un archivo en MDFS                    #\n");
	printf("# 7) Ver, Borrar, Copiar los bloques que componen un archivo   #\n");
	printf("# 8) Agregar un nodo de datos                                  #\n");
	printf("# 9) Eliminar un nodo de datos                                 #\n");
	printf("# 10) Salir                                                    #\n");
	printf("################################################################\n");
}

int Menu(void){
	char opchar[20];
	int opcion=0;
	while (opcion !=10){
		sleep(1);
		DibujarMenu();
		printf("Ingrese opción: ");
		scanf ("%s", opchar);
		opcion = atoi (opchar);
		switch (opcion){
			case 1: printf("Eligió  Formatear el MDFS\n"); break;
			case 2: printf("Eligió Eliminar, Renombrar o Mover archivos\n"); break;
			case 3: printf("Eligió Crear, Eliminar, Renombrar o Mover directorios\n"); break;
			case 4: printf("Eligió Copiar un archivo local al MDFS\n"); break;
			case 5: printf("Eligió Copiar un archivo del MDFS al filesystem local\n"); break;
			case 6: printf("Eligió Solicitar el MD5 de un archivo en MDFS\n"); break;
			case 7: printf("Eligió Ver, Borrar, Copiar los bloques que componen un archivo\n"); break;
			case 8: printf("Eligió Agregar un nodo de datos\n"); break;
			case 9: printf("Eligió Eliminar un nodo de datos\n"); break;
			case 10: printf("Eligió Salir\n"); break;
			default: printf("Opción incorrecta. Por favor ingrese una opción del 1 al 10\n");break;
		}
	}
	return 0;
}
