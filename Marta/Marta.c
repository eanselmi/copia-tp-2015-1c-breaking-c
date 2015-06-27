#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <commons/collections/list.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/temporal.h>
#include "Marta.h"



//Variables Globales
fd_set master; // conjunto maestro de descriptores de fichero
fd_set read_fds; // conjunto temporal de descriptores de fichero para select()
t_log* logger;
t_config * configurador;
int fdmax; // número máximo de descriptores de fichero
int socket_fs;
struct sockaddr_in filesystem; // dirección del servidor
struct sockaddr_in remote_job; // dirección del cliente
char mensaje[MENSAJE_SIZE];
int read_size;
t_list* jobs;
t_list* listaNodos; //lista de nodos conectados al FS
t_list* listaArchivos; //lista de archivos del FS

int main(int argc, char**argv){

	pthread_t escucha_jobs;
	configurador= config_create("resources/martaConfig.conf");
	logger = log_create("./martaLog.log", "Marta", true, LOG_LEVEL_INFO);
	char identificacion[BUF_SIZE]; //para el mensaje que envie al conectarse para identificarse, puede cambiar
	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	printf("%s\n", config_get_string_value(configurador,"IP_FS"));
	filesystem.sin_family = AF_INET;
	filesystem.sin_addr.s_addr = inet_addr(config_get_string_value(configurador,"IP_FS"));
	filesystem.sin_port = htons(config_get_int_value(configurador,"PUERTO_FS"));
	int nbytes;
	int cantNodos;
	//para recibir la informacion de los nodos
	int i;
	char nodoId[6];
	int estadoNodo;
	//char* ipNodo;
	char ipNodo[17];
	int puertoEscuchaNodo;
	//para recibir la informacion de los archivos
	int j, k, l;
	int cantArchivos;
	//char* pathArchivo;
	char nombreArchivo[100];
	uint32_t padreArchivo;
	uint32_t estadoArchivo;
	char nodoIdArchivo[6];
	int bloqueNodoArchivo;
	int cantidadBloquesArchivo;
	int cantidadCopiasArchivo;
	listaNodos = list_create(); //creo la lista para los nodos que me pasa el FS
	listaArchivos = list_create(); //creo la lista para los archivos que me pasa el FS
	jobs=list_create(); //creo la lista de jobs

//	//Variables para probar
//		t_archivo *archivo1;
//		t_archivo *archivo2;
//		t_archivo *archivo3;
//		archivo1 = malloc(sizeof(t_archivo));
//		archivo2 = malloc(sizeof(t_archivo));
//		archivo3 = malloc(sizeof(t_archivo));
//		archivo1->bloques =list_create();
//		archivo2->bloques =list_create();
//		archivo3->bloques =list_create();
//		archivo1->estado =1;
//		archivo2->estado =1;
//		archivo3->estado =1;
//		archivo1->nombre= string_new();
//		archivo2->nombre= string_new();
//		archivo3->nombre= string_new();
//		archivo1->path= string_new();
//		archivo2->path= string_new();
//		archivo3->path= string_new();
//		string_append(&archivo1->nombre, "pam");
//		string_append(&archivo2->nombre, "bruno");
//		string_append(&archivo3->nombre, "archivoTemperatura1.txt");
//		string_append(&archivo1->path, "/home/utnso/pam");
//		string_append(&archivo2->path, "/home/utnso/bruno");
//		string_append(&archivo3->path, "/user/Bruno/datos/archivoTemperatura1.txt");
//
//		t_bloque * bloque0Archivo1;
//		t_bloque * bloque1Archivo1;
//		t_bloque * bloque0Archivo2;
//		t_bloque * bloque0Archivo3;
//		t_bloque * bloque1Archivo3;
//		t_bloque * bloque2Archivo3;
//		bloque0Archivo1 = malloc(sizeof(t_bloque));
//		bloque1Archivo1 = malloc(sizeof(t_bloque));
//		bloque0Archivo2 = malloc(sizeof(t_bloque));
//		bloque0Archivo3 = malloc(sizeof(t_bloque));
//		bloque1Archivo3 = malloc(sizeof(t_bloque));
//		bloque2Archivo3 = malloc(sizeof(t_bloque));
//
//		bloque0Archivo1->copias= list_create();
//		bloque1Archivo1->copias= list_create();
//		bloque0Archivo2->copias= list_create();
//		bloque0Archivo3->copias= list_create();
//		bloque1Archivo3->copias= list_create();
//		bloque2Archivo3->copias= list_create();
//
//		t_copias* copia0Bloque0Archivo1;
//		t_copias* copia1Bloque0Archivo1;
//		t_copias* copia2Bloque0Archivo1;
//		t_copias* copia0Bloque1Archivo1;
//		t_copias* copia1Bloque1Archivo1;
//		t_copias* copia2Bloque1Archivo1;
//		t_copias* copia0Bloque0Archivo2;
//		t_copias* copia1Bloque0Archivo2;
//		t_copias* copia2Bloque0Archivo2;
//		t_copias* copia0Bloque0Archivo3;
//		t_copias* copia1Bloque0Archivo3;
//		t_copias* copia2Bloque0Archivo3;
//		t_copias* copia0Bloque1Archivo3;
//		t_copias* copia1Bloque1Archivo3;
//		t_copias* copia2Bloque1Archivo3;
//		t_copias* copia0Bloque2Archivo3;
//		t_copias* copia1Bloque2Archivo3;
//		t_copias* copia2Bloque2Archivo3;
//
//		copia0Bloque0Archivo1=malloc(sizeof(t_copias));
//		copia1Bloque0Archivo1=malloc(sizeof(t_copias));
//		copia2Bloque0Archivo1=malloc(sizeof(t_copias));
//		copia0Bloque1Archivo1=malloc(sizeof(t_copias));
//		copia1Bloque1Archivo1=malloc(sizeof(t_copias));
//		copia2Bloque1Archivo1=malloc(sizeof(t_copias));
//		copia0Bloque0Archivo2=malloc(sizeof(t_copias));
//		copia1Bloque0Archivo2=malloc(sizeof(t_copias));
//		copia2Bloque0Archivo2=malloc(sizeof(t_copias));
//		copia0Bloque0Archivo3=malloc(sizeof(t_copias));
//		copia1Bloque0Archivo3=malloc(sizeof(t_copias));
//		copia2Bloque0Archivo3=malloc(sizeof(t_copias));
//		copia0Bloque1Archivo3=malloc(sizeof(t_copias));
//		copia1Bloque1Archivo3=malloc(sizeof(t_copias));
//		copia2Bloque1Archivo3=malloc(sizeof(t_copias));
//		copia0Bloque2Archivo3=malloc(sizeof(t_copias));
//		copia1Bloque2Archivo3=malloc(sizeof(t_copias));
//		copia2Bloque2Archivo3=malloc(sizeof(t_copias));
//
//		copia0Bloque0Archivo1->nodo = string_new();
//		copia1Bloque0Archivo1->nodo = string_new();
//		copia2Bloque0Archivo1->nodo = string_new();
//		copia0Bloque1Archivo1->nodo = string_new();
//		copia1Bloque1Archivo1->nodo = string_new();
//		copia2Bloque1Archivo1->nodo = string_new();
//		copia0Bloque0Archivo2->nodo = string_new();
//		copia1Bloque0Archivo2->nodo= string_new();
//		copia2Bloque0Archivo2->nodo = string_new();
//		copia0Bloque0Archivo3->nodo = string_new();
//		copia1Bloque0Archivo3->nodo = string_new();
//		copia2Bloque0Archivo3->nodo = string_new();
//		copia0Bloque1Archivo3->nodo= string_new();
//		copia1Bloque1Archivo3->nodo = string_new();
//		copia2Bloque1Archivo3->nodo = string_new();
//		copia0Bloque2Archivo3->nodo = string_new();
//		copia1Bloque2Archivo3->nodo = string_new();
//		copia2Bloque2Archivo3->nodo = string_new();
//
//		strcpy(copia0Bloque0Archivo1->nodo, "nodo1");
//		strcpy(copia1Bloque0Archivo1->nodo, "nodo2");
//		strcpy(copia2Bloque0Archivo1->nodo, "nodo3");
//		strcpy(copia0Bloque1Archivo1->nodo, "nodo1");
//		strcpy(copia1Bloque1Archivo1->nodo, "nodo2");
//		strcpy(copia2Bloque1Archivo1->nodo, "nodo3");
//		strcpy(copia0Bloque0Archivo2->nodo, "nodo1");
//		strcpy(copia1Bloque0Archivo2->nodo, "nodo2");
//		strcpy(copia2Bloque0Archivo2->nodo, "nodo3");
//		strcpy(copia0Bloque0Archivo3->nodo, "nodo1");
//		strcpy(copia1Bloque0Archivo3->nodo, "nodo2");
//		strcpy(copia2Bloque0Archivo3->nodo, "nodo3");
//		strcpy(copia0Bloque1Archivo3->nodo, "nodo1");
//		strcpy(copia1Bloque1Archivo3->nodo, "nodo2");
//		strcpy(copia2Bloque1Archivo3->nodo, "nodo3");
//		strcpy(copia0Bloque2Archivo3->nodo, "nodo1");
//		strcpy(copia1Bloque2Archivo3->nodo, "nodo2");
//		strcpy(copia2Bloque2Archivo3->nodo, "nodo3");
//
//		copia0Bloque0Archivo1->bloqueNodo=2;
//		copia1Bloque0Archivo1->bloqueNodo=5;
//		copia2Bloque0Archivo1->bloqueNodo=7;
//		copia0Bloque1Archivo1->bloqueNodo=8;
//		copia1Bloque1Archivo1->bloqueNodo=9;
//		copia2Bloque1Archivo1->bloqueNodo=10;
//		copia0Bloque0Archivo2->bloqueNodo=2;
//		copia1Bloque0Archivo2->bloqueNodo=1;
//		copia2Bloque0Archivo2->bloqueNodo=0;
//		copia0Bloque0Archivo3->bloqueNodo=4;
//		copia1Bloque0Archivo3->bloqueNodo=3;
//		copia2Bloque0Archivo3->bloqueNodo=2;
//		copia0Bloque1Archivo3->bloqueNodo=8;
//		copia1Bloque1Archivo3->bloqueNodo=5;
//		copia2Bloque1Archivo3->bloqueNodo=2;
//		copia0Bloque2Archivo3->bloqueNodo=6;
//		copia1Bloque2Archivo3->bloqueNodo=2;
//		copia2Bloque2Archivo3->bloqueNodo=9;
//
//		list_add(bloque0Archivo1->copias, copia0Bloque0Archivo1);
//		list_add(bloque0Archivo1->copias, copia1Bloque0Archivo1);
//		list_add(bloque0Archivo1->copias, copia2Bloque0Archivo1);
//		list_add(bloque1Archivo1->copias, copia0Bloque1Archivo1);
//		list_add(bloque1Archivo1->copias, copia1Bloque1Archivo1);
//		list_add(bloque1Archivo1->copias, copia2Bloque1Archivo1);
//		list_add(bloque0Archivo2->copias, copia0Bloque0Archivo2);
//		list_add(bloque0Archivo2->copias, copia1Bloque0Archivo2);
//		list_add(bloque0Archivo2->copias, copia2Bloque0Archivo2);
//		list_add(bloque0Archivo3->copias, copia0Bloque0Archivo3);
//		list_add(bloque0Archivo3->copias, copia1Bloque0Archivo3);
//		list_add(bloque0Archivo3->copias, copia2Bloque0Archivo3);
//		list_add(bloque1Archivo3->copias, copia0Bloque1Archivo3);
//		list_add(bloque1Archivo3->copias, copia1Bloque1Archivo3);
//		list_add(bloque1Archivo3->copias, copia2Bloque1Archivo3);
//		list_add(bloque2Archivo3->copias, copia0Bloque2Archivo3);
//		list_add(bloque2Archivo3->copias, copia1Bloque2Archivo3);
//		list_add(bloque2Archivo3->copias, copia2Bloque2Archivo3);
//
//		list_add(archivo1->bloques,bloque0Archivo1);
//		list_add(archivo1->bloques,bloque1Archivo1);
//		list_add(archivo2->bloques,bloque0Archivo2);
//		list_add(archivo3->bloques,bloque0Archivo3);
//		list_add(archivo3->bloques,bloque1Archivo3);
//		list_add(archivo3->bloques,bloque2Archivo3);
//
//		list_add(listaArchivos, archivo1);
//		list_add(listaArchivos, archivo2);
//		list_add(listaArchivos, archivo3);
//
//		t_nodo *nodo1;
//		t_nodo *nodo2;
//		t_nodo *nodo3;
//		nodo1 = malloc(sizeof(t_nodo));
//		nodo2 = malloc(sizeof(t_nodo));
//		nodo3 = malloc(sizeof(t_nodo));
//
//		 nodo1->cantMappers = 0;
//		 nodo2->cantMappers = 0;
//		 nodo3->cantMappers = 0;
//		 nodo1->cantReducers = 0;
//		 nodo2->cantReducers = 0;
//		 nodo3->cantReducers = 0;
//		 nodo1->estado =1;
//		 nodo2->estado =1;
//		 nodo3->estado =1;
//
//		 nodo1->ip = string_new();
//		 nodo2->ip = string_new();
//		 nodo3->ip = string_new();
//		 strcpy(nodo1->ip,"127.0.0.1");
//		 strcpy(nodo2->ip,"127.0.0.1");
//		 strcpy(nodo3->ip,"127.0.0.1");
//		 strcpy(nodo1->nodo_id,"nodo1");
//		 strcpy(nodo2->nodo_id,"nodo2");
//		 strcpy(nodo3->nodo_id,"nodo3");
//		 nodo1->puerto_escucha_nodo = 6500;
//		 nodo2->puerto_escucha_nodo = 6510;
//		 nodo3->puerto_escucha_nodo = 6520;
//
//		 list_add(listaNodos, nodo1);
//		 list_add(listaNodos, nodo2);
//		 list_add(listaNodos, nodo3);



//====================== CONEXION CON FS =================================

	if ((socket_fs = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror ("socket");
		log_error(logger,"FALLO la creacion del socket");
		exit (-1);
	}
	if (connect(socket_fs, (struct sockaddr *)&filesystem,sizeof(struct sockaddr)) == -1) {
		perror ("connect");
		log_error(logger,"FALLO la conexion con el FS");
		exit (-1);
	}
	FD_SET(socket_fs, &master);
	fdmax = socket_fs; // por ahora es éste el ultimo socket
	strcpy(identificacion,"marta");
	if((send(socket_fs,identificacion,sizeof(identificacion),MSG_WAITALL))==-1) {
		perror("send");
		log_error(logger,"FALLO el envio del saludo al FS");
		exit(-1);
	}
	//int nbytes;  //AR los subi con el resto de las declaraciones, lo dejo comentado para revisarlo luego
	if ((nbytes = recv(socket_fs, identificacion, sizeof(identificacion), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error, no considero desconexion porque es nuevo
		perror("recv");
		log_error(logger,"FALLO el Recv");
		exit(-1);
	} else if (nbytes == 0){
		printf ("Conexion con FS cerrada, el proceso fs no esta listo o bien ya existe una instancia de marta conectada\n");
		exit(-1);
	}
	if (nbytes > 0 && strncmp(identificacion,"ok",2)==0)	log_info (logger,"Conexion con el FS exitosa");

//==================================== FIN CONEXION CON FS ====================================================


//============================ RECIBO LA LISTA DE NODOS QUE TIENE EL FS =========================================

	if ((nbytes = recv(socket_fs, &cantNodos, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
		perror("recv");
		log_error(logger,"FALLO el Recv de cantidad de nodos");
		exit(-1);
	}
	i=0;
	while (i < cantNodos){
		t_nodo* nodoTemporal = malloc(sizeof(t_nodo));
		if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv de nodoId");
			exit(-1);
		}
		if ((nbytes = recv(socket_fs, &estadoNodo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del estado del nodo");
			exit(-1);
		}
		//ipNodo=string_new();
		memset(ipNodo, '\0',17);
		if ((nbytes = recv(socket_fs, ipNodo, sizeof(ipNodo), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv de la ip del nodo");
			exit(-1);
		}
		if ((nbytes = recv(socket_fs, &puertoEscuchaNodo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del puerto escucha del nodo");
			exit(-1);
		}
		memset(nodoTemporal->nodo_id,'\0', 6);
		strcpy(nodoTemporal->nodo_id, nodoId);
		nodoTemporal->estado =estadoNodo;
		nodoTemporal->ip = strdup(ipNodo);
		nodoTemporal->puerto_escucha_nodo = puertoEscuchaNodo;
		list_add(listaNodos, nodoTemporal);
		i++;
	}


	//VOY A LISTAR LA LISTA DE NODOS PARA VER SI LLEGO BIEN

	printf ("\n\nLista de nodos recibida");
	printf ("\n=======================\n\n");
	int iii, n_nodos;
	t_nodo *elemento=malloc(sizeof(t_nodo));
	n_nodos = list_size(listaNodos);
	for (iii = 0; iii < n_nodos; iii++) {
		elemento = list_get(listaNodos, iii);
		printf("\n\n");
		printf("Nodo_ID: %s\nEstado: %d\nIP: %s\nPuerto de Escucha: %d\n",elemento->nodo_id, elemento->estado, elemento->ip,elemento->puerto_escucha_nodo);
		printf("\n");
	}

//================================== FIN DEL ENVIO DE LA LISTA DE NODOS DEL FS =================================================



//=================================== RECIBO LA LISTA DE ARCHIVOS QUE TIENE EL FS ==============================================

	if ((nbytes = recv(socket_fs, &cantArchivos, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
		perror("recv");
		log_error(logger,"FALLO el Recv de cantidad de archivos");
		exit(-1);
	}
	j=0;
	while (j < cantArchivos){
		//primero los datos de t_archivo, la lista de archivos
		t_archivo* archivoTemporal = malloc(sizeof(t_archivo));
		memset(nombreArchivo,'\0',100);
		if ((nbytes = recv(socket_fs, nombreArchivo, sizeof(nombreArchivo), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del nombre del archivo");
			exit(-1);
		}
		//pathArchivo=string_new(); //no enviamos path por ahora
		if ((nbytes = recv(socket_fs, &padreArchivo, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del padre del archivo");
			exit(-1);
		}
		if ((nbytes = recv(socket_fs, &estadoArchivo, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del estado del archivo");
			exit(-1);
		}
		archivoTemporal->nombre=string_new();
		strcpy(archivoTemporal->nombre, nombreArchivo);
		//strcpy(archivoTemporal->path, pathArchivo);
		archivoTemporal->padre = padreArchivo;
		archivoTemporal->estado =estadoArchivo;

		if ((nbytes = recv(socket_fs, &cantidadBloquesArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv de cantidad de bloques del archivo");
			exit(-1);
		}
		archivoTemporal->bloques = list_create();
		k=0;
		while (k < cantidadBloquesArchivo){
			t_bloque* bloqueArchivoTemporal = malloc(sizeof(t_bloque));
			bloqueArchivoTemporal->copias = list_create();
			if ((nbytes = recv(socket_fs, &cantidadCopiasArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
				perror("recv");
				log_error(logger,"FALLO el Recv de cantidad de copias del bloque del archivo");
				exit(-1);
			}
			l=0;
			while (l < cantidadCopiasArchivo){
				t_copias* copiaBloqueTemporal = malloc(sizeof(t_copias));
				memset(nodoIdArchivo,'\0',6);
				if ((nbytes = recv(socket_fs, nodoIdArchivo, sizeof(nodoIdArchivo), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
					perror("recv");
					log_error(logger,"FALLO el Recv del nodo de la copia del archivo");
					exit(-1);
				}
				if ((nbytes = recv(socket_fs, &bloqueNodoArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
					perror("recv");
					log_error(logger,"FALLO el Recv del bloque del nodo donde está el archivo");
					exit(-1);
				}
				copiaBloqueTemporal->nodo=string_new();
				strcpy(copiaBloqueTemporal->nodo, nodoIdArchivo);
				copiaBloqueTemporal->bloqueNodo =bloqueNodoArchivo;
				list_add(bloqueArchivoTemporal->copias, copiaBloqueTemporal);
				l++;
			}
			list_add(archivoTemporal->bloques,bloqueArchivoTemporal);
			k++;
		}
		list_add(listaArchivos, archivoTemporal);
		j++;
	}

	//VOY A LISTAR LA LISTA DE ARCHIVOS PARA VER SI LLEGO BIEN

	int ii,jj,kk,cant_archivos,cant_bloques,cant_copias;
		t_archivo *archi=malloc(sizeof(t_archivo));
		t_bloque *bloque=malloc(sizeof(t_bloque));
		t_copias *copia=malloc(sizeof(t_copias));
		cant_archivos = list_size(listaArchivos);
		if (cant_archivos==0){
			printf ("No hay archivos cargados en MDFS\n");
		}
		for (ii = 0; ii < cant_archivos; ii++) {
			archi = list_get(listaArchivos, ii);
			printf("\n\n");
			printf("Archivo: %s\nPadre: %d\nEstado: %d\n",archi->nombre,archi->padre,archi->estado);
			printf("\n");
			cant_bloques=list_size(archi->bloques);
			for (jj = 0; jj < cant_bloques; jj++){
				bloque=list_get(archi->bloques,jj);
				printf ("Numero de bloque: %d\n",jj);
				cant_copias=list_size(bloque->copias);
				for (kk=0;kk<cant_copias;kk++){
					copia=list_get(bloque->copias,kk);
					printf ("Copia %d del bloque %d\n",kk,jj);
					printf ("----------------------\n");
					printf ("	Nodo: %s\n	Bloque: %d\n\n",copia->nodo,copia->bloqueNodo);
				}
			}
		}


//================================= FIN DEL ENVIO DE LA LISTA DE ARCHIVOS DEL FS ================================================


	if( pthread_create( &escucha_jobs , NULL , connection_handler_jobs , NULL) < 0){
	    perror("could not create thread");
	    return -1;
	}

	pthread_join(escucha_jobs,NULL);
	return 0;

}//================================== FIN DEL MAIN =====================================================


void *connection_handler_jobs(){
	pthread_t hilojob;
	int newfd,addrlen,i,yes=1;
	int listener, nbytes;
	int *socketJob;
	char handshake[BUF_SIZE];
	if ((listener = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("socket");
		log_info(logger,"FALLO la creacion del socket");
		exit(-1);
	}
	if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes,sizeof(int)) == -1) {
		perror("setsockopt");
		log_info(logger,"FALLO la ejecucion del setsockopt");
		exit(-1);
	}
	remote_job.sin_family = AF_INET;
	remote_job.sin_addr.s_addr = INADDR_ANY;
	remote_job.sin_port = htons(config_get_int_value(configurador,"PUERTO_LISTEN"));
	memset(&(remote_job.sin_zero), '\0', 8);
	if (bind(listener, (struct sockaddr *)&remote_job, sizeof(remote_job)) == -1) {
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
	FD_SET(listener, &master);
	if (listener > fdmax) {
		fdmax = listener;
	}
	while(1){
		read_fds = master;
		if (select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1)
		{
			perror("select:");
			exit(1);
		}
		memset(handshake, '\0', BUF_SIZE);
		socketJob = malloc(sizeof(int));
		for(i=0;i<=fdmax;i++){
			if (FD_ISSET(i, &read_fds)) { // ¡¡tenemos datos!!
				if (i == listener) {
					// gestionar nuevas conexiones, primero hay que aceptarlas
					addrlen = sizeof(struct sockaddr_in);
					if ((newfd = accept(listener, (struct sockaddr*)&remote_job,(socklen_t*)&addrlen)) == -1) {
						perror("accept");
						log_info(logger,"FALLO el ACCEPT");
						//exit(-1);
					} else { //llego una nueva conexion, se acepto y ahora tengo que tratarla
						if ((nbytes = recv(newfd, handshake, sizeof(handshake), MSG_WAITALL)) <= 0) { //si entra aca es porque hubo un error, no considero desconexion porque es nuevo
							perror("recv");
							log_info(logger,"FALLO el Recv");
							//exit(-1);
						} else {
							if (nbytes>0 && strncmp(handshake,"soy job",7)==0){
								*socketJob = newfd;
								log_info(logger,"Se conectó el Job con IP:%s",inet_ntoa(remote_job.sin_addr));
								//Por cada Job que se conecta tiramos un hilo para atenderlo
								if(pthread_create(&hilojob, NULL, (void*)atenderJob, socketJob) != 0) {
									perror("pthread_create");
									log_error(logger,"Fallo la creacion del hilo Job");
								}
							}

						}
					}
					//.................................................
				//hasta aca, es el tratamiento de conexiones nuevas
				//.................................................
				} else {
					// gestionar datos  del fs
					if ((nbytes = recv(i, mensaje, sizeof(mensaje), MSG_WAITALL)) <= 0) { //si entra aca es porque se desconecto o hubo un error
						if (nbytes == 0) {
							//  fs se desconecto, lo identifico
							if (i==socket_fs){ //se desconecto el FS
								close(i); // ¡Hasta luego!
								FD_CLR(i, &master); // eliminar del conjunto maestro
								log_info(logger,"Se desconectó el FileSystem.");
								exit(1);
							}
						} else {
							perror("recv");
							log_info(logger,"FALLO el Recv");
							exit(-1);
							}
					}else {
						// tenemos datos del fs
						// ...... Tratamiento del mensaje nuevo
					}
				}

			}
		}
	}
}

void *atenderJob (int *socketJob) {
	int cantBloques;
	int i;
	int j;
	int k;
	t_nodo *nodo;
	int cantCopias;
	char accion[BUF_SIZE];
	char mensaje_fs[BUF_SIZE];
	t_copias *copia;
	t_bloque *bloque;
	t_list *copiasNodo;
	t_nodo *nodoAux;
	t_list* listaMappers;
	int numeroResultado;
	memset(mensaje_fs,'\0',BUF_SIZE);
	memset(accion,'\0',BUF_SIZE);
	copiasNodo=list_create();
	listaMappers = list_create();
	char archivoResultado[TAM_NOMFINAL];
	pthread_detach(pthread_self());
	int posicionArchivo;
	char mensajeCombiner[3];
	int padre;
	char archivosDelJob[MENSAJE_SIZE];
	memset(mensajeCombiner, '\0', 3);
	memset(archivosDelJob, '\0', MENSAJE_SIZE);
	memset(archivoResultado,'\0', TAM_NOMFINAL);
	char ** arrayTiempo;
	char *tiempo=string_new(); //string que tendrá la hora
	char *nombreArchivoTemp=string_new();//Nombre del archivo que va a mandar a los nodos
	char *pathArchivoTemp = string_new(); //Path donde va a guardar los archivos temporales
	//Recibe mensaje de si es o no combiner
	if(recv(*socketJob,mensajeCombiner,sizeof(mensajeCombiner),MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo al recibir el atributo COMBINER");
		//exit(-1);
	}
	//Para probar que recibio el atributo
	printf("El Job %s acepta combiner\n",(char*)mensajeCombiner);

	//Recibe el archivo resultado del Job
	if(recv(*socketJob,archivoResultado,sizeof(archivoResultado),MSG_WAITALL)==-1){
	perror("recv");
	log_error(logger,"Fallo al recibir el archivo resultado");
	//exit(-1);
	}

	//Para probar que recibio el archivo resultado final
	printf("nombre del archivo resultado final %s \n",(char*)archivoResultado);

	if((recv(*socketJob, archivosDelJob, sizeof(archivosDelJob), MSG_WAITALL)) <= 0) {
		perror("recv");
		log_info(logger,"FALLO el Recv");
		//exit(-1);
	}

	// Separo el mensaje que recibo con los archivos a trabajar (Job envía todos juntos separados con ,)
	char** archivos =string_split((char*)archivosDelJob,",");
	t_list *bloques;
	char** arrayArchivo;
	//Lo siguiente es para probar que efectivamente se reciba la lista de archivos
	char nombreArchivo[TAM_NOMFINAL];
	memset(nombreArchivo,'\0',TAM_NOMFINAL);
	for(posicionArchivo=0;archivos[posicionArchivo]!=NULL;posicionArchivo++){
		printf("Se debe trabajar en el archivo:%s\n",archivos[posicionArchivo]);
		//Separo el nombre del archivo por barras
	    arrayArchivo = string_split(archivos[posicionArchivo], "/");
		for(k=0;arrayArchivo[k]!=NULL;k++){
			if(arrayArchivo[k+1]==NULL){
				//me quedo con el nombre del archivo
				strcpy(nombreArchivo,arrayArchivo[k]);
			}
		}
	strcpy(mensaje_fs, "dame padre");
	 if(send(socket_fs,mensaje_fs, sizeof(mensaje_fs), MSG_WAITALL )==-1){
	 perror("send");
	 log_error(logger,"Fallo el dame padre");
	 //exit(-1);
	 }
	 //Le mando el path a FS para que me mande el padre
	 if(send(socket_fs,archivos[posicionArchivo],sizeof(TAM_NOMFINAL),MSG_WAITALL) == -1) {
	 perror("send");
	 log_error(logger,"Fallo el dame padre");
	 //exit(-1);
	 }
	 //Recibe del FS el padre
	 if(recv(socket_fs,&padre,sizeof(int),MSG_WAITALL)== -1){
	 perror("recv");
	 log_error(logger,"Fallo el recv del padre del FS");
	 //exit(-1);
	 }


	//De cada archivo que nos manda el Job buscamos y nos traemos los bloques
	 bloques=buscarBloques(nombreArchivo,padre);
	//Enviamos rutina Map de cada bloque del archivo al Job que nos envio dicho archivo
		cantBloques = list_size(bloques);
		for(i=0; i<cantBloques; i++){ //recorremos los bloques del archivo que nos mando job
			bloque = list_get(bloques,i);
			cantCopias = list_size(bloque->copias);
			for(j=0;j<cantCopias;j++){ // Por cada bloque del archivo recorremos las copias de dicho archivo
				copia = list_get(bloque->copias,j);
				// Nos traemos cada nodo en donde esta cada una de las copias del archivo
				nodo= buscarCopiaEnNodos(copia);
				if(nodo->estado == 1){
					// Creamos una sublista de la lista global de nodos con los nodos en los que esta cada copia del archivo
					list_add(copiasNodo,nodo);
				}
			}
			// Ordenamos la sublista segun la suma de la cantidad de map y reduce
			list_sort(copiasNodo, (void*) ordenarSegunMapYReduce);
			nodoAux = list_get(copiasNodo,0); // Nos traemos el nodo con menos carga
			//Del nodo que nos trajimos agarramos los datos que necesitamos para mandarle al job
			t_replanificarMap *mapper;
			mapper=malloc(sizeof(t_replanificarMap));
			t_mapper datosMapper;
			memset(datosMapper.ip_nodo,'\0',20);
			memset(datosMapper.archivoResultadoMap,'\0',TAM_NOMFINAL);
			memset(mapper->archivoResultadoMap,'\0',TAM_NOMFINAL);
			memset(mapper->nombreArchivoDelJob,'\0',TAM_NOMFINAL);
			strcpy(datosMapper.ip_nodo,nodoAux->ip);
			datosMapper.puerto_nodo= nodoAux->puerto_escucha_nodo;
			for(j=0;j<cantCopias;j++){
				copia = list_get(bloque->copias,j);
				if(strcmp(copia->nodo,nodoAux->nodo_id)==0){
					datosMapper.bloque=copia->bloqueNodo;
				}
			}
			strcpy(pathArchivoTemp,"/tmp/");
			strcpy(nombreArchivoTemp,"MapTemporal");
			arrayTiempo=string_split(temporal_get_string_time(),":"); //creo array con hora minutos segundos y milisegundos separados
			string_append(&tiempo,arrayTiempo[0]);//Agrego horas
			string_append(&tiempo,arrayTiempo[1]);//Agrego minutos
			string_append(&tiempo,arrayTiempo[2]);//Agrego segundos
			string_append(&tiempo,arrayTiempo[3]);//Agrego milisegundos
			string_append(&nombreArchivoTemp,tiempo); //Concateno la fecha en formato hhmmssmmmm
			string_append(&nombreArchivoTemp,".tmp");
			string_append(&pathArchivoTemp,nombreArchivoTemp);
			strcpy(datosMapper.archivoResultadoMap,pathArchivoTemp); //Falta generar un nombre

			strcpy(accion,"ejecuta map");
			//Le avisamos al job que vamos a mandarle rutina map
			if(send(*socketJob,accion,sizeof(accion),MSG_WAITALL)==-1){
				perror("send");
				log_error(logger,"Fallo el envio de los datos para el mapper");
				exit(-1);
			}
			// Le mandamos los datos que necesita el job para aplicar map
			if(send(*socketJob,&datosMapper,sizeof(t_mapper),MSG_WAITALL)==-1){
				perror("send");
				log_error(logger,"Fallo el envio de los datos para el mapper");
				exit(-1);
			}


			//Rellenar la estructura t_replanificarMap con el nodo_id del nodo que acabo de mandar a hacer el map y los demas campos
			// y agregarlos a una lista de mappers que va a manejar marta q va a estar compuesta por las estructuras t_replanificarMap por cada map
			//de bloque que haga

			mapper->bloqueArchivo = i; //i es el bloque de archivo
			mapper->lista_nodos=list_create();
			strcpy(mapper->nombreArchivoDelJob,archivos[posicionArchivo]);
			strcpy(mapper->archivoResultadoMap,datosMapper.archivoResultadoMap);
			char* nodoIdTemp=string_new();
			string_append(&nodoIdTemp,nodoAux->nodo_id);
			list_add(mapper->lista_nodos,nodoIdTemp);
			list_add(listaMappers,mapper);

			//******************************************************************************
			//Buscar nodoAux en la lista general comparando por nodo_id y sumarle cantMapper
			//*******************************************************************************
			sumarCantMapper(nodoAux);
			list_clean_and_destroy_elements(copiasNodo, (void*) eliminarCopiasNodo);
		}
	}

	for(numeroResultado = 0; numeroResultado < list_size(listaMappers);numeroResultado ++) {
		t_respuestaMap respuestaMap;
		//Recibo respuesta del Job de cada map
		if(recv(*socketJob,&respuestaMap, sizeof(t_respuestaMap),MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo al recibir el ok del job");
		//exit(-1);
		}
		if(respuestaMap.resultado ==1){
			printf("El map falló\n");
			//PAM
			//REPLANIFICAR BLOQUE (buscar t_respuestasMap.archivoResultadoMap con t_replanificarMap el archivo y bloque del archivo
			// que no se pudo hacer el map recorrer sus copias y ordenar la sublista devuelta y mandar el map a el primer nodo de la sublista :)
			//Agregar dicho nodo_id a la lista dentro de la estructura de ese map que se va a mandar
			// y buscarlo en la lista gral de nodos restarle map al que me dio KO = 1 LE RESTO UN cantMap-- y le sumo al nuevo nodo que mando el map
		}else {
			printf("El map salio ok\n");
		//Buscar el respuestaMap.nombreArchvioTemporal, con t_replanificarMap el archivo y bloque del archivo
		// buscar en la lista del struct el nodo_id y luego buscarlo en la lista gral de nodos y restarle 1 a su catMappers
		}

	}


	pthread_exit((void*)0);

}

//Busca y trae todos los bloques de un archivo
t_list* buscarBloques (char *nombreArchivo, uint32_t padre){
	t_archivo *archivoAux;
	t_list *bloques;
	int i;
	for(i=0; i < list_size(listaArchivos); i++){ //recorre la lista global de archivos
		archivoAux = list_get(listaArchivos,i);
		if (strcmp(archivoAux->nombre,nombreArchivo) ==0 && archivoAux->padre==padre){ //compara el nomnre y padre del archivo del job con cada nombre y padre de archivo de la lista global
			bloques = archivoAux->bloques;

		}
	}
	return bloques;
}


static void eliminarCopiasNodo(t_list *self){
	free(self);
}

//Buscamos los nodos de la lista global en los que esta cada copia
t_nodo* buscarCopiaEnNodos(t_copias *copia){
	int i;
	int cantNodos;
	t_nodo *nodo;
	t_nodo *nodoAux;
	cantNodos = list_size(listaNodos);
	for(i=0; i<cantNodos; i++){ //Recorremos la lista de nodos global
		nodo = list_get(listaNodos,i);
		if(strcmp(nodo->nodo_id, copia->nodo)==0){ //Comparamos el nodo de la copia con cada nodo la lista global
			nodoAux = nodo;
			break;
		}
	}
	return nodoAux;
}

bool ordenarSegunMapYReduce(t_nodo *menosCarga, t_nodo* mayorCarga){
	int resultado1;
	int resultado2;
	resultado1 = menosCarga->cantMappers + (menosCarga->cantReducers * 5);
	resultado2 = mayorCarga->cantMappers + (mayorCarga->cantReducers * 5);
	return resultado1<resultado2;
}

void sumarCantMapper(t_nodo* nodoParaSumar){
	int tamanio=list_size(listaNodos);
	int indice=0;
	t_nodo* unNodo;
	for(indice=0;indice<tamanio;indice++){
		unNodo=list_get(listaNodos,indice);
		if(strcmp(unNodo->nodo_id,nodoParaSumar->nodo_id)==0){
			unNodo->cantMappers++;
		}
	}
}
