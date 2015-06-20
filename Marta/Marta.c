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
	char ipNodo[15];
	int puertoEscuchaNodo;
	//para recibir la informacion de los archivos
	int j, k, l;
	int cantArchivos;
	char* pathArchivo;
	char* nombreArchivo;
	uint32_t estadoArchivo;
	char* nodoArchivo;
	char nodoIdArchivo[6];
	int bloqueNodoArchivo;
	int cantidadBloquesArchivo;
	int cantidadCopiasArchivo;
	listaNodos = list_create(); //creo la lista para los nodos que me pasa el FS
	listaArchivos = list_create(); //creo la lista para los archivos que me pasa el FS
	jobs=list_create(); //creo la lista de jobs


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


//Para recibir los nodos de FS
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
		memset(ipNodo, '\0',15);
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
		memset(nodoTemporal->nodo_id, '\0', 6);
		strcpy(nodoTemporal->nodo_id, nodoId);
		nodoTemporal->estado =estadoNodo;
		nodoTemporal->ip = strdup(ipNodo);
		nodoTemporal->puerto_escucha_nodo = puertoEscuchaNodo;
		list_add(listaNodos, nodoTemporal);
		i++;
	}

	//para recibir los archivos de FS

/*
	if ((nbytes = recv(socket_fs, &cantArchivos, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
		perror("recv");
		log_error(logger,"FALLO el Recv de cantidad de archivos");
		exit(-1);
	}
	j=0;
	while (j < cantArchivos){
		//primero los datos de t_archivo, la lista de archivos
		t_archivo* archivoTemporal = malloc(sizeof(t_archivo));
		pathArchivo=string_new();
		if ((nbytes = recv(socket_fs, pathArchivo, sizeof(pathArchivo), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del path de archivo");
			exit(-1);
		}
		nombreArchivo=string_new();
		if ((nbytes = recv(socket_fs, nombreArchivo, sizeof(nombreArchivo), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del nombre del archivo");
			exit(-1);
		}
		if ((nbytes = recv(socket_fs, estadoArchivo, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del estado del archivo");
			exit(-1);
		}
		strcpy(archivoTemporal->nombre, nombreArchivo);
		strcpy(archivoTemporal->path, pathArchivo);
		archivoTemporal->estado =estadoArchivo;

		if ((nbytes = recv(socket_fs, cantidadBloquesArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv de cantidad de bloques del archivo");
			exit(-1);
		}
		while (k < cantidadBloquesArchivo){
			t_bloque* bloqueArchivoTemporal = malloc(sizeof(t_bloque));
			bloqueArchivoTemporal->copias = list_create();
			if ((nbytes = recv(socket_fs, cantidadCopiasArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
				perror("recv");
				log_error(logger,"FALLO el Recv de cantidad de copias del bloque del archivo");
				exit(-1);
			}
			while (l < cantidadCopiasArchivo){
				t_copias* copiaBloqueTemporal = malloc(sizeof(t_copias));
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
*/

	if( pthread_create( &escucha_jobs , NULL , connection_handler_jobs , NULL) < 0){
	    perror("could not create thread");
	    return -1;
	}

	pthread_join(escucha_jobs,NULL);
	return 0;
}


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
								// Se conecta un nuevo job, lo guardamos en el set master y actualizamos fdmax

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
	pthread_detach(pthread_self());
	int posicionArchivo;
	char mensajeCombiner[3];
	char message[MENSAJE_SIZE];
	memset(mensajeCombiner, '\0', 3);
	memset(message, '\0', MENSAJE_SIZE);
	if((recv(*socketJob, message, sizeof(message), MSG_WAITALL)) <= 0) {
		perror("recv");
		log_info(logger,"FALLO el Recv");
		//exit(-1);
	}
	// Separo el mensaje que recibo con los archivos a trabajar (Job envía todos juntos separados con ,)
	char** archivos =string_split((char*)message,",");

	//Lo siguiente es para probar que efectivamente se reciba la lista de archivos

	for(posicionArchivo=0;archivos[posicionArchivo]!=NULL;posicionArchivo++){
		printf("Se debe trabajar en el archivo:%s\n",archivos[posicionArchivo]);
	}
	//fin de la prueba

	if(recv(*socketJob,mensajeCombiner,sizeof(mensajeCombiner),MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo al recibir el atributo COMBINER");
		//exit(-1);
	}
	//Para probar que recibio el atributo
	printf("El Job %s acepta combiner\n",(char*)mensajeCombiner);

	/* PLANIFICACION
	 * MaRTA le va a pedir al FS los bloques de los archivos involucrados
	 * FS le deverá devolver: nodo(ip,puerto)-bloque
	 * Buscará la combinación que maximice la distribución de las tareas en los nodos e
	 * irá indicando al Job cada Hilo Mapper que deberá iniciar y qué NodoBloque debe
	 * procesar hasta que la rutina de Mapping haya sido aplicada en tod0 el set de datos.
	 */

	t_mapper datosMapper;
	strcpy(datosMapper.ip_nodo,"127.0.0.1");
	datosMapper.puerto_nodo=6500;
	datosMapper.bloque=1;
	strcpy(datosMapper.nombreArchivoTemporal,"/tmp/mapBloque1.txt");

	if(send(*socketJob,&datosMapper,sizeof(t_mapper),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envio de los datos para el mapper");
		exit(-1);
	}

	pthread_exit((void*)0);

}
