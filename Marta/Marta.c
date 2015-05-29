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

#define BUF_SIZE 50
#define MENSAJE_SIZE 4096



//Prototipos de funciones
void *connection_handler_jobs(); // Esta funcion escucha continuamente si recibo nuevos mensajes


//Variables Globales
fd_set master; // conjunto maestro de descriptores de fichero
fd_set read_fds; // conjunto temporal de descriptores de fichero para select()
t_log* logger;
t_config * configurador;
int fdmax; // número máximo de descriptores de fichero
int socket_fs;
struct sockaddr_in filesystem; // dirección del servidor
struct sockaddr_in remote_job; // dirección del cliente
char identificacion[BUF_SIZE]; // buffer para datos del cliente
char mensaje[MENSAJE_SIZE];
char mensajeCombiner[3]; //Dice si el Job acepta combiner (SI/NO)
int read_size;
t_list* jobs;

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
	if((send(socket_fs,identificacion,sizeof(identificacion),0))==-1) {
		perror("send");
		log_error(logger,"FALLO el envio del saludo al FS");
	exit(-1);
	}
	int nbytes;
	if ((nbytes = recv(socket_fs, identificacion, sizeof(identificacion), 0)) < 0) { //si entra aca es porque hubo un error, no considero desconexion porque es nuevo
		perror("recv");
		log_error(logger,"FALLO el Recv");
		exit(-1);
	} else if (nbytes == 0){
		printf ("Conexion con FS cerrada, el proceso fs no esta listo o bien ya existe una instancia de marta conectada\n");
		exit(-1);
	}
	if (nbytes > 0 && strncmp(identificacion,"ok",2)==0)	printf ("Conexion con el FS exitosa\n");

	jobs=list_create(); //creo la lista de jobs

	if( pthread_create( &escucha_jobs , NULL , connection_handler_jobs , NULL) < 0){
	    perror("could not create thread");
	    return -1;
	}

	pthread_join(escucha_jobs,NULL);
	return 0;
}

void *connection_handler_jobs(){
	int newfd,addrlen,i,yes=1;
	int listener, nbytes;
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
		for(i=0;i<=fdmax;i++){
			if (FD_ISSET(i, &read_fds)) { // ¡¡tenemos datos!!
				if (i == listener) {
					// gestionar nuevas conexiones, primero hay que aceptarlas
					addrlen = sizeof(struct sockaddr_in);
					if ((newfd = accept(listener, (struct sockaddr*)&remote_job,(socklen_t*)&addrlen)) == -1) {
						perror("accept");
						log_info(logger,"FALLO el ACCEPT");
						exit(-1);
					} else { //llego una nueva conexion, se acepto y ahora tengo que tratarla
						if ((nbytes = recv(newfd, mensaje, sizeof(mensaje), 0)) <= 0) { //si entra aca es porque hubo un error, no considero desconexion porque es nuevo
							perror("recv");
							log_info(logger,"FALLO el Recv");
							exit(-1);
						} else {
							// Se conecta un nuevo job, lo guardamos en el set master y actualizamos fdmax
							log_info(logger,"Se conectó el Job con IP:%s",inet_ntoa(remote_job.sin_addr));
							FD_SET(newfd,&master);
							if(newfd>fdmax){
								fdmax=newfd;
							}

							// Separo el mensaje que recibo con los archivos a trabajar (Job envía todos juntos separados con ,)
							char** archivos =string_split((char*)mensaje,",");

							//Lo siguiente es para probar que efectivamente se reciba la lista de archivos
							int i;
							for(i=0;archivos[i]!=NULL;i++){
								printf("Se debe trabajar en el archivo:%s\n",archivos[i]);
							}
							//fin de la prueba

							if(recv(newfd,mensajeCombiner,sizeof(mensajeCombiner),0)==-1){
								perror("recv");
								log_error(logger,"Fallo al recibir el atributo COMBINER");
								exit(-1);
							}
							//Para probar que recibio el atributo
							printf("El Job %s acepta combiner\n",(char*)mensajeCombiner);

							/*
							 * MaRTA le va a pedir al FS los bloques de los archivos involucrados
							 * FS le deverá devolver: nodo(ip,puerto)-bloque
							 * Buscará la combinación que maximice la distribución de las tareas en los nodos e
							 * irá indicando al Job cada Hilo Mapper que deberá iniciar y qué NodoBloque debe
							 * procesar hasta que la rutina de Mapping haya sido aplicada en todo el set de datos.
							 */

							t_mapper datosMapper;
							strcpy(datosMapper.ip_nodo,"127.0.0.1");
							datosMapper.puerto_nodo=6500;
							datosMapper.bloque=3;
							strcpy(datosMapper.nombreArchivoTemporal,"/tmp/map3tmp.txt");

							if(send(newfd,&datosMapper,sizeof(t_mapper),0)==-1){
								perror("send");
								log_error(logger,"Fallo el envio de los datos para el mapper");
								exit(-1);
							}

						}
					}
					//.................................................
				//hasta aca, es el tratamiento de conexiones nuevas
				//.................................................
				} else {
					// gestionar datos de un job o del fs
					if ((nbytes = recv(i, mensaje, sizeof(mensaje), 0)) <= 0) { //si entra aca es porque se desconecto o hubo un error
						if (nbytes == 0) {
							// Un job o el fs se desconecto, lo identifico
							if (i==socket_fs){ //se desconecto el FS
								//haremos algo aca
								close(i); // ¡Hasta luego!
								FD_CLR(i, &master); // eliminar del conjunto maestro
							} else { //se desconecto un job
								//haremos algo aca tambien
								close(i); // ¡Hasta luego!
								FD_CLR(i, &master); // eliminar del conjunto maestro
							}
						} else {
							perror("recv");
							log_info(logger,"FALLO el Recv");
							exit(-1);
							}
					} else {
						// tenemos datos de algún job o del fs
						// ...... Tratamiento del mensaje nuevo
					}
				}

			}
		}
	}
}
