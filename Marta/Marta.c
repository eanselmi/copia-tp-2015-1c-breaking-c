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
t_list* listaJobs;
t_list* listaNodos; //lista de nodos conectados al FS
t_list* listaArchivos; //lista de archivos del FS
char identificacion[BUF_SIZE]; //para el mensaje que envie al conectarse para identificarse, puede cambiar
int nroJob; //Variable global utilizada por cada hilo Job para diferenciar el nombre de un resultado Map/Reduce
pthread_mutex_t mutexNroJob=PTHREAD_MUTEX_INITIALIZER; //Para que cada Job tenga su numero diferente utilizaran este mutex
pthread_mutex_t mutexModNodo=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexJobs=PTHREAD_MUTEX_INITIALIZER;

int main(int argc, char**argv){

	pthread_t escucha_jobs;
	configurador= config_create("resources/martaConfig.conf");
	logger = log_create("./martaLog.log", "Marta", true, LOG_LEVEL_INFO);
	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
//	printf("%s\n", config_get_string_value(configurador,"IP_FS"));
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
	char nombreArchivo[200];
	uint32_t padreArchivo;
	char nodoIdArchivo[6];
	int bloqueNodoArchivo;
	int cantidadBloquesArchivo;
	int cantidadCopiasArchivo;
	listaNodos = list_create(); //creo la lista para los nodos que me pasa el FS
	listaArchivos = list_create(); //creo la lista para los archivos que me pasa el FS
	listaJobs=list_create(); //creo la lista de jobs


//====================== CONEXION CON FS =================================

	nroJob=0; //inicializo la variable global de numero de Job actual a 0

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
		nodoTemporal->cantMappers=0;
		nodoTemporal->cantReducers=0;
		list_add(listaNodos, nodoTemporal);
		i++;
	}


	//VOY A LISTAR LA LISTA DE NODOS PARA VER SI LLEGO BIEN

//	printf ("\n\nLista de nodos recibida");
//	printf ("\n=======================\n\n");
//	int iii, n_nodos;
//	t_nodo *elemento=malloc(sizeof(t_nodo));
//	n_nodos = list_size(listaNodos);
//	for (iii = 0; iii < n_nodos; iii++) {
//		elemento = list_get(listaNodos, iii);
//		printf("\n\n");
//		printf("Nodo_ID: %s\nEstado: %d\nIP: %s\nPuerto de Escucha: %d\n",elemento->nodo_id, elemento->estado, elemento->ip,elemento->puerto_escucha_nodo);
//		printf("Cantidad de mappers:%d\n",elemento->cantMappers);
//		printf("Cantidad de reducers:%d\n",elemento->cantReducers);
//		printf("\n");
//	}

	estadoMarta();
//================================== FIN DEL ENVIO DE LA LISTA DE NODOS DEL FS =================================================



//=================================== RECIBO LA LISTA DE ARCHIVOS QUE TIENE EL FS ==============================================

	if ((nbytes = recv(socket_fs, &cantArchivos, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
		perror("recv");
		log_error(logger,"FALLO el Recv de cantidad de archivos");
		exit(-1);
	}
	//printf ("Cantidad de archivos a recibir: %d\n",cantArchivos);
	j=0;
	while (j < cantArchivos){
		//primero los datos de t_archivo, la lista de archivos
		t_archivo* archivoTemporal = malloc(sizeof(t_archivo));
		memset(nombreArchivo,'\0',200);
		if ((nbytes = recv(socket_fs, nombreArchivo, sizeof(nombreArchivo), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del nombre del archivo");
			exit(-1);
		}
		//printf ("...Nombre Archivo: %s\n",nombreArchivo);
		//pathArchivo=string_new(); //no enviamos path por ahora
		if ((nbytes = recv(socket_fs, &padreArchivo, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del padre del archivo");
			exit(-1);
		}
		//printf ("...Padre del archivo: %d\n",padreArchivo);
		strcpy(archivoTemporal->nombre, nombreArchivo);
		//strcpy(archivoTemporal->path, pathArchivo);
		archivoTemporal->padre = padreArchivo;

		if ((nbytes = recv(socket_fs, &cantidadBloquesArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv de cantidad de bloques del archivo");
			exit(-1);
		}
		//printf ("...Cantidad de bloques del archivo: %d\n",cantidadBloquesArchivo);
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
		//	printf ("... ...Cantidad de copias del bloque:%d\n",cantidadCopiasArchivo);
			l=0;
			while (l < cantidadCopiasArchivo){
				t_copias* copiaBloqueTemporal = malloc(sizeof(t_copias));
				memset(nodoIdArchivo,'\0',6);
				if ((nbytes = recv(socket_fs, nodoIdArchivo, sizeof(nodoIdArchivo), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
					perror("recv");
					log_error(logger,"FALLO el Recv del nodo de la copia del archivo");
					exit(-1);
				}
			//	printf ("... ... ...Nodo ID: %s\n",nodoIdArchivo);
				if ((nbytes = recv(socket_fs, &bloqueNodoArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
					perror("recv");
					log_error(logger,"FALLO el Recv del bloque del nodo donde está el archivo");
					exit(-1);
				}
			//	printf ("... ... ...Bloque Nodo: %d\n",bloqueNodoArchivo);
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
//
//	int ii,jj,kk,cant_archivos,cant_bloques,cant_copias;
//		t_archivo *archi;
//		t_bloque *bloque;
//		t_copias *copia;
//		cant_archivos = list_size(listaArchivos);
//		if (cant_archivos==0){
//			printf ("No hay archivos cargados en MDFS\n");
//		}
//		for (ii = 0; ii < cant_archivos; ii++) {
//			archi = list_get(listaArchivos, ii);
//			printf("\n\n");
//			printf("Archivo: %s\nPadre: %d\n",archi->nombre,archi->padre);
//			printf("\n");
//			cant_bloques=list_size(archi->bloques);
//			for (jj = 0; jj < cant_bloques; jj++){
//				bloque=list_get(archi->bloques,jj);
//				printf ("Numero de bloque: %d\n",jj);
//				cant_copias=list_size(bloque->copias);
//				for (kk=0;kk<cant_copias;kk++){
//					copia=list_get(bloque->copias,kk);
//					printf ("Copia %d del bloque %d\n",kk,jj);
//					printf ("----------------------\n");
//					printf ("	Nodo: %s\n	Bloque: %d\n\n",copia->nodo,copia->bloqueNodo);
//				}
//			}
//		}


//================================= FIN DEL ENVIO DE LA LISTA DE ARCHIVOS DEL FS ================================================



		//TEST PARA NOTIFICAR AL FS QUE BUSQUE UN ARCHIVO RESULTADO DE UN REDUCE
/*
		memset(identificacion,'\0',BUF_SIZE);
		strcpy(identificacion,"resultado");
		if((send(socket_fs,identificacion,sizeof(identificacion),MSG_WAITALL))==-1) {
			perror("send");
			log_error(logger,"FALLO el envio del saludo al FS");
			exit(-1);
		}
		char test[6];
		memset(test, '\0', 6);
		strcpy(test,"nodo2");
		if((send(socket_fs,test,sizeof(test),MSG_WAITALL))==-1) {
			perror("send");
			log_error(logger,"FALLO el envio del saludo al FS");
			exit(-1);
		}
		char test2[60];
		memset(test2, '\0', 60);
		strcpy(test2,"alton.txt");
		if((send(socket_fs,test2,sizeof(test2),MSG_WAITALL))==-1) {
			perror("send");
			log_error(logger,"FALLO el envio del saludo al FS");
			exit(-1);
		}
		char path_mdfs[200];
		memset(path_mdfs,'\0',200);
		strcpy(path_mdfs,"/resultado/alton");
		printf ("%s\n",path_mdfs);
		if((send(socket_fs,path_mdfs,sizeof(path_mdfs),MSG_WAITALL))==-1) {
			perror("send");
			log_error(logger,"FALLO el envio del saludo al FS");
			exit(-1);
		}
*/





	if( pthread_create( &escucha_jobs , NULL , connection_handler_jobs , NULL) < 0){
	    perror("could not create thread");
	    return -1;
	}

	pthread_join(escucha_jobs,NULL);
	return 0;

}//================================== FIN DEL MAIN =====================================================

//Buscar la posición del archivo por nombre y padre
int BuscarArchivoPos(char* nombreArch, uint32_t idPadre){
	t_archivo* archAux;
	int posicionArchivo=-1;
	int p;
	int tam = list_size(listaArchivos);
	if(tam!=0){
		for (p = 0; p < tam; p++) {
			archAux = list_get(listaArchivos, p);
			if ((strcmp(archAux->nombre, nombreArch) == 0) && (archAux->padre == idPadre)) {
				posicionArchivo = p;
				return posicionArchivo;
			}
		}
	}
	return posicionArchivo;
}

static void eliminarListaCopias (t_copias* self){
	free(self->nodo);
	free(self);
}

static void eliminarListaBloques(t_bloque* self){
	list_destroy(self->copias);
	free(self);
}

static void eliminarListaArchivos (t_archivo* self){
	list_destroy(self->bloques);
	free(self);
}

static void eliminarListaBloques2(t_bloque *self){
	list_destroy_and_destroy_elements(self->copias, (void*) eliminarListaCopias);
	free(self);
}

static void eliminarListaArchivos2 (t_archivo *self){
	list_destroy_and_destroy_elements(self->bloques, (void*) eliminarListaBloques2);
	free(self);
}



void *connection_handler_jobs(){
	pthread_t hilojob;
	int newfd,addrlen,i,yes=1;
	int listener, nbytes;
	int *socketJob;
	char handshake[BUF_SIZE];
	char nombreArchivoNovedad[200];
	char nuevoNombreArchivoNovedad[200];
	uint32_t nuevoPadreArchivoNovedad;
	uint32_t padreArchivoNovedad;
	char nodoId[6];
	int bloqueNodo;
	int bloqueNodoDestino;
	int cantidadBloquesArchivo=0;
	int cantCopiasArchivoNovedad=0;
	int a, b;
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
					if (i==socket_fs){
						memset(identificacion,'\0',BUF_SIZE);
						if ((nbytes = recv(i, identificacion, sizeof(identificacion), MSG_WAITALL)) <= 0) { //si entra aca es porque se desconecto o hubo un error
							if (nbytes == 0) {
								//  fs se desconecto, lo identifico
								close(i); // ¡Hasta luego!
								FD_CLR(i, &master); // eliminar del conjunto maestro
								log_info(logger,"Se desconectó el FileSystem.");
								exit(1);
							} else {
								perror("recv");
								log_info(logger,"FALLO el Recv");
								exit(-1);
							}
						}
						//UPDATES DE MARTA, novedades que envia el FS
//						printf ("%s\n",identificacion);
						if (strcmp(identificacion,"marta_formatea")==0){
							//printf ("Voy a formatear las estructuras por pedido del FS\n");
//							int cantArchi;
//							cantArchi=list_size(listaArchivos);
							//printf("Cantidad de archivos antes de formatear: %d\n", cantArchi);
							//Se borra la lista de archivos
							list_destroy_and_destroy_elements(listaArchivos, (void*) eliminarListaArchivos2);
							//Se pasan a estado no disponible los nodos que haya conectados
							t_nodo* unNodoF;
							int u;
							for (u=0;u<list_size(listaNodos);u++){
								unNodoF=list_get(listaNodos,u);
								unNodoF->estado=0;
							}
//							int cantArchi2;
//							cantArchi2=list_size(listaArchivos);
					//		printf("Cantidad de archivos en MDFS luego de formatear: %d\n", cantArchi2);
							//listo para ver comprobar que los cambio de estado
//							t_nodo* nodoL;
//							int v, w;
//							v= list_size(listaNodos);
//							//printf("Nodos en la lista: %d \n", v);
//							for (w=0;w<v;w++){
//								nodoL = list_get(listaNodos,w);
//							//	printf ("Nodo_ID:%s Estado: %d\n", nodoL->nodo_id, nodoL->estado );
//							}

						}
						//fin de update FormatearFileSystem
						if (strcmp(identificacion,"elim_arch")==0){
							//printf ("Voy a borrar un archivo de las estructuras\n");
							memset(nombreArchivoNovedad, '\0',200);
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad,	sizeof(nombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo a eliminar");
								exit(-1);
							}
							//printf("...Nombre Archivo a eliminar: %s\n", nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del padre del archivo a eliminar");
								exit(-1);
							}
						//	printf ("...Padre del archivo a eliminar: %d\n",padreArchivoNovedad);
							t_archivo* archivoAux;
							t_bloque* bloqueAux;
							int posArchivoAux;
							int g, h;
							posArchivoAux = BuscarArchivoPos(nombreArchivoNovedad, padreArchivoNovedad);
							archivoAux = list_get(listaArchivos, posArchivoAux);
							for (g=0;g<list_size(archivoAux->bloques);g++){
								bloqueAux=list_get(archivoAux->bloques,g);
								for (h=0;h<list_size(bloqueAux->copias);h++){
									list_remove_and_destroy_element(bloqueAux->copias,h,(void*)eliminarListaCopias);
								}
							}
							for (g=0;g<list_size(archivoAux->bloques);g++){
								list_remove_and_destroy_element(archivoAux->bloques,g,(void*)eliminarListaBloques);
							}
							list_remove_and_destroy_element(listaArchivos,posArchivoAux,(void*)eliminarListaArchivos);
						}
						//fin update de EliminarArchivo
						if (strcmp(identificacion,"renom_arch")==0){
						//	printf ("Voy a renombrar un archivo de las estructuras\n");
							memset(nombreArchivoNovedad, '\0',200);
							memset(nuevoNombreArchivoNovedad, '\0',200);
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad,	sizeof(nombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre viejo del archivo a renombrar");
								exit(-1);
							}
						//	printf("...Nombre Archivo a renombrar: %s\n", nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del padre del archivo a renombrar");
								exit(-1);
							}
						//	printf ("...Padre del archivo a renombrar: %d\n",padreArchivoNovedad);
							if ((nbytes = recv(socket_fs, nuevoNombreArchivoNovedad,	sizeof(nuevoNombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo a renombrar");
								exit(-1);
							}
						//	printf("...Nombre nuevo de archivo a renombrado: %s\n", nuevoNombreArchivoNovedad);
							t_archivo* archivoAux;
							archivoAux = buscarArchivo(nombreArchivoNovedad, padreArchivoNovedad);
							strcpy(archivoAux->nombre, nuevoNombreArchivoNovedad);
//							printf("archivo renombrado a: %s\n", archivoAux->nombre);
//							printf("padre de archivo renombrado: %d\n", archivoAux->padre);
						}
						//fin update de RenombrarArchivo
						if (strcmp(identificacion,"mov_arch")==0){
//							printf ("Voy a mover un archivo de las estructuras\n");
							memset(nombreArchivoNovedad, '\0',200);
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad,	sizeof(nombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo a mover");
								exit(-1);
							}
//							printf("...Nombre Archivo a mover: %s\n", nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del viejo padre del archivo");
								exit(-1);
							}
//							printf("...Viejo padre del archivo: %d\n", padreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &nuevoPadreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nuevo padre del archivo");
								exit(-1);
							}
//							printf("...Nuevo padre del archivo: %d\n", nuevoPadreArchivoNovedad);
							t_archivo* archivoAux;
							archivoAux = buscarArchivo(nombreArchivoNovedad, padreArchivoNovedad);
							archivoAux->padre= nuevoPadreArchivoNovedad;
//							printf("archivo movido a: %s\n", archivoAux->nombre);
//							printf("padre nuevo: %d\n", archivoAux->padre);
						}
						//fin update de mover archivo
						if (strcmp(identificacion,"nuevo_arch")==0){
//							printf ("Voy a agregar un nuevo archivo a las estructuras\n");
							t_archivo* nuevoArchivo = malloc(sizeof(t_archivo));
							memset(nombreArchivoNovedad,'\0',200);
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad, sizeof(nombreArchivoNovedad), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo novedad");
								exit(-1);
							}
//							printf ("...Nombre Archivo: %s\n",nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del padre del archivo");
								exit(-1);
							}
//							printf ("...Padre del archivo: %d\n",padreArchivoNovedad);
							strcpy(nuevoArchivo->nombre, nombreArchivoNovedad);
							nuevoArchivo->padre = padreArchivoNovedad;
							if ((nbytes = recv(socket_fs, &cantidadBloquesArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv de cantidad de bloques del archivo");
								exit(-1);
							}
//							printf ("...Cantidad de bloques del archivo: %d\n",cantidadBloquesArchivo);
							nuevoArchivo->bloques = list_create();
							a=0;
							while (a < cantidadBloquesArchivo){
								t_bloque* bloqueArchivoNovedad = malloc(sizeof(t_bloque));
								bloqueArchivoNovedad->copias = list_create();
								if ((nbytes = recv(socket_fs, &cantCopiasArchivoNovedad, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
									perror("recv");
									log_error(logger,"FALLO el Recv de cantidad de copias del bloque del archivo");
									exit(-1);
								}
//								printf ("... ...Cantidad de copias del bloque:%d\n",cantCopiasArchivoNovedad);
								b=0;
								while (b < cantCopiasArchivoNovedad){
									t_copias* copiaBloqueNovedad = malloc(sizeof(t_copias));
									memset(nodoId,'\0',6);
									if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
										perror("recv");
										log_error(logger,"FALLO el Recv del nodo de la copia del archivo");
										exit(-1);
									}
//									printf ("... ... ...Nodo ID: %s\n",nodoId);
									if ((nbytes = recv(socket_fs, &bloqueNodo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
										perror("recv");
										log_error(logger,"FALLO el Recv del bloque del nodo donde está el archivo");
										exit(-1);
									}
//									printf ("... ... ...Bloque Nodo: %d\n",bloqueNodo);
									copiaBloqueNovedad->nodo=string_new();
									strcpy(copiaBloqueNovedad->nodo, nodoId);
									copiaBloqueNovedad->bloqueNodo =bloqueNodo;
									list_add(bloqueArchivoNovedad->copias, copiaBloqueNovedad);
									b++;
								}
								list_add(nuevoArchivo->bloques,bloqueArchivoNovedad);
								a++;
							}
							list_add(listaArchivos, nuevoArchivo);

//							//listar para ver si se agregó bien a la lista
//							int iu,ju,ku, cant_archivosu,cant_bloquesu,cant_copiasu;
//							t_archivo *archiu;
//							t_bloque *bloqueu;
//							t_copias *copiau;
//							cant_archivosu = list_size(listaArchivos);
//							if (cant_archivosu==0){
////								printf ("No hay archivos cargados en MDFS\n");
//							}
//							for (iu = 0; iu < cant_archivosu; iu++) {
//								archiu = list_get(listaArchivos, iu);
////								printf("\n\n");
////								printf("Archivo: %s\nPadre: %d\n",archiu->nombre,archiu->padre);
////								printf("\n");
//								cant_bloquesu=list_size(archiu->bloques);
//								for (ju = 0; ju < cant_bloquesu; ju++){
//									bloqueu=list_get(archiu->bloques,ju);
////									printf ("Numero de bloque: %d\n",ju);
//									cant_copiasu=list_size(bloqueu->copias);
//									for (ku=0;ku<cant_copiasu;ku++){
//										copiau=list_get(bloqueu->copias,ku);
////										printf ("Copia %d del bloque %d\n",ku,ju);
////										printf ("----------------------\n");
////										printf ("	Nodo: %s\n	Bloque: %d\n\n",copiau->nodo,copiau->bloqueNodo);
//									}
//								}
//							}



						}
						//fin update de CopiarArchivoAMDFS

						if (strcmp(identificacion,"elim_bloque")==0){
//							printf ("Voy a eliminar una copia de un bloque de un archivo a las estructuras\n");
							memset(nombreArchivoNovedad, '\0',200);
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad,	sizeof(nombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo del borrar bloque");
								exit(-1);
							}
//							printf("...Nombre del archivo del borrar bloque: %s\n", nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del padre del archivo del borrar bloque");
								exit(-1);
							}
//							printf ("...Padre del archivo del borrar bloque: %d\n",padreArchivoNovedad);
							memset(nodoId,'\0',6);
							if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nodo del borrar bloque");
								exit(-1);
							}
//							printf("...Nodo del archivo del borrar bloque: %s\n", nodoId);
							if ((nbytes = recv(socket_fs, &bloqueNodo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del bloque del nodo del borrar bloque");
								exit(-1);
							}
//							printf ("...Bloque del borrar bloque: %d\n",bloqueNodo);
							t_list * B;
							int copiaEncontrada = 0;
							B = buscarBloques (nombreArchivoNovedad, padreArchivoNovedad);
							int CB;
							CB = list_size(B);
							int PB;
							t_bloque * block;
							for(PB = 0; PB < CB; PB++){
								block = list_get(B, PB);
								t_copias * CB;
								int CCB;
								int PC;
								CCB = list_size(block->copias);
								for(PC = 0; PC < CCB; PC ++){
									CB = list_get(block->copias,PC);
									if(strcmp(CB->nodo, nodoId) ==0 && CB->bloqueNodo == bloqueNodo){
										copiaEncontrada = 1;
										break;
									}
								}
								if(copiaEncontrada == 1){
									list_remove_and_destroy_element(block->copias,PC,(void*)eliminarCopia);
									break;
								}
							}
							if(copiaEncontrada == 1) break;
						}

						// fin de update eliminar bloque
						if (strcmp(identificacion,"nuevo_bloque")==0){
//							printf ("Voy a agregar una nueva copia de un bloque de un archivo a las estructuras\n");
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad,	sizeof(nombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo del copiar bloque");
								exit(-1);
							}
//							printf("...Nombre del archivo del copiar bloque: %s\n", nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del padre del archivo del copiar bloque");
								exit(-1);
							}
//							printf ("...Padre del archivo del copiar bloque: %d\n",padreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &bloqueNodo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del bloque del nodo del copiar bloque");
								exit(-1);
							}
//							printf ("...Bloque del copiar bloque: %d\n",bloqueNodo);
							memset(nodoId,'\0',6);
							if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nodo del copiar bloque");
								exit(-1);
							}
//							printf("...Nodo del archivo del copiar bloque: %s\n", nodoId);
							if ((nbytes = recv(socket_fs, &bloqueNodoDestino, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del bloque del nodo destino del copiar bloque");
								exit(-1);
							}
//							printf ("...Bloque del copiar bloque: %d\n",bloqueNodoDestino);
							t_archivo *miArchivo;
							t_bloque *miBloque;
							t_copias *miCopia=malloc(sizeof(t_copias));
							int g;
							for (g=0;g<list_size(listaArchivos);g++){
								miArchivo=list_get(listaArchivos,g);
								if (miArchivo->padre==padreArchivoNovedad && strcmp(miArchivo->nombre,nombreArchivoNovedad)==0){
									miBloque=list_get(miArchivo->bloques,bloqueNodo);
									miCopia->bloqueNodo=bloqueNodoDestino;
									miCopia->nodo=strdup(nodoId);
									list_add(miBloque->copias,miCopia);
									break;
								}
							}




							/*t_list* B1;
							t_bloque* B;
							B1 = buscarBloques(nombreArchivoNovedad, padreArchivoNovedad);
							B = list_get(B1, bloqueNodoDestino);
							t_copias *D = malloc(sizeof(t_copias));
							memset(D->nodo,'\0',6);
							strcpy(D->nodo,nodoId);
							D->bloqueNodo = bloqueNodo;
							list_add(B->copias, D);*/

						}

						//Fin de update copiar bloque
						if (strcmp(identificacion,"nodo_desc")==0){
//							printf ("Voy a actualizar el estado de un nodo desconectado\n");
							memset(nodoId,'\0',6);
							if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nodo del copiar bloque");
								exit(-1);
							}
							t_nodo *unNodoParaActualizar;
							int pos_nodo;
							for (pos_nodo=0;pos_nodo<list_size(listaNodos);pos_nodo++){
								unNodoParaActualizar=list_get(listaNodos,pos_nodo);
								if (strcmp(unNodoParaActualizar->nodo_id,nodoId)==0){
									unNodoParaActualizar->estado=0;
									break;
								}
							}
							estadoMarta();
						}
						if (strcmp(identificacion,"nodo_elim")==0){
//							printf ("Voy a actualizar el estado de un nodo eliminado\n");
							memset(nodoId,'\0',6);
							if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nodo del copiar bloque");
								exit(-1);
							}
							t_nodo *unNodoParaActualizar;
							int pos_nodo;
							for (pos_nodo=0;pos_nodo<list_size(listaNodos);pos_nodo++){
								unNodoParaActualizar=list_get(listaNodos,pos_nodo);
								if (strcmp(unNodoParaActualizar->nodo_id,nodoId)==0){
									unNodoParaActualizar->estado=0;
									break;
								}
							}
							estadoMarta();
						}
						if (strcmp(identificacion,"nodo_agre")==0){
//							printf ("Voy a actualizar el estado de un nodo agregado\n");
							memset(nodoId,'\0',6);
							if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nodo del copiar bloque");
								exit(-1);
							}
							t_nodo *unNodoParaActualizar;
							int pos_nodo;
							for (pos_nodo=0;pos_nodo<list_size(listaNodos);pos_nodo++){
								unNodoParaActualizar=list_get(listaNodos,pos_nodo);
								if (strcmp(unNodoParaActualizar->nodo_id,nodoId)==0){
									unNodoParaActualizar->estado=1;
									break;
								}
							}
							estadoMarta();
						}
						if (strcmp(identificacion,"nodo_nuevo")==0){
//							printf ("Voy a actualizar la lista de nodos agregando un nodo nuevo\n");
							memset(nodoId,'\0',6);
							if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv de nodoId");
								exit(-1);
							}
							char ipNodo[17];
							memset(ipNodo, '\0',17);
							if ((nbytes = recv(socket_fs, ipNodo, sizeof(ipNodo), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv de la ip del nodo");
								exit(-1);
							}
							int puertoEscuchaNodo;
							if ((nbytes = recv(socket_fs, &puertoEscuchaNodo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del puerto escucha del nodo");
								exit(-1);
							}
							t_nodo *nodoTemporal=malloc(sizeof(t_nodo));
							memset(nodoTemporal->nodo_id,'\0', 6);
							strcpy(nodoTemporal->nodo_id, nodoId);
							nodoTemporal->estado =0;
							nodoTemporal->ip = strdup(ipNodo);
							nodoTemporal->puerto_escucha_nodo = puertoEscuchaNodo;
							nodoTemporal->cantMappers=0;
							nodoTemporal->cantReducers=0;
							list_add(listaNodos, nodoTemporal);
							i++;
							estadoMarta();

						}
					}
//					else{
//						//aca hablara el job
//					}
				}
			}
		}
	}
}

void *atenderJob (int *socketJob) {
	pthread_detach(pthread_self());
	char accion[BUF_SIZE];
	char mensaje_fs[BUF_SIZE];
	t_list* listaMappers;
	t_list* listaReducerParcial;
	memset(mensaje_fs,'\0',BUF_SIZE);
	memset(accion,'\0',BUF_SIZE);
	listaMappers = list_create();
	listaReducerParcial=list_create();
	char archivoResultado[200];
	int posicionArchivo;
	int nroRespuesta;
	char mensajeCombiner[3];
	char archivosDelJob[MENSAJE_SIZE];
	memset(mensajeCombiner, '\0', 3);
	memset(archivosDelJob, '\0', MENSAJE_SIZE);
	memset(archivoResultado,'\0', 200);
	char* stringNroJob=string_new();
	t_job* nuevoJob=malloc(sizeof(t_job));

	memset(nuevoJob->estado,'\0',10);
	memset(nuevoJob->resultadoDelJob,'\0',200);
	memset(nuevoJob->combiner,'\0',3);
	nuevoJob->mapperPendientes=0;
	nuevoJob->reducePendientes=0;

	//Mediante un mutex asigno al string "stringNroJob" el numero actual de nroJob y le sumo 1
	pthread_mutex_lock(&mutexNroJob);
	string_append(&stringNroJob,string_itoa(nroJob));
	nuevoJob->nroJob=nroJob;
	nroJob++;
	pthread_mutex_unlock(&mutexNroJob);


	//Recibe mensaje de si es o no combiner
	if(recv(*socketJob,mensajeCombiner,sizeof(mensajeCombiner),MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo al recibir el atributo COMBINER");
		//exit(-1);
	}
	//Para probar que recibio el atributo
//	printf("El Job %s acepta combiner\n",(char*)mensajeCombiner);

	//Recibe el archivo resultado del Job
	if(recv(*socketJob,archivoResultado,sizeof(archivoResultado),MSG_WAITALL)==-1){
	perror("recv");
	log_error(logger,"Fallo al recibir el archivo resultado");
	//exit(-1);
	}


	strcpy(nuevoJob->resultadoDelJob,archivoResultado);
	strcpy(nuevoJob->combiner,mensajeCombiner);
	strcpy(nuevoJob->estado,"En curso");

	pthread_mutex_lock(&mutexJobs);

	list_add(listaJobs,nuevoJob);

	pthread_mutex_unlock(&mutexJobs);


	//Para probar que recibio el archivo resultado final
//	printf("nombre del archivo resultado final %s \n",(char*)archivoResultado);

	if((recv(*socketJob, archivosDelJob, sizeof(archivosDelJob), MSG_WAITALL)) <= 0) {
		perror("recv");
		log_info(logger,"FALLO el Recv");
		//exit(-1);
	}

	/*Sumamos la cantidad total de bloques del todos los archivo de un JOB.
	 * Esa cantidad es la cantida total de los map que se deben ejecutar.*/



	// Separo el mensaje que recibo con los archivos a trabajar (Job envía todos juntos separados con ,)
	char** archivos =string_split((char*)archivosDelJob,",");


	//Lo siguiente es para probar que efectivamente se reciba la lista de archivos
	char nombreArchivo[TAM_NOMFINAL];
	memset(nombreArchivo,'\0',TAM_NOMFINAL);
	t_nodo *nodoAux;
	for(posicionArchivo=0;archivos[posicionArchivo]!=NULL;posicionArchivo++){
		int posArray;
		int cantBloques;
		int posBloques;
		int padre;
		char archivoAPedirPadre[TAM_NOMFINAL];
		char** arrayArchivo;
		t_list *bloques;

		memset(archivoAPedirPadre,'\0',TAM_NOMFINAL);

//		printf("Se debe trabajar en el archivo:%s\n",archivos[posicionArchivo]);
		//Separo el nombre del archivo por barras
		arrayArchivo = string_split(archivos[posicionArchivo], "/");
		for(posArray=0;arrayArchivo[posArray]!=NULL;posArray++){
			if(arrayArchivo[posArray+1]==NULL){
				//me quedo con el nombre del archivo
				strcpy(nombreArchivo,arrayArchivo[posArray]);
			}
		}
		memset(mensaje_fs,'\0',BUF_SIZE);
		strcpy(mensaje_fs, "dame padre");
		if(send(socket_fs,mensaje_fs, sizeof(mensaje_fs), MSG_WAITALL )==-1){
			perror("send");
			log_error(logger,"Fallo el dame padre");
			//exit(-1);
		}
		//Le mando el path a FS para que me mande el padre
		strcpy(archivoAPedirPadre,archivos[posicionArchivo]);

		if(send(socket_fs,archivoAPedirPadre,sizeof(archivoAPedirPadre),MSG_WAITALL) == -1) {
			perror("send");
			log_error(logger,"Fallo el envio del nombre de archivo al FS");
			//exit(-1);
		}
		//Recibe del FS el padre
		if(recv(socket_fs,&padre,sizeof(int),MSG_WAITALL)== -1){
			perror("recv");
			log_error(logger,"Fallo el recv del padre del FS");
			//exit(-1);
		}

//		//Validacion si existe el archivo
//		if(padre==-1){
//			char msjJob[BUF_SIZE];
//			memset(msjJob,'\0',BUF_SIZE);
//			strcpy(msjJob,"arch no disp");
//			log_warning(logger,"El archivo %s no existe en el MDFS",archivoAPedirPadre);
//			if(send(*socketJob,msjJob,sizeof(msjJob),MSG_WAITALL)==-1){
//				perror("Send");
//				log_error(logger,"Fallo el envio del mensaje \"archivo no disponible\" al job");
//			}
//			close(*socketJob);
//			nuevoJob->mapperPendientes=0;
//			nuevoJob->reducePendientes=0;
//			memset(nuevoJob->estado,'\0',10);
//			strcpy(nuevoJob->estado,"Fallido");
//			pthread_exit((void*)0);
//		}

		//De cada archivo que nos manda el Job buscamos y nos traemos los bloques

		bloques=buscarBloques(nombreArchivo,padre);
		cantBloques = list_size(bloques);
		for(posBloques=0; posBloques<cantBloques; posBloques++){ //recorremos los bloques del archivo que nos mando job

			//Si el archivo no está disponible, no se hace el Job
			if(!archivoDisponible(buscarArchivo(nombreArchivo,padre))){
				memset(nuevoJob->estado,'\0',10);
				strcpy(nuevoJob->estado,"Fallido");
				estadoMarta();

				char msjJob[BUF_SIZE];
				memset(msjJob,'\0',BUF_SIZE);
				strcpy(msjJob,"arch no disp");
				log_info(logger,"El archivo no está disponible, no se podrá hacer el Job");
				if(send(*socketJob,msjJob,sizeof(msjJob),MSG_WAITALL)==-1){
					perror("Send");
					log_error(logger,"Fallo el envio del mensaje \"archivo no disponible\" al job");
				}
				close(*socketJob);
				pthread_exit((void*)0);
			}


			int cantCopias;
			t_bloque *bloque;

			t_replanificarMap *mapper;
			t_mapper datosMapper;
			t_list *copiasNodo;
			copiasNodo=list_create();
			bloque = list_get(bloques,posBloques);
			cantCopias = list_size(bloque->copias);
			int posCopia;
			for(posCopia=0;posCopia<cantCopias;posCopia++){ // Por cada bloque del archivo recorremos las copias de dicho archivo
				t_nodo *nodo;
				t_copias *copia;
				copia = list_get(bloque->copias,posCopia);
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

			mapper=malloc(sizeof(t_replanificarMap));

			memset(datosMapper.ip_nodo,'\0',20);
			memset(datosMapper.archivoResultadoMap,'\0',TAM_NOMFINAL);
			memset(mapper->archivoResultadoMap,'\0',TAM_NOMFINAL);
			memset(mapper->nombreArchivoDelJob,'\0',TAM_NOMFINAL);
			strcpy(datosMapper.ip_nodo,nodoAux->ip);
			datosMapper.puerto_nodo= nodoAux->puerto_escucha_nodo;
			for(posCopia=0;posCopia<cantCopias;posCopia++){
				t_copias *copia;
				copia = list_get(bloque->copias,posCopia);
				if(strcmp(copia->nodo,nodoAux->nodo_id)==0){
					datosMapper.bloque=copia->bloqueNodo;
				}
			}

			char *nombreArchivoTemp=string_new();//Nombre del archivo que va a mandar a los nodos
			char *pathArchivoTemp = string_new(); //Path donde va a guardar los archivos temporales
			char *tiempo=string_new(); //string que tendrá la hora
			char **arrayNomArchivo=string_split(nombreArchivo,".");
			string_append(&pathArchivoTemp,"/tmp/");
			string_append(&nombreArchivoTemp,stringNroJob);
			string_append(&nombreArchivoTemp,arrayNomArchivo[0]);
			char **arrayTiempo=string_split(temporal_get_string_time(),":"); //creo array con hora minutos segundos y milisegundos separados
			string_append(&tiempo,arrayTiempo[0]);//Agrego horas
			string_append(&tiempo,arrayTiempo[1]);//Agrego minutos
			string_append(&tiempo,arrayTiempo[2]);//Agrego segundos
			string_append(&tiempo,arrayTiempo[3]);//Agrego milisegundos
			string_append(&nombreArchivoTemp,"_Bloq");
			string_append(&nombreArchivoTemp,string_itoa(posBloques));
			string_append(&nombreArchivoTemp,"_");
			string_append(&nombreArchivoTemp,tiempo);
			string_append(&nombreArchivoTemp,".txt");
			string_append(&pathArchivoTemp,nombreArchivoTemp);
			strcpy(datosMapper.archivoResultadoMap,pathArchivoTemp);

			memset(accion,'\0',BUF_SIZE);
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

			mapper->bloqueArchivo = posBloques;
			memset(mapper->nodoId,'\0',6);
			strcpy(mapper->nombreArchivoDelJob,nombreArchivo);
			strcpy(mapper->archivoResultadoMap,datosMapper.archivoResultadoMap);
			char* nodoIdTemp=string_new();
			string_append(&nodoIdTemp,nodoAux->nodo_id);
			strcpy(mapper->nodoId,nodoIdTemp);
			mapper->resultado=2;
			mapper->padreArchivoJob=padre;
			list_add(listaMappers,mapper);

			//Buscar nodoAux en la lista general comparando por nodo_id y sumarle cantMapper
			pthread_mutex_lock(&mutexModNodo);
			sumarCantMapper(nodoAux->nodo_id);
			pthread_mutex_unlock(&mutexModNodo);

			nuevoJob->mapperPendientes++;

			free(nombreArchivoTemp);
			free(pathArchivoTemp);
//			free(tiempo);

			list_destroy(copiasNodo);
		}
	}

	//printf("Se enviaron todos los map --> Se esperan las respuestas\n");
	memset(nuevoJob->estado,'\0',10);
	strcpy(nuevoJob->estado,"Maps");
	estadoMarta();
	//SE ESPERAN EN UN CICLO FOR LA CANTIDAD DE RESPUESTAS IGUAL A CUANTOS MAPPER SE HALLAN ENVIADO (con tamaño de la listaMappers)

	for(nroRespuesta=0;nroRespuesta<list_size(listaMappers);nroRespuesta++){

		if(nroRespuesta%4==0){ //cada 4 respuestas muestro el estado
			estadoMarta();
		}

		t_respuestaMap respuestaMap;
		//Recibo respuesta del Job de cada map
		if(recv(*socketJob,&respuestaMap, sizeof(t_respuestaMap),MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo al recibir el ok del job");
		//exit(-1);
		}
		t_replanificarMap *map;
		int posMapper;
		//Recorro lista de mappers y comparo cada archivoResultadoMap con el que me dio la respuesta del job, cuando coincide corta
		for(posMapper = 0; posMapper < list_size(listaMappers); posMapper++){
			map = list_get(listaMappers, posMapper);
			if(strcmp(map->archivoResultadoMap,respuestaMap.archivoResultadoMap)==0){
				break;
			}
		}
		// Al resultado del map que coincidio le asigno el resultado de la respuesta del job
		map->resultado = respuestaMap.resultado;

		if(map->resultado ==1){
//			printf("La cantidad de map pendiente del job es %d",unJob.mapperPendientes ++);
//			printf("El map %s falló\n",map->archivoResultadoMap);
//			printf("El nodo: %s esta deshabiltado\n", nodoAux->nodo_id);
			int posCopia;
			int cantidadCopias;
			t_list *bloquesNoMap;
			t_bloque *bloqueQueFallo;
			t_copias* copiaBloque;
			t_nodo *nodoCopia;
			t_list* nodoSinMap;
			nodoSinMap=list_create();

			//Recorro la lista de t_replanificarMap para buscar los que salieron OK del mismo nodo y los marco como fallido
			//Luego replanifico éste map que llego mal y todos los que estaban ok

			int posMapp;
			t_list* nodosQueFallaron=list_create();
			list_add(nodosQueFallaron,map);
			for(posMapp=0;posMapp<list_size(listaMappers);posMapp++){
				t_replanificarMap* mapcito;
				mapcito = list_get(listaMappers,posMapp);
				if((strcmp(map->nodoId,mapcito->nodoId)==0)&&(mapcito->resultado==0)){
					mapcito->resultado = 1;
					list_add(nodosQueFallaron,mapcito);
				}
			}

			if(list_size(nodosQueFallaron)>1){ // Si ya habia maps OK para este Job, y es el primero de un nodo que llego KO
				nuevoJob->mapperPendientes=nuevoJob->mapperPendientes+(list_size(nodosQueFallaron)-1);
			}


			int posFalla;
			for(posFalla=0;posFalla<list_size(nodosQueFallaron);posFalla++){

				t_replanificarMap* nodoAReplanificar;
				nodoAReplanificar=list_get(nodosQueFallaron,posFalla);

				//buscamos en la lista general de archivos los bloques del map que fallo
				bloquesNoMap = buscarBloques(nodoAReplanificar->nombreArchivoDelJob,nodoAReplanificar->padreArchivoJob);

				//Si el archivo no está disponible, no se hace el Job
				if(!archivoDisponible(buscarArchivo(nodoAReplanificar->nombreArchivoDelJob,nodoAReplanificar->padreArchivoJob))){
					memset(nuevoJob->estado,'\0',10);
					strcpy(nuevoJob->estado,"Fallido");
					estadoMarta();
					char msjJob[BUF_SIZE];
					memset(msjJob,'\0',BUF_SIZE);
					strcpy(msjJob,"arch no disp");
					log_info(logger,"El archivo no está disponible, no se podrá hacer el Job");
					if(send(*socketJob,msjJob,sizeof(msjJob),MSG_WAITALL)==-1){
						perror("Send");
						log_error(logger,"Fallo el envio del mensaje \"archivo no disponible\" al job");
					}
					close(*socketJob);
					pthread_exit((void*)0);
				}

				//buscamos en los bloques el bloque en el que salio mal el MAP
				bloqueQueFallo = list_get(bloquesNoMap,nodoAReplanificar->bloqueArchivo);
				cantidadCopias = list_size(bloqueQueFallo->copias);


				for(posCopia=0;posCopia<cantidadCopias;posCopia++){ // recorremos las copias del bloque que salio mal el MAP

					copiaBloque = list_get(bloqueQueFallo->copias,posCopia);
					// Nos traemos cada nodo en donde esta cada una de las copias del bloque que fallo
					nodoCopia= buscarCopiaEnNodos(copiaBloque);

					//si esta el nodo de la copia activo
					if(nodoCopia->estado == 1){
						// Creamos una sublista de la lista global de nodos con los nodos en los que esta cada copia del archivo
						list_add(nodoSinMap,nodoCopia);
					}
				}

				t_nodo *otroNodoAux;
				// Ordenamos la sublista segun la suma de la cantidad de map y reduce
				list_sort(nodoSinMap, (void*) ordenarSegunMapYReduce);
				otroNodoAux = list_get(nodoSinMap,0); // Nos traemos el nodo con menos carga
				//Del nodo que nos trajimos agarramos los datos que necesitamos para mandarle al job

				t_mapper datosReplanificacionMap;

				memset(datosReplanificacionMap.archivoResultadoMap,'\0',TAM_NOMFINAL);
				memset(datosReplanificacionMap.ip_nodo,'\0',20);
				strcpy(datosReplanificacionMap.ip_nodo, otroNodoAux->ip);
				datosReplanificacionMap.puerto_nodo = otroNodoAux->puerto_escucha_nodo;
				for(posCopia=0;posCopia<cantidadCopias;posCopia++){
					copiaBloque = list_get(bloqueQueFallo->copias,posCopia);
					if(strcmp(copiaBloque->nodo,otroNodoAux->nodo_id)==0){
						datosReplanificacionMap.bloque=copiaBloque->bloqueNodo;
					}
				}
				char* tiempoReplanificado=string_new();
				char** arrayTiempoReplanificado;
				char* archivoReplanificadoTemp=string_new();
				char* pathArchivoRTemp= string_new();
				char **arrayNomArchivo=string_split(nodoAReplanificar->nombreArchivoDelJob,".");

				strcpy(pathArchivoRTemp,"/tmp/");
				string_append(&archivoReplanificadoTemp,stringNroJob);
				string_append(&archivoReplanificadoTemp,arrayNomArchivo[0]);
				arrayTiempoReplanificado=string_split(temporal_get_string_time(),":"); //creo array con hora minutos segundos y milisegundos separados
				string_append(&tiempoReplanificado,arrayTiempoReplanificado[0]);//Agrego horas
				string_append(&tiempoReplanificado,arrayTiempoReplanificado[1]);//Agrego minutos
				string_append(&tiempoReplanificado,arrayTiempoReplanificado[2]);//Agrego segundos
				string_append(&tiempoReplanificado,arrayTiempoReplanificado[3]);//Agrego milisegundos
				string_append(&archivoReplanificadoTemp,"_Bloq");
				string_append(&archivoReplanificadoTemp,string_itoa(nodoAReplanificar->bloqueArchivo));
				string_append(&archivoReplanificadoTemp,"_");
				string_append(&archivoReplanificadoTemp,tiempoReplanificado);
				string_append(&archivoReplanificadoTemp,"Rep.txt");
				string_append(&pathArchivoRTemp,archivoReplanificadoTemp);
				strcpy(datosReplanificacionMap.archivoResultadoMap,pathArchivoRTemp);

				memset(accion,'\0',BUF_SIZE);
				strcpy(accion,"ejecuta map");
				//Le avisamos al job que vamos a mandarle rutina map
				if(send(*socketJob,accion,sizeof(accion),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger,"Fallo el envio de los datos para el mapper");
					exit(-1);
				}
				// Le mandamos los datos que necesita el job para aplicar map
				if(send(*socketJob,&datosReplanificacionMap,sizeof(t_mapper),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger,"Fallo el envio de los datos para el mapper");
					exit(-1);
				}
//				printf("El estado del nodo %s es habilitado\n", otroNodoAux->nodo_id);
//				printf("El nodo %s está ejecutando map\n", otroNodoAux->nodo_id);
				//rellenamos un nuevo struct t_replanificar map conlos datos del map que acabamos de enviar y lo agregamos a lista mappers
				t_replanificarMap *nuevoMap=malloc(sizeof(t_replanificarMap));

				nuevoMap->bloqueArchivo = nodoAReplanificar->bloqueArchivo ;
				memset(nuevoMap->nodoId,'\0',6);
				memset(nuevoMap->archivoResultadoMap,'\0',TAM_NOMFINAL);
				memset(nuevoMap->nombreArchivoDelJob,'\0',TAM_NOMFINAL);
				strcpy(nuevoMap->nombreArchivoDelJob,nodoAReplanificar->nombreArchivoDelJob);
				strcpy(nuevoMap->archivoResultadoMap,datosReplanificacionMap.archivoResultadoMap);
				char* nodoIdTemp=string_new();
				string_append(&nodoIdTemp,otroNodoAux->nodo_id);
				strcpy(nuevoMap->nodoId,nodoIdTemp);
				nuevoMap->resultado=2;
				nuevoMap->padreArchivoJob=nodoAReplanificar->padreArchivoJob;
				list_add(listaMappers,nuevoMap);

				pthread_mutex_lock(&mutexModNodo);
				// le resto cantMappers al nodo que le salio mal el MAP
				t_nodo* nodoASetearACero=traerNodo(nodoAReplanificar->nodoId);
				nodoASetearACero->cantMappers=0;
				nodoASetearACero->cantReducers=0;
				// le sumo cantMappers al nodo que acabo de mandar a hacer MAP
				sumarCantMapper(nuevoMap->nodoId);
				pthread_mutex_unlock(&mutexModNodo);
			}

		}
		if(map->resultado==0){

//			printf("El map %s salio ok\n",map->archivoResultadoMap);
			// buscar en la lista del struct el nodo_id y luego buscarlo en la lista gral de nodos y restarle 1 a su catMappers
			pthread_mutex_lock(&mutexModNodo);
			restarCantMapper(map->nodoId);
			pthread_mutex_unlock(&mutexModNodo);
			nuevoJob->mapperPendientes--;

		}
	}

	//printf("Llegaron todas las respuestas --> Se planifican los reduce\n");
	memset(nuevoJob->estado,'\0',10);
	strcpy(nuevoJob->estado,"Reduce");
	estadoMarta();

/************************************************************************************************************************************************
* SIN COMBINER
* ************************************************************************************************************************************************/
	//printf("Si es con combiner la cantidad de reduce pendientes es %d\n",unJob.reducePendientes);
	sleep(5);

	//Si es sin combiner manda a hacer reduce al nodo que tenga mas archivos resultados MAP
	if(strcmp(mensajeCombiner, "NO")==0){
		int posicionMapper,cantNodosMapOk;
		t_replanificarMap *mapperOk;
		t_list* listaNodosMapperOk;
		t_list* listaNodosDistintos=list_create();
		listaNodosMapperOk = list_create();
		//Recorro lista de mappers y por cada mapperOk con resultado 0 que es map ok, me lo guardo  en una lista
		for (posicionMapper =0; posicionMapper < list_size(listaMappers); posicionMapper++){
			mapperOk = list_get(listaMappers, posicionMapper);
			if(mapperOk->resultado == 0){
				list_add(listaNodosMapperOk,mapperOk);
			}

		}
		int posMapOK;
		t_replanificarMap *mapOk;
		cantNodosMapOk = list_size(listaNodosMapperOk);

		//recorro la lista de mappers ok y por cada uno saco la cantidad de veces que esta repetido, es decir donde se hicieron mas Maps locales
		//y la asigno como mayor
		//Genero una lista con todos los nodos
		for(posMapOK=0;posMapOK<cantNodosMapOk;posMapOK++){
			char *nodoId=string_new();
			int indice;
			int agregar=1;
			mapOk = list_get(listaNodosMapperOk,posMapOK);
			strcpy(nodoId,mapOk->nodoId);
			//Busco si el nodoId está en la listaNodosDistintos, si no está lo agrego en esa lista
			for(indice=0;indice<list_size(listaNodosDistintos);indice++){
				char* nodoDeLaLista=list_get(listaNodosDistintos,indice);
				if(strcmp(nodoDeLaLista,nodoId)==0){
					agregar=0;
				}
			}
			if(agregar==1){
				list_add(listaNodosDistintos,nodoId);
			}
		}

		int posicionNodo,posNodos;
		int mayorRepetido;
		int cantNodoRepetido=0;
		char* nodoAEvaluar;

		//Considero el primer nodo como el más repetido;
		char* nodoMasRep=list_get(listaNodosDistintos,0);
		mayorRepetido = 0;

		//Busco por cada nodo que hay, todos los repetidos, si es mayor asigno ese nodo
		for(posNodos=0;posNodos<list_size(listaNodosDistintos);posNodos++){
			nodoAEvaluar=list_get(listaNodosDistintos,posNodos);
			for(posicionNodo = 0; posicionNodo < cantNodosMapOk; posicionNodo++){
				mapOk = list_get(listaNodosMapperOk, posicionNodo);
				if(strcmp(mapOk->nodoId,nodoAEvaluar)==0){
					cantNodoRepetido++;
				}
			}
			if(cantNodoRepetido>mayorRepetido){
				mayorRepetido=cantNodoRepetido;
				nodoMasRep=nodoAEvaluar;
			}
			cantNodoRepetido=0;
		}

		t_nodo * nodoG =traerNodo(nodoMasRep);
		t_reduce nodoReducer;

		//Inicializo estructura del nodo que le voy a mandar a JOB
		memset(nodoReducer.ip_nodoPpal,'\0',20);
		memset(nodoReducer.nombreArchivoFinal,'\0',TAM_NOMFINAL);
		//Completo estructura del nodo que le voy a mandar a JOB
		strcpy(nodoReducer.ip_nodoPpal, nodoG->ip);
		nodoReducer.puerto_nodoPpal = nodoG->puerto_escucha_nodo;


		char* tiempoReduce=string_new();
		char** arrayTiempoReduce;
		char* archivoReduceTemp=string_new();

		string_append(&archivoReduceTemp,"/tmp/Job");
		string_append(&archivoReduceTemp,stringNroJob);
		arrayTiempoReduce=string_split(temporal_get_string_time(),":"); //creo array con hora minutos segundos y milisegundos separados
		string_append(&tiempoReduce,arrayTiempoReduce[0]);//Agrego horas
		string_append(&tiempoReduce,arrayTiempoReduce[1]);//Agrego minutos
		string_append(&tiempoReduce,arrayTiempoReduce[2]);//Agrego segundos
		string_append(&tiempoReduce,arrayTiempoReduce[3]);//Agrego milisegundos
		string_append(&archivoReduceTemp,"_ReduceFinal_");
		string_append(&archivoReduceTemp,tiempoReduce);
		string_append(&archivoReduceTemp,".txt");

		strcpy(nodoReducer.nombreArchivoFinal,archivoReduceTemp);

		// Mando a ejecutar reduce
		char mensajeReducer[BUF_SIZE];
		memset(mensajeReducer, '\0', BUF_SIZE);
		strcpy(mensajeReducer,"ejecuta reduce");
		if(send(*socketJob, mensajeReducer,sizeof(mensajeReducer),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger, "Fallo mandar hacer reducer");
		}

		if(send(*socketJob, &nodoReducer,sizeof(t_reduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger, "Fallo mandar datos para hacer reducer");
		}


		// Mando cantidad de archivos a hacer reduce
		if(send(*socketJob, &cantNodosMapOk,sizeof(int),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger, "Fallo mandar la cantidad de archivos a hacer reduce");
			//exit(-1);
		}

		int posicionNodoOk;
		t_replanificarMap *nodoOk;
		for(posicionNodoOk = 0; posicionNodoOk < cantNodosMapOk; posicionNodoOk ++){
			t_archivosReduce archReduce;
			memset(archReduce.archivoAAplicarReduce,'\0',TAM_NOMFINAL);
			memset(archReduce.ip_nodo,'\0',20);

			nodoOk = list_get(listaNodosMapperOk,posicionNodoOk);
			strcpy(archReduce.archivoAAplicarReduce, nodoOk->archivoResultadoMap);
			t_nodo *nodoGlobal =traerNodo(nodoOk->nodoId);
			strcpy(archReduce.ip_nodo, nodoGlobal->ip);
			archReduce.puerto_nodo = nodoGlobal->puerto_escucha_nodo;

			// Mando por cada t_replanificarMap ok, los datos de cada archivo
			if(send(*socketJob, &archReduce,sizeof(t_archivosReduce),MSG_WAITALL)==-1){
				perror("send");
				log_error(logger, "Fallo mandar el archivo a hacer reduce");
				//exit(-1);
			}
		}

		t_nodo* nodoASumarReduce=buscarNodoPorIPYPuerto(nodoReducer.ip_nodoPpal,nodoReducer.puerto_nodoPpal);

		pthread_mutex_lock(&mutexModNodo);
		sumarCantReducers(nodoASumarReduce->nodo_id);
		pthread_mutex_unlock(&mutexModNodo);

		nuevoJob->reducePendientes++;

//		printf("Se envio un reduce sin combiner\n");
		estadoMarta();

		//ACA RECIBO LA RESPUESTA DE JOB
		t_respuestaReduce respuestaReduceFinal;
		if(recv(*socketJob,&respuestaReduceFinal, sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("recv");
			log_error(logger,"Fallo al recibir el ok del job");
			//exit(-1);
		}

		if(respuestaReduceFinal.resultado == 1){

			memset(nuevoJob->estado,'\0',10);
			strcpy(nuevoJob->estado,"Fallido");
			//Se aborta la ejecución de Reduce
			log_info(logger,"Falló el Reduce %s. Se abortará el job",respuestaReduceFinal.archivoResultadoReduce);
			char jobAborta[BUF_SIZE];
			memset(jobAborta,'\0',BUF_SIZE);
			strcpy(jobAborta,"aborta");
			if(send(*socketJob,jobAborta,sizeof(jobAborta),MSG_WAITALL)==-1){
				log_error(logger,"Fallo el envío al Job de que aborte por falla de un reduce");
			}
			t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduceFinal.ip_nodo,respuestaReduceFinal.puerto_nodo);
			pthread_mutex_lock(&mutexModNodo);
			restarCantReducers(nodoARestar->nodo_id);
			pthread_mutex_unlock(&mutexModNodo);

			nuevoJob->reducePendientes--;

			estadoMarta();

			close(*socketJob);
			pthread_exit((void*)0);
		}

		if(respuestaReduceFinal.resultado == 0){

//			printf("Reduce %s exitoso\n",respuestaReduceFinal.archivoResultadoReduce);
			t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduceFinal.ip_nodo,respuestaReduceFinal.puerto_nodo);

			pthread_mutex_lock(&mutexModNodo);
			restarCantReducers(nodoARestar->nodo_id);
			pthread_mutex_unlock(&mutexModNodo);
			nuevoJob->reducePendientes--;

		}

//		printf("Termino el reduce sin combiner\n");
		memset(nuevoJob->estado,'\0',10);
		strcpy(nuevoJob->estado,"Exitoso");
		estadoMarta();


		//Si llega hasta acá, el Job sin combiner termino OK
		//Le digo al Job que finalice
		char finaliza[BUF_SIZE];
		memset(finaliza,'\0',BUF_SIZE);
		strcpy(finaliza,"finaliza");
		if(send(*socketJob,finaliza,sizeof(finaliza),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo al enviarle al Job que finalize");
		}
		close(*socketJob);

		//Buscar el nodo donde está el archivo resultado del reduce para mandarle al FS el nodoID
		t_nodo* nodoResultado;
		nodoResultado = buscarNodoPorIPYPuerto(nodoReducer.ip_nodoPpal,nodoReducer.puerto_nodoPpal);
//		printf("El estado del nodo %s donde se va a guardar el archivo resultado esta habilitado\n", nodoResultado->nodo_id);
		//Le digo al FS que se copie el resultado
		log_info(logger,"El job %d termino OK.",nuevoJob->nroJob);

		char** nombreResSpliteado=string_split(nodoReducer.nombreArchivoFinal,"/");
		char nombreRes[TAM_NOMFINAL];
		memset(nombreRes,'\0',TAM_NOMFINAL);
		strcpy(nombreRes,nombreResSpliteado[1]);

		//Le aviso a FS que le voy a mandar el Nodo ID
		memset(mensaje_fs,'\0',BUF_SIZE);
		strcpy(mensaje_fs, "resultado");
		if(send(socket_fs,mensaje_fs, sizeof(mensaje_fs), MSG_WAITALL )==-1){
			perror("send");
			log_error(logger,"Fallo el envío del mensaje");
			//exit(-1);
		}
		//Le mando a FS el Nodo ID
		if(send(socket_fs,nodoResultado->nodo_id,sizeof(nodoResultado->nodo_id),MSG_WAITALL) == -1) {
			perror("send");
			log_error(logger,"Fallo el envio del archivo resultado del reduce con combiner al FS");
			//exit(-1);
		}

		//Le mando el archivo Resultado de reduce
		if(send(socket_fs,nombreRes,sizeof(nombreRes),MSG_WAITALL) == -1) {
			perror("send");
			log_error(logger,"Fallo el envio del archivo resultado del reduce con combiner al FS");
			//exit(-1);
		}
		//Le mando al FS donde debe guardar el resultado
		if(send(socket_fs,archivoResultado,sizeof(archivoResultado),MSG_WAITALL) == -1) {
			perror("send");
			log_error(logger,"Fallo el envio del archivo resultado del reduce con combiner al FS");
			//exit(-1);
		}



	}


	/************************************************************************************************************************************************
	 * CON COMBINER
	 * ************************************************************************************************************************************************/

	if(strcmp(mensajeCombiner,"SI")==0){
		int jobAbortado=0;
		t_replanificarMap *mapOk;
		t_list *listaMapOk; //Lista de nodo en los que el map salió OK
		t_list *listaNodosDistintos;
		listaMapOk = list_create();
		int posMapper;
		int posMapOK;
		listaNodosDistintos = list_create();
		//Recorrer la lista t_replanificarMap y guardo en una lista todos los nodos en los que map salió OK
		for(posMapper=0;posMapper<list_size(listaMappers);posMapper++){
			mapOk = list_get(listaMappers,posMapper);
			if((mapOk->resultado == 0)){
				list_add(listaMapOk,mapOk);
			}
		}
		//Guardo en la listaNodosDistintos todos los nodoId que hay que mandar para hacer reduce
		for(posMapOK=0;posMapOK<list_size(listaMapOk);posMapOK++){
			char *nodoId=string_new();
			int indice;
			int agregar=1;
			mapOk = list_get(listaMapOk,posMapOK);
			strcpy(nodoId,mapOk->nodoId);
			//Busco si el nodoId está en la listaNodosDistintos, si no está lo agrego en esa lista
			for(indice=0;indice<list_size(listaNodosDistintos);indice++){
				char* nodoDeLaLista=list_get(listaNodosDistintos,indice);
				if(strcmp(nodoDeLaLista,nodoId)==0){
					agregar=0;
				}
			}
			if(agregar==1){
				list_add(listaNodosDistintos,nodoId);
			}
		}
//		printf("Cuando es con combiner la cantidad de reduce pendientes del job es %d\n", unJob.reducePendientes + 1);

		int i;
		t_list *listaMapDelNodo;
		for(i=0;i<list_size(listaNodosDistintos);i++){
			t_nodo* nodoPrincipal;
			t_reduce nodoReducerParcial;
			t_reduce* nodoReducer=malloc(sizeof(t_reduce));
			memset(nodoReducerParcial.ip_nodoPpal,'\0',20);
			memset(nodoReducerParcial.nombreArchivoFinal,'\0',TAM_NOMFINAL);
			memset(nodoReducer->ip_nodoPpal,'\0',20);
			memset(nodoReducer->nombreArchivoFinal,'\0',TAM_NOMFINAL);
			listaMapDelNodo = list_create();
			char* nodoDistinto=list_get(listaNodosDistintos,i);
			nodoPrincipal=traerNodo(nodoDistinto);

			int j;
			for(j=0; j<list_size(listaMapOk);j++){
				t_replanificarMap *nodoMapOk;
				nodoMapOk = list_get(listaMapOk,j);
				if(strcmp(nodoMapOk->nodoId,nodoDistinto)==0){
					list_add(listaMapDelNodo,nodoMapOk);
				}
			}
			//if(list_size(listaMapDelNodo)>1){
				// Mando a ejecutar reduce
				char mensajeReducerParcial[BUF_SIZE];
				memset(mensajeReducerParcial, '\0', BUF_SIZE);
				strcpy(mensajeReducerParcial,"ejecuta reduce");
				if(send(*socketJob, mensajeReducerParcial,sizeof(mensajeReducerParcial),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger, "Fallo mandar hacer reducer");
					//exit(-1);
				}
				//Mando los datos del Nodo Principal y el nombre del archivo resultado//

				strcpy(nodoReducerParcial.ip_nodoPpal,nodoPrincipal->ip);
				nodoReducerParcial.puerto_nodoPpal=nodoPrincipal->puerto_escucha_nodo;
				strcpy(nodoReducer->ip_nodoPpal,nodoPrincipal->ip);
				nodoReducer->puerto_nodoPpal=nodoPrincipal->puerto_escucha_nodo;

				char* tiempoReduce=string_new();
				char** arrayTiempoReduce;
				char* archivoReduceTemp=string_new();

				string_append(&archivoReduceTemp,"/tmp/Job");
				string_append(&archivoReduceTemp,stringNroJob);
				arrayTiempoReduce=string_split(temporal_get_string_time(),":"); //creo array con hora minutos segundos y milisegundos separados
				string_append(&tiempoReduce,arrayTiempoReduce[0]);//Agrego horas
				string_append(&tiempoReduce,arrayTiempoReduce[1]);//Agrego minutos
				string_append(&tiempoReduce,arrayTiempoReduce[2]);//Agrego segundos
				string_append(&tiempoReduce,arrayTiempoReduce[3]);//Agrego milisegundos
				string_append(&archivoReduceTemp,"_Reduce");
				string_append(&archivoReduceTemp,string_itoa(i));
				string_append(&archivoReduceTemp,"_");
				string_append(&archivoReduceTemp,tiempoReduce);
				string_append(&archivoReduceTemp,".txt");

				strcpy(nodoReducerParcial.nombreArchivoFinal,archivoReduceTemp);
				strcpy(nodoReducer->nombreArchivoFinal,archivoReduceTemp);

				if(send(*socketJob, &nodoReducerParcial,sizeof(t_reduce),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger, "Fallo mandar datos para hacer reducer");
					//exit(-1);
				}

				// Mando cantidad de archivos a hacer reduce

				int cantArch = list_size(listaMapDelNodo);
				if(send(*socketJob, &cantArch,sizeof(int),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger, "Fallo mandar la cantidad de archivos a hacer reduce");
					//exit(-1);
				}
				//Le mando los datos de cada uno de los archivos: IP Nodo, Puerto Nodo, nombreArchivo resultado de map (t_archivosReduce)
				int posNodoOk;
				t_replanificarMap *nodoOk;
				for(posNodoOk = 0; posNodoOk < list_size(listaMapDelNodo); posNodoOk ++){
					t_archivosReduce archReducePorNodo;
					memset(archReducePorNodo.archivoAAplicarReduce,'\0',TAM_NOMFINAL);
					memset(archReducePorNodo.ip_nodo,'\0',20);

					nodoOk = list_get(listaMapDelNodo,posNodoOk);
					strcpy(archReducePorNodo.archivoAAplicarReduce, nodoOk->archivoResultadoMap);
					strcpy(archReducePorNodo.ip_nodo, nodoPrincipal->ip);
					archReducePorNodo.puerto_nodo = nodoPrincipal->puerto_escucha_nodo;

					// Mando por cada t_replanificarMap ok, los datos de cada archivo
					if(send(*socketJob, &archReducePorNodo,sizeof(t_archivosReduce),MSG_WAITALL)==-1){
						perror("send");
						log_error(logger, "Fallo mandar la archivo a hacer reduce");
						//exit(-1);
					}
				}

				pthread_mutex_lock(&mutexModNodo);
				sumarCantReducers(nodoPrincipal->nodo_id);
				pthread_mutex_unlock(&mutexModNodo);
				nuevoJob->reducePendientes++;

				list_add(listaReducerParcial,nodoReducer);
//			}
//			if(list_size(listaMapDelNodo)==1){
//				t_replanificarMap *nodoMapOkk;
//				nodoMapOkk=list_get(listaMapDelNodo,0);
//				strcpy(nodoReducer->ip_nodoPpal,nodoPrincipal->ip);
//				nodoReducer->puerto_nodoPpal=nodoPrincipal->puerto_escucha_nodo;
//				strcpy(nodoReducer->nombreArchivoFinal,nodoMapOkk->archivoResultadoMap);
//				list_add(listaReducerDeUnSoloArchivo,nodoReducer);
//			}
			list_destroy(listaMapDelNodo);
		}

		nuevoJob->reducePendientes++;

//		printf("Se enviaron todos los reduce con combiner --> Se esperan las respuestas \n");
		estadoMarta();
		//Recibir la respuesta del JOB confirmando que termino con los reduce de los que están en el mismo nodo
		//SE ESPERAN EN UN CICLO FOR LA CANTIDAD DE RESPUESTAS IGUAL A CUANTOS REDUCE SE HALLAN ENVIADO
		int posResp;

		for(posResp=0;posResp<list_size(listaReducerParcial);posResp++){
			t_respuestaReduce respuestaReduce;
			//Recibo respuesta del Job de cada reduce
			if(recv(*socketJob,&respuestaReduce, sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
				perror("recv");
				log_error(logger,"Fallo al recibir el ok del job");
				//exit(-1);
			}

			if(respuestaReduce.resultado == 1){
				t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduce.ip_nodo,respuestaReduce.puerto_nodo);
				//Se aborta la ejecución de Reduce
				log_info(logger,"Falló el Reduce %s. Se abortará el job",respuestaReduce.archivoResultadoReduce);

				pthread_mutex_lock(&mutexModNodo);
				restarCantReducers(nodoARestar->nodo_id);
				pthread_mutex_unlock(&mutexModNodo);
				nuevoJob->reducePendientes--;


				jobAbortado=1; //Se marca el flag del job abortado. Se esperan las demas respuestas para bajar 1 en cantreducers de los nodos
			}
			if(respuestaReduce.resultado == 0){
//				printf("El reduce %s salio OK\n",respuestaReduce.archivoResultadoReduce);

				t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduce.ip_nodo,respuestaReduce.puerto_nodo);
				pthread_mutex_lock(&mutexModNodo);
				restarCantReducers(nodoARestar->nodo_id);
				pthread_mutex_unlock(&mutexModNodo);
				nuevoJob->reducePendientes--;
				estadoMarta();

			}
		}

		if(jobAbortado==1){
			memset(nuevoJob->estado,'\0',10);
			strcpy(nuevoJob->estado,"Fallido");
			nuevoJob->reducePendientes--;

			estadoMarta();

			//Si entra acá significa que salio mal algun reduce
			char jobAborta[BUF_SIZE];
			memset(jobAborta,'\0',BUF_SIZE);
			strcpy(jobAborta,"aborta");
			if(send(*socketJob,jobAborta,sizeof(jobAborta),MSG_WAITALL)==-1){
				log_error(logger,"Fallo el envío al Job de que aborte por falla de un reduce");
			}
			close(*socketJob);
			pthread_exit((void*)0);
		}

		//Si llega acá, salieron todos los reduce parciales bien//

//		printf("Llegaron las respuesta de los reduce --> Se envia reduce final\n");
		estadoMarta();

		/*************************** R E D U C E   F I N A L **********************************************/

		sleep(2);
		//Buscar el nodo con menos carga para asignar a hacer reduce final
		int posDistintos;
		t_list* listaMismoNodo = list_create(); //Lista para buscar el nodo con menos carga
		t_nodo* nodoGeneral;
		for(posDistintos=0;posDistintos<list_size(listaNodosDistintos);posDistintos++){
			char* nodoDistinto = list_get(listaNodosDistintos,posDistintos);
			nodoGeneral = traerNodo(nodoDistinto);
			list_add(listaMismoNodo,nodoGeneral);
		}
		// Ordenamos la sublista segun la suma de la cantidad de map y reduce
		list_sort(listaMismoNodo, (void*) ordenarSegunMapYReduce);
		t_nodo *nodoRF; //Nodo que se va a asignar para hacer el reduce final
		nodoRF = list_get(listaMismoNodo,0); // Nos traemos el nodo con menos carga

		//Inicializo estructura del nodo que voy a mandarle a JOB
		t_reduce nodoReduceFinal; //Nodo con los datos que se va a mandar a JOB
		memset(nodoReduceFinal.ip_nodoPpal,'\0',20);
		memset(nodoReduceFinal.nombreArchivoFinal,'\0',TAM_NOMFINAL);

		//Completo estructura del nodo que le voy a mandar a JOB
		strcpy(nodoReduceFinal.ip_nodoPpal,nodoRF->ip);
		nodoReduceFinal.puerto_nodoPpal = nodoRF->puerto_escucha_nodo;

		char* tiempoReduce=string_new();
		char** arrayTiempoReduce;
		char* archivoReduceTemp=string_new();

		string_append(&archivoReduceTemp,"/tmp/Job");
		string_append(&archivoReduceTemp,stringNroJob);
		arrayTiempoReduce=string_split(temporal_get_string_time(),":"); //creo array con hora minutos segundos y milisegundos separados
		string_append(&tiempoReduce,arrayTiempoReduce[0]);//Agrego horas
		string_append(&tiempoReduce,arrayTiempoReduce[1]);//Agrego minutos
		string_append(&tiempoReduce,arrayTiempoReduce[2]);//Agrego segundos
		string_append(&tiempoReduce,arrayTiempoReduce[3]);//Agrego milisegundos
		string_append(&archivoReduceTemp,"_ReduceFinal_");
		string_append(&archivoReduceTemp,tiempoReduce);
		string_append(&archivoReduceTemp,".txt");

		strcpy(nodoReduceFinal.nombreArchivoFinal,archivoReduceTemp);

		// Mando a ejecutar reduce
		char mensajeReducerFinal[BUF_SIZE];
		memset(mensajeReducerFinal, '\0', BUF_SIZE);
		strcpy(mensajeReducerFinal,"ejecuta reduce");
		if(send(*socketJob, mensajeReducerFinal,sizeof(mensajeReducerFinal),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger, "Fallo mandar hacer reducer");
			//exit(-1);
		}

		if(send(*socketJob, &nodoReduceFinal,sizeof(t_reduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger, "Fallo mandar datos para hacer reducer");
			//exit(-1);
		}

		// Mando cantidad de archivos a hacer reduce
		int cantArch = list_size(listaMismoNodo);
		if(send(*socketJob, &cantArch,sizeof(int),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger, "Fallo mandar la cantidad de archivos a hacer reduce");
			//exit(-1);
		}
		/*Le mando los datos de cada uno de los archivos de la lista en la que se aplicó reduce parcial(listaReducerParcial):
		IP Nodo, Puerto Nodo, nombreArchivo resultado de map (t_archivosReduce)*/
		int posNodoOk;
		t_reduce *nodoOk;
		for(posNodoOk = 0; posNodoOk < list_size(listaReducerParcial); posNodoOk ++){
			t_archivosReduce archivosReduceFinal;
			memset(archivosReduceFinal.archivoAAplicarReduce,'\0',TAM_NOMFINAL);
			memset(archivosReduceFinal.ip_nodo,'\0',20);

			nodoOk = list_get(listaReducerParcial,posNodoOk);
			strcpy(archivosReduceFinal.archivoAAplicarReduce, nodoOk->nombreArchivoFinal);
			strcpy(archivosReduceFinal.ip_nodo, nodoOk->ip_nodoPpal);
			archivosReduceFinal.puerto_nodo = nodoOk->puerto_nodoPpal;

			// Mando por cada t_reduce , los datos de cada archivo
			if(send(*socketJob, &archivosReduceFinal,sizeof(t_archivosReduce),MSG_WAITALL)==-1){
				perror("send");
				log_error(logger, "Fallo mandar la archivo a hacer reduce");
				//exit(-1);
			}
		}
		/*Le mando los datos de cada uno de los archivos de la listaReducerDeUnSoloArchivo:
		 * IP Nodo, Puerto Nodo, nombreArchivo resultado de map (t_archivosReduce) */
//		for(posNodoOk = 0; posNodoOk < list_size(listaReducerDeUnSoloArchivo); posNodoOk ++){
//			t_archivosReduce archivosReduceFinal;
//			memset(archivosReduceFinal.archivoAAplicarReduce,'\0',TAM_NOMFINAL);
//			memset(archivosReduceFinal.ip_nodo,'\0',20);
//
//			nodoOk = list_get(listaReducerDeUnSoloArchivo,posNodoOk);
//			strcpy(archivosReduceFinal.archivoAAplicarReduce, nodoOk->nombreArchivoFinal);
//			strcpy(archivosReduceFinal.ip_nodo, nodoOk->ip_nodoPpal);
//			archivosReduceFinal.puerto_nodo = nodoOk->puerto_nodoPpal;
//
//			// Mando por cada t_reduce , los datos de cada archivo
//			if(send(*socketJob, &archivosReduceFinal,sizeof(t_archivosReduce),MSG_WAITALL)==-1){
//				perror("send");
//				log_error(logger, "Fallo mandar la archivo a hacer reduce");
//				//exit(-1);
//			}
//		}

		pthread_mutex_lock(&mutexModNodo);
		sumarCantReducers(nodoRF->nodo_id);
		pthread_mutex_unlock(&mutexModNodo);
		//nuevoJob->reducePendientes++;

//		printf("Se envio reduce final --> Se espera la respuesta\n");
		estadoMarta();


		//ACA RECIBIRIA LA RESPUESTA, A CONTINUACION DE ENVIAR EL ULTIMO REDUCE, PORQUE ES UNA SOLA//
		t_respuestaReduce respuestaReduceFinal;
		if(recv(*socketJob,&respuestaReduceFinal, sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("recv");
			log_error(logger,"Fallo al recibir el ok del job");
			//exit(-1);
		}

		if(respuestaReduceFinal.resultado == 1){
			memset(nuevoJob->estado,'\0',10);
			strcpy(nuevoJob->estado,"Fallido");

			//Se aborta la ejecución de Reduce
			t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduceFinal.ip_nodo,respuestaReduceFinal.puerto_nodo);
			char jobAborta[BUF_SIZE];
			memset(jobAborta,'\0',BUF_SIZE);
			log_info(logger,"Falló el Reduce %s. Se abortará el job",respuestaReduceFinal.archivoResultadoReduce);
			strcpy(jobAborta,"aborta");
			if(send(*socketJob,jobAborta,sizeof(jobAborta),MSG_WAITALL)==-1){
				log_error(logger,"Fallo el envío al Job de que aborte por falla de un reduce");
			}
			pthread_mutex_lock(&mutexModNodo);
			restarCantReducers(nodoARestar->nodo_id);
			pthread_mutex_unlock(&mutexModNodo);
			nuevoJob->reducePendientes--;
			estadoMarta();

			close(*socketJob);
			pthread_exit((void*)0);
		}
		if(respuestaReduceFinal.resultado == 0){
			t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduceFinal.ip_nodo,respuestaReduceFinal.puerto_nodo);
//			printf("En el nodo %s se ejecutó reduce con combiner\n", nodoARestar->nodo_id);
//			printf("El estado del nodo %s es habilitado\n", nodoARestar->nodo_id);
//			printf("Reduce %s exitoso\n",respuestaReduceFinal.archivoResultadoReduce);
			pthread_mutex_lock(&mutexModNodo);
			restarCantReducers(nodoARestar->nodo_id);
			pthread_mutex_unlock(&mutexModNodo);
			nuevoJob->reducePendientes--;

		}

//		printf("Termino el reduce con combiner\n");
		log_info(logger,"El job %d termino OK.",nuevoJob->nroJob);

		memset(nuevoJob->estado,'\0',10);
		strcpy(nuevoJob->estado,"Exitoso");
		estadoMarta();

		//Si llega hasta acá, el Job con combiner termino OK
		//Le digo al Job que finalice
		char finaliza[BUF_SIZE];
		memset(finaliza,'\0',BUF_SIZE);
		strcpy(finaliza,"finaliza");
		if(send(*socketJob,finaliza,sizeof(finaliza),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo al enviarle al Job que finalize");
		}

		close(*socketJob);

		//Buscar el nodo donde está el archivo resultado del reduce para mandarle al FS el nodoID
		t_nodo* nodoResultadoCC;
		nodoResultadoCC = buscarNodoPorIPYPuerto(nodoReduceFinal.ip_nodoPpal,nodoReduceFinal.puerto_nodoPpal);

		//Le digo al FS que se copie el resultado
//		printf("El job con combiner termino OK\nMandar a FS que busque el resultado %s en el nodo %s\n",nodoReduceFinal.nombreArchivoFinal,nodoResultadoCC->nodo_id);

		char** nombreResSpliteado=string_split(nodoReduceFinal.nombreArchivoFinal,"/");
		char nombreRes[TAM_NOMFINAL];
		memset(nombreRes,'\0',TAM_NOMFINAL);
		strcpy(nombreRes,nombreResSpliteado[1]);

		//Le aviso a FS que le voy a mandar el Nodo ID
		memset(mensaje_fs,'\0',BUF_SIZE);
		strcpy(mensaje_fs, "resultado");
		if(send(socket_fs,mensaje_fs, sizeof(mensaje_fs), MSG_WAITALL )==-1){
			perror("send");
			log_error(logger,"Fallo el envío del mensaje");
			//exit(-1);
		}
		//Le mando a FS el Nodo ID
		if(send(socket_fs,nodoResultadoCC->nodo_id,sizeof(nodoResultadoCC->nodo_id),MSG_WAITALL) == -1) {
			perror("send");
			log_error(logger,"Fallo el envio del archivo resultado del reduce con combiner al FS");
			//exit(-1);
		}

		//Le mando el archivo Resultado de reduce
		if(send(socket_fs,nombreRes,sizeof(nombreRes),MSG_WAITALL) == -1) {
			perror("send");
			log_error(logger,"Fallo el envio del archivo resultado del reduce con combiner al FS");
			//exit(-1);
		}
		//Le mando al FS donde debe guardar el resultado
		if(send(socket_fs,archivoResultado,sizeof(archivoResultado),MSG_WAITALL) == -1) {
			perror("send");
			log_error(logger,"Fallo el envio del archivo resultado del reduce con combiner al FS");
			//exit(-1);
		}
	}
	pthread_exit((void*)0);
}

bool nodoIdMasRepetido (char* antNodoId, char* posNodoId){
	bool esVerdadero;
	if(strcmp(antNodoId,posNodoId)==0){
		esVerdadero = true;
	}
	return esVerdadero;
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
			break;
		}
	}
	return bloques;
}


//static void eliminarCopiasNodo(t_list *self){
//	free(self);
//}

//Buscamos los nodos de la lista global en los que esta cada copia
t_nodo* buscarCopiaEnNodos(t_copias *copia){
	int posNodo;
	int cantNodos;
	t_nodo *nodo;
	t_nodo *nodoAux;
	cantNodos = list_size(listaNodos);
	for(posNodo=0; posNodo<cantNodos; posNodo++){ //Recorremos la lista de nodos global
		nodo = list_get(listaNodos,posNodo);
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

void sumarCantMapper(char* idNodoASumar){
	int tamanio=list_size(listaNodos);
	int indice=0;
	t_nodo* unNodo;
	for(indice=0;indice<tamanio;indice++){
		unNodo=list_get(listaNodos,indice);
		if(strcmp(unNodo->nodo_id,idNodoASumar)==0){
			unNodo->cantMappers++;
		}
	}
}

void restarCantMapper(char* idNodoARestar){
	int tamanio=list_size(listaNodos);
	int indice=0;
	t_nodo* unNodo;
	for(indice=0;indice<tamanio;indice++){
		unNodo=list_get(listaNodos,indice);
		if(strcmp(unNodo->nodo_id,idNodoARestar)==0){
			unNodo->cantMappers--;
		}
	}
}

void sumarCantReducers(char* idNodoASumar){
	int tamanio=list_size(listaNodos);
	int indice=0;
	t_nodo* unNodo;
	for(indice=0;indice<tamanio;indice++){
		unNodo=list_get(listaNodos,indice);
		if(strcmp(unNodo->nodo_id,idNodoASumar)==0){
			unNodo->cantReducers++;
		}
	}
}

void restarCantReducers(char* idNodoARestar){
	int tamanio=list_size(listaNodos);
	int indice=0;
	t_nodo* unNodo;
	for(indice=0;indice<tamanio;indice++){
		unNodo=list_get(listaNodos,indice);
		if(strcmp(unNodo->nodo_id,idNodoARestar)==0){
			unNodo->cantReducers--;
		}
	}
}



t_archivo* buscarArchivo(char* nombreArchivo, int padre){
	t_archivo *archivoAux;
	t_archivo *archivoEncontrado;
	int i;
	for(i=0; i < list_size(listaArchivos); i++){ //recorre la lista global de archivos
		archivoAux = list_get(listaArchivos,i);
		if (strcmp(archivoAux->nombre,nombreArchivo) ==0 && archivoAux->padre==padre){ //compara el nomnre y padre del archivo del job con cada nombre y padre de archivo de la lista global
			archivoEncontrado = archivoAux;
		}
	}
	return archivoEncontrado;
}



bool archivoDisponible(t_archivo* archivo){
	int indice=0;
	t_bloque* bloqueDelArchivo;
	for(indice=0;indice<list_size(archivo->bloques);indice++){
		bloqueDelArchivo=list_get(archivo->bloques,indice);
		if(list_all_satisfy(bloqueDelArchivo->copias,(void*) nodoNoDisponible)){
			return false;
		}
	}
	return true;
}

bool nodoNoDisponible(t_copias* copia){
	t_nodo* nodoDeListaGeneral;
	int indice;
	bool resultado;
	for(indice=0;indice<list_size(listaNodos);indice++){
		nodoDeListaGeneral=list_get(listaNodos,indice);
		if(strcmp(nodoDeListaGeneral->nodo_id,copia->nodo)==0){
			if(nodoDeListaGeneral->estado==0){
				return true;
			}
			if(nodoDeListaGeneral->estado==1){
				 resultado = false;
			}
		}
	}
	return resultado;
}

t_nodo* traerNodo(char* nodoId){
	int indice;
	t_nodo* nodoAux;
	for(indice=0;indice<list_size(listaNodos);indice++){
		nodoAux=list_get(listaNodos,indice);
		if (strcmp(nodoAux->nodo_id,nodoId)==0){
			return nodoAux;
		}
	}
	return NULL;
}

t_nodo* buscarNodoPorIPYPuerto(char* ipNodo,int puertoNodo){
	int i;
	t_nodo *nodoAux;
	for(i=0;i<list_size(listaNodos);i++){
		nodoAux= list_get(listaNodos,i);
		if ((strcmp(nodoAux->ip,ipNodo)==0)&&(nodoAux->puerto_escucha_nodo == puertoNodo)){
			return nodoAux;
		}
	}
	return NULL;
}

int cantidadTotalDeBloques(char* archivosJob){
	// Separo el mensaje que recibo con los archivos a trabajar (Job envía todos juntos separados con ,)
	char** archivos =string_split((char*)archivosJob,",");
	char **arrayArchivo;
	int cantBloques;
	int posArchivo;
	char nombreArchivo[TAM_NOMFINAL];
	memset(nombreArchivo,'\0',TAM_NOMFINAL);
	int cantBloquesTotales = 0;
	//recorrer los archivos
	for(posArchivo=0; archivos[posArchivo]!=NULL;posArchivo++){
		//Separo el nombre del archivo por barras
		int posArray;
		arrayArchivo = string_split(archivos[posArchivo], "/");
		for(posArray=0;arrayArchivo[posArray]!=NULL;posArray++){
			if(arrayArchivo[posArray+1]==NULL){
				//me quedo con el nombre del archivo
				strcpy(nombreArchivo,arrayArchivo[posArray]);
			}
		}
		t_list* bloques;
		bloques=buscarBloquesTotales(nombreArchivo);
		cantBloques = list_size(bloques);
		int cantBloquesTotales=0;
		cantBloquesTotales = cantBloquesTotales + cantBloques;
	}
	return cantBloquesTotales;
}

t_list * buscarBloquesTotales(char* nombreArchivo){
	t_archivo *archivoAux;
	t_list *bloques;
	int i;
	for(i=0; i < list_size(listaArchivos); i++){ //recorre la lista global de archivos
		archivoAux = list_get(listaArchivos,i);
		if (strcmp(archivoAux->nombre,nombreArchivo) ==0){ //compara el nomnre del archivo del job con cada nombre de archivo de la lista global
			bloques = archivoAux->bloques;
			break;
		}
	}
	return bloques;
}

void estadoNodos(){
	int tamanio,indice;
	t_nodo* unNodo;
	tamanio=list_size(listaNodos);
	printf("#### ESTADO DE LOS NODOS ####\n\n");
	for(indice=0;indice<tamanio;indice++){
		unNodo=list_get(listaNodos,indice);
		printf("El %s tiene:\n\t%d mapper en curso\n\t%d reducers en curso\n\tEstado: %d\n\n",unNodo->nodo_id,unNodo->cantMappers,unNodo->cantReducers,unNodo->estado);
	}
	printf("#############################\n");
	return;
}

static void eliminarCopia(t_copias *self){
	free(self);
}

void estadoJobs(){
	int tamanio,indice;
	t_job* unJob;
	tamanio=list_size(listaJobs);
	printf("#### ESTADO DE LOS JOBS ####\n\n");
	for(indice=0;indice<tamanio;indice++){
		unJob=list_get(listaJobs,indice);
		printf("Job %d:\n\tArchivo Resultado: %s\n\tCombiner: %s\n\tEstado: %s\n\tMappers pendientes: %d\n\tReduce pendientes: %d\n\n",unJob->nroJob,unJob->resultadoDelJob,unJob->combiner,unJob->estado,unJob->mapperPendientes,unJob->reducePendientes);
	}
	printf("############################\n");
}


void estadoMarta(){
	estadoNodos();
	if(list_size(listaJobs)>0){
		estadoJobs();
	}
	else{
		printf("\n* No se lanzo ningún Job *\n");
	}
}

