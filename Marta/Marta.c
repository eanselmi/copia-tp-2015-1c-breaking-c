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
char identificacion[BUF_SIZE]; //para el mensaje que envie al conectarse para identificarse, puede cambiar
int nroJob; //Variable global utilizada por cada hilo Job para diferenciar el nombre de un resultado Map/Reduce
pthread_mutex_t mutexNroJob=PTHREAD_MUTEX_INITIALIZER; //Para que cada Job tenga su numero diferente utilizaran este mutex

int main(int argc, char**argv){

	pthread_t escucha_jobs;
	configurador= config_create("resources/martaConfig.conf");
	logger = log_create("./martaLog.log", "Marta", true, LOG_LEVEL_INFO);
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
	char nombreArchivo[200];
	uint32_t padreArchivo;
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
//		archivo1->nombre= string_new();
//		archivo2->nombre= string_new();
//		archivo3->nombre= string_new();
//
//		string_append(&archivo1->nombre, "pam");
//		string_append(&archivo2->nombre, "bruno");
//		string_append(&archivo3->nombre, "archivoTemperatura1.txt");
//		archivo1->padre=1;
//		archivo2->padre=1;
//		archivo3->padre=1;
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

	printf ("\n\nLista de nodos recibida");
	printf ("\n=======================\n\n");
	int iii, n_nodos;
	t_nodo *elemento=malloc(sizeof(t_nodo));
	n_nodos = list_size(listaNodos);
	for (iii = 0; iii < n_nodos; iii++) {
		elemento = list_get(listaNodos, iii);
		printf("\n\n");
		printf("Nodo_ID: %s\nEstado: %d\nIP: %s\nPuerto de Escucha: %d\n",elemento->nodo_id, elemento->estado, elemento->ip,elemento->puerto_escucha_nodo);
		printf("Cantidad de mappers:%d\n",elemento->cantMappers);
		printf("Cantidad de reducers:%d\n",elemento->cantReducers);
		printf("\n");
	}

//================================== FIN DEL ENVIO DE LA LISTA DE NODOS DEL FS =================================================



//=================================== RECIBO LA LISTA DE ARCHIVOS QUE TIENE EL FS ==============================================

	if ((nbytes = recv(socket_fs, &cantArchivos, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
		perror("recv");
		log_error(logger,"FALLO el Recv de cantidad de archivos");
		exit(-1);
	}
	printf ("Cantidad de archivos a recibir: %d\n",cantArchivos);
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
		printf ("...Nombre Archivo: %s\n",nombreArchivo);
		//pathArchivo=string_new(); //no enviamos path por ahora
		if ((nbytes = recv(socket_fs, &padreArchivo, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv del padre del archivo");
			exit(-1);
		}
		printf ("...Padre del archivo: %d\n",padreArchivo);
		archivoTemporal->nombre=string_new();
		strcpy(archivoTemporal->nombre, nombreArchivo);
		//strcpy(archivoTemporal->path, pathArchivo);
		archivoTemporal->padre = padreArchivo;

		if ((nbytes = recv(socket_fs, &cantidadBloquesArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
			perror("recv");
			log_error(logger,"FALLO el Recv de cantidad de bloques del archivo");
			exit(-1);
		}
		printf ("...Cantidad de bloques del archivo: %d\n",cantidadBloquesArchivo);
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
			printf ("... ...Cantidad de copias del bloque:%d\n",cantidadCopiasArchivo);
			l=0;
			while (l < cantidadCopiasArchivo){
				t_copias* copiaBloqueTemporal = malloc(sizeof(t_copias));
				memset(nodoIdArchivo,'\0',6);
				if ((nbytes = recv(socket_fs, nodoIdArchivo, sizeof(nodoIdArchivo), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
					perror("recv");
					log_error(logger,"FALLO el Recv del nodo de la copia del archivo");
					exit(-1);
				}
				printf ("... ... ...Nodo ID: %s\n",nodoIdArchivo);
				if ((nbytes = recv(socket_fs, &bloqueNodoArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
					perror("recv");
					log_error(logger,"FALLO el Recv del bloque del nodo donde está el archivo");
					exit(-1);
				}
				printf ("... ... ...Bloque Nodo: %d\n",bloqueNodoArchivo);
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
		t_archivo *archi;
		t_bloque *bloque;
		t_copias *copia;
		cant_archivos = list_size(listaArchivos);
		if (cant_archivos==0){
			printf ("No hay archivos cargados en MDFS\n");
		}
		for (ii = 0; ii < cant_archivos; ii++) {
			archi = list_get(listaArchivos, ii);
			printf("\n\n");
			printf("Archivo: %s\nPadre: %d\n",archi->nombre,archi->padre);
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



		//TEST PARA NOTIFICAR AL FS QUE BUSQUE UN ARCHIVO RESULTADO DE UN REDUCE
		/*memset(identificacion,'\0',BUF_SIZE);
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
		char test2[100];
		memset(test2, '\0', 100);
		strcpy(test2,"alton.txt");
		if((send(socket_fs,test2,sizeof(test2),MSG_WAITALL))==-1) {
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
						printf ("%s\n",identificacion);
						if (strcmp(identificacion,"marta_formatea")==0){
							printf ("Voy a formatear las estructuras por pedido del FS\n");
							//TODO limpiar estructuras de Marta
						}
						if (strcmp(identificacion,"elim_arch")==0){
							printf ("Voy a borrar un archivo de las estructuras\n");
							memset(nombreArchivoNovedad, '\0',200);
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad,	sizeof(nombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo a eliminar");
								exit(-1);
							}
							printf("...Nombre Archivo a eliminar: %s\n", nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del padre del archivo a eliminar");
								exit(-1);
							}
							printf ("...Padre del archivo a eliminar: %d\n",padreArchivoNovedad);
							//TODO limpiar estructuras del archivo
						}
						if (strcmp(identificacion,"renom_arch")==0){
							printf ("Voy a renombrar un archivo de las estructuras\n");
							memset(nombreArchivoNovedad, '\0',200);
							memset(nuevoNombreArchivoNovedad, '\0',200);
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad,	sizeof(nombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre viejo del archivo a renombrar");
								exit(-1);
							}
							printf("...Nombre Archivo a renombrar: %s\n", nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del padre del archivo a renombrar");
								exit(-1);
							}
							printf ("...Padre del archivo a renombrar: %d\n",padreArchivoNovedad);
							if ((nbytes = recv(socket_fs, nuevoNombreArchivoNovedad,	sizeof(nuevoNombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo a renombrar");
								exit(-1);
							}
							printf("...Nombre nuevo de archivo a renombrado: %s\n", nuevoNombreArchivoNovedad);
							//TODO modificar el nombre del archivo
						}
						if (strcmp(identificacion,"mov_arch")==0){
							printf ("Voy a mover un archivo de las estructuras\n");
							memset(nombreArchivoNovedad, '\0',200);
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad,	sizeof(nombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo a mover");
								exit(-1);
							}
							printf("...Nombre Archivo a mover: %s\n", nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del viejo padre del archivo");
								exit(-1);
							}
							printf("...Viejo padre del archivo: %d\n", padreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &nuevoPadreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nuevo padre del archivo");
								exit(-1);
							}
							printf("...Nuevo padre del archivo: %d\n", nuevoPadreArchivoNovedad);
							//TODO modificar el padre del archivo
						}
						if (strcmp(identificacion,"nuevo_arch")==0){
							printf ("Voy a agregar un nuevo archivo a las estructuras\n");
							//TODO revisar recv de nuevo archivo
							t_archivo* nuevoArchivo = malloc(sizeof(t_archivo));
							memset(nombreArchivoNovedad,'\0',200);
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad, sizeof(nombreArchivoNovedad), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo novedad");
								exit(-1);
							}
							printf ("...Nombre Archivo: %s\n",nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del padre del archivo");
								exit(-1);
							}
							printf ("...Padre del archivo: %d\n",padreArchivoNovedad);
							nuevoArchivo->nombre=string_new();
							strcpy(nuevoArchivo->nombre, nombreArchivoNovedad);
							nuevoArchivo->padre = padreArchivoNovedad;
							if ((nbytes = recv(socket_fs, &cantidadBloquesArchivo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv de cantidad de bloques del archivo");
								exit(-1);
							}
							printf ("...Cantidad de bloques del archivo: %d\n",cantidadBloquesArchivo);
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
								printf ("... ...Cantidad de copias del bloque:%d\n",cantCopiasArchivoNovedad);
								b=0;
								while (b < cantCopiasArchivoNovedad){
									t_copias* copiaBloqueNovedad = malloc(sizeof(t_copias));
									memset(nodoId,'\0',6);
									if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
										perror("recv");
										log_error(logger,"FALLO el Recv del nodo de la copia del archivo");
										exit(-1);
									}
									printf ("... ... ...Nodo ID: %s\n",nodoId);
									if ((nbytes = recv(socket_fs, &bloqueNodo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
										perror("recv");
										log_error(logger,"FALLO el Recv del bloque del nodo donde está el archivo");
										exit(-1);
									}
									printf ("... ... ...Bloque Nodo: %d\n",bloqueNodo);
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

							//listar para ver si se agregó bien a la lista
							int iu,ju,ku, cant_archivosu,cant_bloquesu,cant_copiasu;
							t_archivo *archiu;
							t_bloque *bloqueu;
							t_copias *copiau;
							cant_archivosu = list_size(listaArchivos);
							if (cant_archivosu==0){
								printf ("No hay archivos cargados en MDFS\n");
							}
							for (iu = 0; iu < cant_archivosu; iu++) {
								archiu = list_get(listaArchivos, iu);
								printf("\n\n");
								printf("Archivo: %s\nPadre: %d\n",archiu->nombre,archiu->padre);
								printf("\n");
								cant_bloquesu=list_size(archiu->bloques);
								for (ju = 0; ju < cant_bloquesu; ju++){
									bloqueu=list_get(archiu->bloques,ju);
									printf ("Numero de bloque: %d\n",ju);
									cant_copiasu=list_size(bloqueu->copias);
									for (ku=0;ku<cant_copiasu;ku++){
										copiau=list_get(bloqueu->copias,ku);
										printf ("Copia %d del bloque %d\n",ku,ju);
										printf ("----------------------\n");
										printf ("	Nodo: %s\n	Bloque: %d\n\n",copiau->nodo,copiau->bloqueNodo);
									}
								}
							}



						}
						//fin update nuevo_arch

						if (strcmp(identificacion,"elim_bloque")==0){
							printf ("Voy a eliminar una copia de un bloque de un archivo a las estructuras\n");
							memset(nombreArchivoNovedad, '\0',200);
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad,	sizeof(nombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo del borrar bloque");
								exit(-1);
							}
							printf("...Nombre del archivo del borrar bloque: %s\n", nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del padre del archivo del borrar bloque");
								exit(-1);
							}
							printf ("...Padre del archivo del borrar bloque: %d\n",padreArchivoNovedad);
							memset(nodoId,'\0',6);
							if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nodo del borrar bloque");
								exit(-1);
							}
							printf("...Nodo del archivo del borrar bloque: %s\n", nodoId);
							if ((nbytes = recv(socket_fs, &bloqueNodo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del bloque del nodo del borrar bloque");
								exit(-1);
							}
							printf ("...Bloque del borrar bloque: %d\n",bloqueNodo);
							//TODO borrar bloque
						}
						if (strcmp(identificacion,"nuevo_bloque")==0){
							printf ("Voy a agregar una nueva copia de un bloque de un archivo a las estructuras\n");
							if ((nbytes = recv(socket_fs, nombreArchivoNovedad,	sizeof(nombreArchivoNovedad), MSG_WAITALL))	< 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nombre del archivo del copiar bloque");
								exit(-1);
							}
							printf("...Nombre del archivo del borrar bloque: %s\n", nombreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &padreArchivoNovedad, sizeof(uint32_t), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del padre del archivo del copiar bloque");
								exit(-1);
							}
							printf ("...Padre del archivo del borrar bloque: %d\n",padreArchivoNovedad);
							if ((nbytes = recv(socket_fs, &bloqueNodo, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del bloque del nodo del copiar bloque");
								exit(-1);
							}
							printf ("...Bloque del borrar bloque: %d\n",bloqueNodo);
							memset(nodoId,'\0',6);
							if ((nbytes = recv(socket_fs, nodoId, sizeof(nodoId), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del nodo del copiar bloque");
								exit(-1);
							}
							printf("...Nodo del archivo del borrar bloque: %s\n", nodoId);
							if ((nbytes = recv(socket_fs, &bloqueNodoDestino, sizeof(int), MSG_WAITALL)) < 0) { //si entra aca es porque hubo un error
								perror("recv");
								log_error(logger,"FALLO el Recv del bloque del nodo destino del copiar bloque");
								exit(-1);
							}
							printf ("...Bloque del borrar bloque: %d\n",bloqueNodoDestino);
							//TODO agregar bloque a la estructura de archivo

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
	t_list* listaReducerDeUnSoloArchivo;
	t_list* listaReducerParcial;
	memset(mensaje_fs,'\0',BUF_SIZE);
	memset(accion,'\0',BUF_SIZE);
	listaMappers = list_create();
	listaReducerDeUnSoloArchivo=list_create();
	listaReducerParcial=list_create();
	char archivoResultado[TAM_NOMFINAL];
	int posicionArchivo;
	int nroRespuesta;
	char mensajeCombiner[3];
	char archivosDelJob[MENSAJE_SIZE];
	memset(mensajeCombiner, '\0', 3);
	memset(archivosDelJob, '\0', MENSAJE_SIZE);
	memset(archivoResultado,'\0', TAM_NOMFINAL);
	char* stringNroJob=string_new();

	//Mediante un mutex asigno al string "stringNroJob" el numero actual de nroJob y le sumo 1
	pthread_mutex_lock(&mutexNroJob);
	string_append(&stringNroJob,string_itoa(nroJob));
	nroJob++;
	pthread_mutex_unlock(&mutexNroJob);


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

	//Lo siguiente es para probar que efectivamente se reciba la lista de archivos
	char nombreArchivo[TAM_NOMFINAL];
	memset(nombreArchivo,'\0',TAM_NOMFINAL);
	for(posicionArchivo=0;archivos[posicionArchivo]!=NULL;posicionArchivo++){
		int posArray;
		int cantBloques;
		int posBloques;
		int padre;
		char archivoAPedirPadre[TAM_NOMFINAL];
		char** arrayArchivo;
		t_list *bloques;

		memset(archivoAPedirPadre,'\0',TAM_NOMFINAL);

		printf("Se debe trabajar en el archivo:%s\n",archivos[posicionArchivo]);
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

		//De cada archivo que nos manda el Job buscamos y nos traemos los bloques

		bloques=buscarBloques(nombreArchivo,padre);
		cantBloques = list_size(bloques);
		for(posBloques=0; posBloques<cantBloques; posBloques++){ //recorremos los bloques del archivo que nos mando job

			//Si el archivo no está disponible, no se hace el Job
			if(!archivoDisponible(buscarArchivo(nombreArchivo,padre))){
				char msjJob[BUF_SIZE];
				memset(msjJob,'\0',BUF_SIZE);
				strcpy(msjJob,"arch no disp");
				log_info(logger,"El archivo no está disponible, no se podrá hacer el Job");
				if(send(*socketJob,msjJob,sizeof(msjJob),MSG_WAITALL)==-1){
					perror("Send");
					log_error(logger,"Fallo el envio del mensaje \"archivo no disponible\" al job");
				}
				pthread_exit((void*)0);
			}


			int cantCopias;
			t_bloque *bloque;
			t_nodo *nodoAux;
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
			sumarCantMapper(nodoAux->nodo_id);

			free(nombreArchivoTemp);
			free(pathArchivoTemp);
//			free(tiempo);

			list_destroy(copiasNodo);
		}
	}

	//SE ESPERAN EN UN CICLO FOR LA CANTIDAD DE RESPUESTAS IGUAL A CUANTOS MAPPER SE HALLAN ENVIADO (con tamaño de la listaMappers)

	for(nroRespuesta=0;nroRespuesta<list_size(listaMappers);nroRespuesta++){

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
			printf("El map %s falló\n",map->archivoResultadoMap);
			t_list *nodosQueFallaron;
			nodosQueFallaron=list_create();
			int posRepl;
			int posCopia;
			int cantidadCopias;
			t_list *bloquesNoMap;
			t_bloque *bloqueQueFallo;
			t_copias* copiaBloque;
			t_nodo *nodoCopia;
			t_list* nodoSinMap;
			t_replanificarMap *mapperEnviado;
			nodoSinMap=list_create();
			//Recorro la lista de t_replanificarMap para buscar los que fallaron y los guardo en una lista para no volver a asignarlos
			for(posRepl=0;posRepl<list_size(listaMappers);posRepl++){
				mapperEnviado = list_get(listaMappers,posRepl);
				if((strcmp(mapperEnviado->nombreArchivoDelJob,map->nombreArchivoDelJob)==0)
						&&(mapperEnviado->padreArchivoJob==map->padreArchivoJob)&&(mapperEnviado->bloqueArchivo==map->bloqueArchivo)
						&&(mapperEnviado->resultado==1)){
					char* idNodoANoConsiderar=string_new();
					string_append(&idNodoANoConsiderar,mapperEnviado->nodoId);
					list_add(nodosQueFallaron,idNodoANoConsiderar);

				}
			}


			//buscamos en la lista general de archivos los bloques del map que fallo
			bloquesNoMap = buscarBloques(map->nombreArchivoDelJob,map->padreArchivoJob);

			//Si el archivo no está disponible, no se hace el Job
			if(!archivoDisponible(buscarArchivo(map->nombreArchivoDelJob,map->padreArchivoJob))){
				char msjJob[BUF_SIZE];
				memset(msjJob,'\0',BUF_SIZE);
				strcpy(msjJob,"arch no disp");
				log_info(logger,"El archivo no está disponible, no se podrá hacer el Job");
				if(send(*socketJob,msjJob,sizeof(msjJob),MSG_WAITALL)==-1){
					perror("Send");
					log_error(logger,"Fallo el envio del mensaje \"archivo no disponible\" al job");
				}
				pthread_exit((void*)0);
			}

			//buscamos en los bloques el bloque en el que salio mal el MAP
			bloqueQueFallo = list_get(bloquesNoMap,map->bloqueArchivo);
			cantidadCopias = list_size(bloqueQueFallo->copias);

			int tamanioListaANoConsiderar=list_size(nodosQueFallaron);

			for(posCopia=0;posCopia<cantidadCopias;posCopia++){ // recorremos las copias del bloque que salio mal el MAP
				int estaEnListaParaNoConsiderar,indice;
				copiaBloque = list_get(bloqueQueFallo->copias,posCopia);
				// Nos traemos cada nodo en donde esta cada una de las copias del bloque que fallo
				nodoCopia= buscarCopiaEnNodos(copiaBloque);
				estaEnListaParaNoConsiderar=0;
				// recorremos la lista de nodos que fallaron
				for(indice=0;indice<tamanioListaANoConsiderar;indice++){
					char* nodo_id_acomparar;
					nodo_id_acomparar=list_get(nodosQueFallaron,indice);
					// por cada nodo que fallo lo comparo con un nodo de una copia del bloque que fallo
					if(strcmp(nodo_id_acomparar,nodoCopia->nodo_id)==0){
						estaEnListaParaNoConsiderar=1;
					}
				}
				//si esta el nodo de la copia activo y no era el fallado
				if((nodoCopia->estado == 1)&&(!estaEnListaParaNoConsiderar)){
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
			char **arrayNomArchivo=string_split(map->nombreArchivoDelJob,".");

			strcpy(pathArchivoRTemp,"/tmp/");
			string_append(&archivoReplanificadoTemp,stringNroJob);
			string_append(&archivoReplanificadoTemp,arrayNomArchivo[0]);
			arrayTiempoReplanificado=string_split(temporal_get_string_time(),":"); //creo array con hora minutos segundos y milisegundos separados
			string_append(&tiempoReplanificado,arrayTiempoReplanificado[0]);//Agrego horas
			string_append(&tiempoReplanificado,arrayTiempoReplanificado[1]);//Agrego minutos
			string_append(&tiempoReplanificado,arrayTiempoReplanificado[2]);//Agrego segundos
			string_append(&tiempoReplanificado,arrayTiempoReplanificado[3]);//Agrego milisegundos
			string_append(&archivoReplanificadoTemp,"_Bloq");
			string_append(&archivoReplanificadoTemp,string_itoa(map->bloqueArchivo));
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
			//rellenamos un nuevo struct t_replanificar map conlos datos del map que acabamos de enviar y lo agregamos a lista mappers
			t_replanificarMap *nuevoMap=malloc(sizeof(t_replanificarMap));

			nuevoMap->bloqueArchivo = map->bloqueArchivo ;
			memset(nuevoMap->nodoId,'\0',6);
			memset(nuevoMap->archivoResultadoMap,'\0',TAM_NOMFINAL);
			memset(nuevoMap->nombreArchivoDelJob,'\0',TAM_NOMFINAL);
			strcpy(nuevoMap->nombreArchivoDelJob,map->nombreArchivoDelJob);
			strcpy(nuevoMap->archivoResultadoMap,datosReplanificacionMap.archivoResultadoMap);
			char* nodoIdTemp=string_new();
			string_append(&nodoIdTemp,otroNodoAux->nodo_id);
			strcpy(nuevoMap->nodoId,nodoIdTemp);
			nuevoMap->resultado=2;
			nuevoMap->padreArchivoJob=map->padreArchivoJob;
			list_add(listaMappers,nuevoMap);
			// le resto cantMappers al nodo que le salio mal el MAP
			restarCantMapper(map->nodoId);
			// le sumo cantMappers al nodo que acabo de mandar a hacer MAP
			sumarCantMapper(nuevoMap->nodoId);

		}
		if(map->resultado==0){
			printf("El map %s salio ok\n",map->archivoResultadoMap);
			// buscar en la lista del struct el nodo_id y luego buscarlo en la lista gral de nodos y restarle 1 a su catMappers
			restarCantMapper(map->nodoId);
		}
	}

/************************************************************************************************************************************************
* SIN COMBINER
* ************************************************************************************************************************************************/

	//Si es sin combiner manda a hacer reduce al nodo que tenga mas archivos resultados MAP
	if(strcmp(mensajeCombiner, "NO")==0){
		int jobAbort=0;
		int posicionMapper;
		t_replanificarMap *mapperOk;
		t_list* listaNodosMapperOk;
		listaNodosMapperOk = list_create();
		//Recorro lista de mappers y por cada mapperOk con resultado 0 que es map ok, me lo guardo  en una lista
		for (posicionMapper =0; posicionMapper < list_size(listaMappers); posicionMapper++){
			mapperOk = list_get(listaMappers, posicionMapper);
			if(mapperOk->resultado == 0){
				list_add(listaNodosMapperOk,mapperOk);
			}

		}
		int posicionNodo;
		int cantNodosMapOk;
		cantNodosMapOk = list_size(listaNodosMapperOk);
		int mayorRepetido = 0;
		int cantNodoRepetido;
		t_replanificarMap *nodoMasRepetido;
		t_replanificarMap *mapOk;
		//recorro la lista de mappers ok y por cada uno saco la cantidad de veces que esta repetido, es decir donde se hicieron mas Maps locales
		//y la asigno como mayor
		for(posicionNodo = 0; posicionNodo < cantNodosMapOk; posicionNodo++){
			mapOk = list_get(listaNodosMapperOk, posicionNodo);
			cantNodoRepetido = list_count_satisfying(listaNodosMapperOk,(void*) nodoIdMasRepetido);
			if(cantNodoRepetido > mayorRepetido){
				mayorRepetido = cantNodoRepetido;
				//Del que se hicieron mas MAPS locales me lo guardo
				nodoMasRepetido = mapOk;


			}
		}
		int posNG;
		t_nodo * nodoG;
		t_reduce nodoReducer;
		for(posNG=0; posNG < list_size(listaNodos); posNG++){
			nodoG = list_get(listaNodos, posNG);
			if(strcmp(nodoMasRepetido->nodoId, nodoG->nodo_id)==0){
				strcpy(nodoReducer.ip_nodoPpal, nodoG->ip);
				nodoReducer.puerto_nodoPpal = nodoG->puerto_escucha_nodo;
			}

		}
		//Completo estructura del nodo que le voy a mandar a JOB
		strcpy(nodoReducer.ip_nodoPpal,nodoG->ip);
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
		string_append(&archivoReduceTemp,"_ReduceSinCombiner_");
		string_append(&archivoReduceTemp,tiempoReduce);
		string_append(&archivoReduceTemp,".txt");

		strcpy(nodoReducer.nombreArchivoFinal,archivoReduceTemp);

		// Mando a ejecutar reduce
		char mensajeReducer[TAM_NOMFINAL];
		memset(mensajeReducer, '\0', TAM_NOMFINAL);
		strcpy(mensajeReducer,"ejecuta reduce");
		if(send(*socketJob, mensajeReducer,sizeof(TAM_NOMFINAL),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger, "Fallo mandar hacer reducer");
		//exit(-1);
		}
		if(send(*socketJob, &nodoReducer,sizeof(t_reduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger, "Fallo mandar datos para hacer reducer");
			//exit(-1);
		}
		// Mando cantidad de archivos a hacer reduce
		if(send(*socketJob, &cantNodosMapOk,sizeof(int),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger, "Fallo mandar la cantidad de archivos a hacer reduce");
			//exit(-1);
		}
		int posicionNodoOk;
		t_replanificarMap *nodoOk;
		t_archivosReduce archReduce;
		memset(archReduce.archivoAAplicarReduce,'\0',TAM_NOMFINAL);
		for(posicionNodoOk = 0; posicionNodoOk < cantNodosMapOk; posicionNodoOk ++){
			nodoOk = list_get(listaNodosMapperOk,posicionNodoOk);
			strcpy(archReduce.archivoAAplicarReduce, nodoOk->archivoResultadoMap);
			int posNodoGlobal;
			t_nodo *nodoGlobal;
			for(posNodoGlobal = 0; posNodoGlobal < list_size(listaNodos);posNodoGlobal ++){
				nodoGlobal = list_get(listaNodos,posNodoGlobal);
				if(strcmp(nodoOk->nodoId, nodoGlobal->nodo_id)==0){
					strcpy(archReduce.ip_nodo, nodoGlobal->ip);
					archReduce.puerto_nodo = nodoGlobal->puerto_escucha_nodo;
				}
			}
			// Mando por cada t_replanificarMap ok, los datos de cada archivo
			if(send(*socketJob, &archReduce,sizeof(t_archivosReduce),MSG_WAITALL)==-1){
				perror("send");
				log_error(logger, "Fallo mandar la archivo a hacer reduce");
				//exit(-1);
			}
		}
		//ACA RECIBO LA RESPUESTA DE JOB
		t_respuestaReduce respuestaReduceFinal;
		if(recv(*socketJob,&respuestaReduceFinal, sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("recv");
			log_error(logger,"Fallo al recibir el ok del job");
			//exit(-1);
		}

		if(respuestaReduceFinal.resultado == 1){
			//Se aborta la ejecución de Reduce
			log_info(logger,"Falló el Reduce %s. Se abortará el job",respuestaReduceFinal.archivoResultadoReduce);
			t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduceFinal.ip_nodo,respuestaReduceFinal.puerto_nodo);
			restarCantReducers(nodoARestar->nodo_id);
			jobAbort=1; //Se marca el flag del job abortado. Se esperan las demas respuestas para bajar 1 en cantreducers de los nodos
		}
		if(respuestaReduceFinal.resultado == 0){
			printf("Reduce %s exitoso\n",respuestaReduceFinal.archivoResultadoReduce);
			t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduceFinal.ip_nodo,respuestaReduceFinal.puerto_nodo);
			restarCantReducers(nodoARestar->nodo_id);
		}

		if(jobAbort==1){
			//Si entra acá significa que salio mal algun reduce
			char jobAborta[BUF_SIZE];
			memset(jobAborta,'\0',BUF_SIZE);
			strcpy(jobAborta,"aborta");
			if(send(*socketJob,jobAborta,sizeof(jobAborta),MSG_WAITALL)==-1){
				log_error(logger,"Fallo el envío al Job de que aborte por falla de un reduce");
			}
			pthread_exit((void*)0);
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
			if(list_size(listaMapDelNodo)>1){
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
				sumarCantReducers(nodoPrincipal->nodo_id);
				list_add(listaReducerParcial,nodoReducer);
			}
			if(list_size(listaMapDelNodo)==1){
				t_replanificarMap *nodoMapOkk;
				nodoMapOkk=list_get(listaMapDelNodo,0);
				strcpy(nodoReducer->ip_nodoPpal,nodoPrincipal->ip);
				nodoReducer->puerto_nodoPpal=nodoPrincipal->puerto_escucha_nodo;
				strcpy(nodoReducer->nombreArchivoFinal,nodoMapOkk->nombreArchivoDelJob);
				list_add(listaReducerDeUnSoloArchivo,nodoReducer);
			}
			list_destroy(listaMapDelNodo);
		}
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
				//Se aborta la ejecución de Reduce
				log_info(logger,"Falló el Reduce %s. Se abortará el job",respuestaReduce.archivoResultadoReduce);
				t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduce.ip_nodo,respuestaReduce.puerto_nodo);
				restarCantReducers(nodoARestar->nodo_id);
				jobAbortado=1; //Se marca el flag del job abortado. Se esperan las demas respuestas para bajar 1 en cantreducers de los nodos
			}
			if(respuestaReduce.resultado == 0){
				printf("Reduce %s exitoso\n",respuestaReduce.archivoResultadoReduce);
				t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduce.ip_nodo,respuestaReduce.puerto_nodo);
				restarCantReducers(nodoARestar->nodo_id);
			}
		}

		if(jobAbortado==1){
			//Si entra acá significa que salio mal algun reduce
			char jobAborta[BUF_SIZE];
			memset(jobAborta,'\0',BUF_SIZE);
			strcpy(jobAborta,"aborta");
			if(send(*socketJob,jobAborta,sizeof(jobAborta),MSG_WAITALL)==-1){
				log_error(logger,"Fallo el envío al Job de que aborte por falla de un reduce");
			}
			pthread_exit((void*)0);
		}

		//Si llega acá, salieron todos los reduce parciales bien//

		/****REDUCE FINAL****/

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
		for(posNodoOk = 0; posNodoOk < list_size(listaReducerDeUnSoloArchivo); posNodoOk ++){
			t_archivosReduce archivosReduceFinal;
			memset(archivosReduceFinal.archivoAAplicarReduce,'\0',TAM_NOMFINAL);
			memset(archivosReduceFinal.ip_nodo,'\0',20);

			nodoOk = list_get(listaReducerDeUnSoloArchivo,posNodoOk);
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

		sumarCantReducers(nodoRF->nodo_id);

		//ACA RECIBIRIA LA RESPUESTA, A CONTINUACION DE ENVIAR EL ULTIMO REDUCE, PORQUE ES UNA SOLA//
		t_respuestaReduce respuestaReduceFinal;
		if(recv(*socketJob,&respuestaReduceFinal, sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("recv");
			log_error(logger,"Fallo al recibir el ok del job");
			//exit(-1);
		}

		if(respuestaReduceFinal.resultado == 1){
			//Se aborta la ejecución de Reduce
			char jobAborta[BUF_SIZE];
			memset(jobAborta,'\0',BUF_SIZE);
			log_info(logger,"Falló el Reduce %s. Se abortará el job",respuestaReduceFinal.archivoResultadoReduce);
			strcpy(jobAborta,"aborta");
			if(send(*socketJob,jobAborta,sizeof(jobAborta),MSG_WAITALL)==-1){
				log_error(logger,"Fallo el envío al Job de que aborte por falla de un reduce");
			}
			t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduceFinal.ip_nodo,respuestaReduceFinal.puerto_nodo);
			restarCantReducers(nodoARestar->nodo_id);
			pthread_exit((void*)0);
		}
		if(respuestaReduceFinal.resultado == 0){
			printf("Reduce %s exitoso\n",respuestaReduceFinal.archivoResultadoReduce);
			t_nodo *nodoARestar = buscarNodoPorIPYPuerto(respuestaReduceFinal.ip_nodo,respuestaReduceFinal.puerto_nodo);
			restarCantReducers(nodoARestar->nodo_id);
		}

		//Si llega hasta acá, el Job con combiner termino OK
		//Le digo al Job que finalice
		char finaliza[BUF_SIZE];
		memset(finaliza,'\0',BUF_SIZE);
		strcpy(finaliza,"finaliza");
		if(send(*socketJob,finaliza,sizeof(finaliza),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo al enviarle al Job que finalize");
		}

		//Le digo al FS que se copie el resultado
		printf("El job con combiner termino OK\nMandar a FS que busque el resultado %s en el nodo con IP %s puerto %d\n",nodoReduceFinal.nombreArchivoFinal,nodoReduceFinal.ip_nodoPpal,nodoReduceFinal.puerto_nodoPpal);

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
	int i;
	for(i=0; i < list_size(listaArchivos); i++){ //recorre la lista global de archivos
		archivoAux = list_get(listaArchivos,i);
		if (strcmp(archivoAux->nombre,nombreArchivo) ==0 && archivoAux->padre==padre){ //compara el nomnre y padre del archivo del job con cada nombre y padre de archivo de la lista global
			return archivoAux;
		}
	}
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
	for(indice=0;indice<list_size(listaNodos);indice++){
		nodoDeListaGeneral=list_get(listaNodos,indice);
		if(strcmp(nodoDeListaGeneral->nodo_id,copia->nodo)==0){
			if(nodoDeListaGeneral->estado==0){
				return true;
			}
			if(nodoDeListaGeneral->estado==1){
				return false;
			}
		}
	}
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
		if ((strcmp(nodoAux->ip,ipNodo)==0)&&(nodoAux->puerto_escucha_nodo = puertoNodo)){
			return nodoAux;
		}
	}
	return NULL;
}
