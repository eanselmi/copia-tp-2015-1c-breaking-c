#define _FILE_OFFSET_BITS 64

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
#include <pthread.h>
#include <commons/collections/list.h>
#include <commons/temporal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <wait.h>


#include "Nodo.h"

//Declaración de variables Globales
//uint32_t n_bloque;
t_datos_y_bloque combo;
t_config* configurador;
t_log* logger; //log en pantalla y archivo de log
t_log* logger_archivo; //log solo en archivo de log
char* fileDeDatos;
unsigned int sizeFileDatos;
fd_set master; // conjunto maestro de descriptores de fichero
fd_set read_fds; // conjunto temporal de descriptores de fichero para select()
int fdmax;//Numero maximo de descriptores de fichero
int conectorFS; //socket para conectarse al FS
int listener; //socket encargado de escuchar nuevas conexiones
t_list* listaNodosConectados; //Lista con los nodos conectados
t_list* listaMappersConectados; //Lista con los mappers conectados
t_list* listaReducersConectados; //lista con los reducers conectados
t_list* archivosAbiertos; //Archivos ya abiertos que otro nodo me pide que pase renglon
//sem_t semBloques[205]; //Soporta nodos de hasta 4GB 205 *20MB = 4100 MB --> ~4GB  (serían 205 bloques)
int nroMap; //Maps totales
int nroReduce; //Reduce totales
pthread_mutex_t mutexMap=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexReduce=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexNroMap=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexNroReduce=PTHREAD_MUTEX_INITIALIZER;



int main(int argc , char *argv[]){

	//-------------------------- Cuerpo ppal del programa ---------------------------------------
	//------------ Variables locales a la funcion main --------------------

//	int semBloque;
	pthread_t escucha; // Hilo que va a escuchar nuevas conexiones
	configurador= config_create("resources/nodoConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
	logger = log_create("./nodoLog.log", "Nodo", true, LOG_LEVEL_INFO);
	logger_archivo = log_create("./nodoLog.log", "Nodo", false, LOG_LEVEL_INFO);
	fileDeDatos=mapearFileDeDatos();//La siguiente función va a mapear el archivo de datos que esta especificado en el archivo conf a memoria, y asignarle al puntero fileDeDatos la direccion donde arranca el file. Utilizando mmap()
	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	int yes=1; // para setsockopt() SO_REUSEADDR, más abajo
	char identificacion[BUF_SIZE]; //para el mensaje que envie al conectarse para identificarse, puede cambiar
	int *bloquesTotales;
	int *puerto_escucha;
	struct sockaddr_in filesystem; //direccion del fs a donde se conectará
	struct sockaddr_in nodoAddr; //direccion del nodo que será servidor
	char nodo_id[6];
	memset(&filesystem, 0, sizeof(filesystem));
	memset(identificacion,'\0',BUF_SIZE);
	memset(nodo_id,'\0',6);
	listaNodosConectados=list_create();
	listaMappersConectados=list_create();
	listaReducersConectados=list_create();
	archivosAbiertos=list_create();


	bloquesTotales=malloc(sizeof(int));
	*bloquesTotales=sizeFileDatos/20971520;

	nroMap=0; //Inicializo maps totales a 0
	nroReduce=0; //Inicializo reduces totales a 0

//	for(semBloque=0;semBloque<*bloquesTotales;semBloque++){
//		sem_init(&semBloques[semBloque],0,1);
//	}

	//Estructura para conexion con FS
	filesystem.sin_family = AF_INET;
	filesystem.sin_addr.s_addr = inet_addr(config_get_string_value(configurador,"IP_FS"));
	filesystem.sin_port = htons(config_get_int_value(configurador,"PUERTO_FS"));
	//-------------------------------
	puerto_escucha=malloc(sizeof(int));
	*puerto_escucha=config_get_int_value(configurador,"PUERTO_NODO");
	strcpy(nodo_id,config_get_string_value(configurador,"NODO_ID"));
	if ((conectorFS = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror ("socket");
		log_error(logger,"FALLO la creacion del socket");
		exit (-1);
	}
	if (connect(conectorFS, (struct sockaddr *)&filesystem,sizeof(struct sockaddr)) == -1) {
		perror ("connect");
		log_error(logger,"FALLO la conexion con el FS");
		exit (-1);
	}
	log_info(logger,"Se conectó al FS IP: %s, en el puerto: %d",config_get_string_value(configurador,"IP_FS"),config_get_int_value(configurador,"PUERTO_FS"));
	FD_SET(conectorFS,&master); //Agrego al conector con el FS al conjunto maestro
	fdmax=conectorFS; //Por ahora el FD más grande es éste
	// aca revisaria si el nodo es nuevo o si es un nodo que se esta reconectando y dependiendo el caso, envia un mensaje y otro

	if (string_equals_ignore_case(config_get_string_value(configurador,"NODO_NUEVO"),"SI")){ //verifica si el nodo es nuevo
			//envio mensaje de identificación
			strcpy(identificacion,"nuevo");
			if((send(conectorFS,identificacion,sizeof(identificacion),MSG_WAITALL))==-1) {
					perror("send");
					log_error(logger,"FALLO el envio del saludo al FS");
					exit(-1);
			}
			//envio cantidad de bloques totales
			if((send(conectorFS,bloquesTotales,sizeof(int),MSG_WAITALL))==-1){
				perror("send");
				log_error(logger,"FALLO el envío de la cantidad de bloques totales al FS");
				exit(-1);
			}
			if((send(conectorFS,puerto_escucha,sizeof(int),MSG_WAITALL))==-1){
				perror("send");
				log_error(logger,"FALLO el envío de la cantidad de bloques totales al FS");
				exit(-1);
			}
			if((send(conectorFS,nodo_id,sizeof(nodo_id),MSG_WAITALL))==-1){
				perror("send");
				log_error(logger,"FALLO el envío del ID del nodo al FS");
				exit(-1);
			}
		}
		else {
			//si el if da falso por nodo existente que se esta reconectando
			strcpy(identificacion,"reconectado");
			if((send(conectorFS,identificacion,sizeof(identificacion),MSG_WAITALL))==-1) {
					perror("send");
					log_error(logger,"FALLO el envio del saludo al FS");
					exit(-1);
			}
			if((send(conectorFS,nodo_id,sizeof(nodo_id),MSG_WAITALL))==-1){
				perror("send");
				log_error(logger,"FALLO el envío de la cantidad de bloques totales al FS");
				exit(-1);
			}
		}

	/*
	El nodo ya se conectó al FS, ahora queda a la espera de conexiones de hilos mapper/hilos reduce/otros nodos
	*/

	if ((listener = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
			perror("socket");
			log_error(logger,"FALLO la creacion del socket");
			exit(-1);
		}
	// obviar el mensaje "address already in use" (la dirección ya se está usando)
	if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes,sizeof(int)) == -1) {
		perror("setsockopt");
		log_error(logger,"FALLO la ejecucion del setsockopt");
		exit(-1);
	}
	// enlazar
	nodoAddr.sin_family = AF_INET;
	nodoAddr.sin_addr.s_addr = inet_addr(config_get_string_value(configurador,"IP_NODO"));
	nodoAddr.sin_port = htons(config_get_int_value(configurador,"PUERTO_NODO"));
	memset(&(nodoAddr.sin_zero), '\0', 8);
	if (bind(listener, (struct sockaddr *)&nodoAddr, sizeof(nodoAddr)) == -1) {
		perror("bind");
		log_error(logger,"FALLO el Bind");
		exit(-1);
	}
	// escuchar
	if (listen(listener, 100) == -1) {
		perror("listen");
		log_error(logger,"FALLO el Listen");
		exit(1);
	}
	FD_SET(listener,&master); //Agrego al listener al conjunto maestro
	if(listener>fdmax){
		fdmax=listener; //el fd máximo hasta el momento es el listener
	}


	//Creación del hilo que va a manejar nuevas conexiones / cambios en las conexiones
	if( pthread_create(&escucha, NULL, manejador_de_escuchas, NULL) != 0 ) {
		perror("pthread_create");
		log_error(logger,"Fallo la creación del hilo manejador de escuchas");
		return 1;
	}


	pthread_join(escucha,NULL);
	log_destroy(logger);
	log_destroy(logger_archivo);
	config_destroy(configurador);
	return 0;
}

void *manejador_de_escuchas(){
	pthread_t mapperThread;
	pthread_t reducerThread;
	char mensaje[BUF_SIZE]; //Mensaje que recivirá de los clientes
	char archivoAPasar[TAM_NOMFINAL]; //Nombre del archivo que me pide otro nodo
	char renglones[TAM_RENGLONES]; //Renglones que entran en 2048 bytes para otro nodo
	char renglonTemporal[512];
	char bufferRenglones[TAM_RENGLONES];
	int socketModificado,nbytes,newfd,addrlen,read_size;
	struct sockaddr_in remote_client; //Direccion del cliente que se conectará
	int* socketNodo; //para identificar los que son nodos conectados
	int* socketMapper; //para identificar los que son mappers conectados
	int* socketReducer; //para identificar los que son reducers conectados
	int* bloqueParaFS;


	printf("Nodo en la espera de conexiones/solicitudes del FS\n");
	while(1) {
		read_fds = master;
		if (select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1) {
			perror("select");
			log_error(logger,"FALLO el Select");
			exit(-1);
		}
		memset(mensaje,'\0',BUF_SIZE);
//		socketMapper=malloc(sizeof(int));
//		socketReducer=malloc(sizeof(int));
//		socketNodo=malloc(sizeof(int));
		memset(combo.buf_20mb,'\0',BLOCK_SIZE);

		//memset(buffer,'\0',BLOCK_SIZE);

		// explorar conexiones existentes en busca de datos que leer
		for(socketModificado = 0; socketModificado <= fdmax; socketModificado++) {
			if (FD_ISSET(socketModificado, &read_fds)) {	// ¡¡tenemos datos!!

				/* -- Nuevas conexiones(o nodo, o hilo map, o hilo reducer) --*/

				if(socketModificado==listener){
					addrlen=sizeof(struct sockaddr_in);
					if ((newfd = accept(listener, (struct sockaddr*)&remote_client,(socklen_t*)&addrlen)) == -1) {
						perror("accept");
						log_error(logger,"FALLO el ACCEPT");
						exit(-1);
					} else {//llego una nueva conexion, se acepto y ahora tengo que tratarla
						if((nbytes=recv(newfd,mensaje,sizeof(mensaje),MSG_WAITALL))<=0){ //error
							perror("recive");
							log_error(logger,"Falló el receive");
							exit(-1);
						}
						else{
							// el nuevo conectado me manda algo, se identifica como mapper, reducer o nodo
							if(nbytes>0 && strncmp(mensaje,"soy nodo",9)==0){
								//se conectó un nodo
								socketNodo=malloc(sizeof(int));
								*socketNodo=newfd;
								list_add(listaNodosConectados,socketNodo); //agrego el nuevo socket a la lista de Nodos conectados
								FD_SET(newfd,&master); //añadir al conjunto maestro
								if(newfd>fdmax){ //actualizar el máximo
									fdmax=newfd;
								}
								log_info(logger,"Se conectó el nodo %s, puerto %d",inet_ntoa(remote_client.sin_addr),ntohs(remote_client.sin_port));

							}
							if(nbytes>0 && strncmp(mensaje,"soy mapper",11)==0){
								//se conectó un hilo mapper
								socketMapper=malloc(sizeof(int));
								*socketMapper=newfd;

								log_info(logger,"Se conectó un hilo mapper desde %s",inet_ntoa(remote_client.sin_addr));

								if(pthread_create(&mapperThread,NULL,(void*)rutinaMap,socketMapper)!=0){
									perror("pthread_create");
									log_error(logger,"Fallo la creación del hilo manejador de escuchas");
								}

							}
							if(nbytes>0 && strncmp(mensaje,"soy reducer",12)==0){
								//se conectó un hilo reducer
								socketReducer=malloc(sizeof(int));
								*socketReducer=newfd;

								log_info(logger,"Se conectó un hilo reducer desde %s",inet_ntoa(remote_client.sin_addr));

								if(pthread_create(&reducerThread,NULL,(void*)rutinaReduce,socketReducer)!=0){
									perror("pthread_create");
									log_error(logger,"Fallo la creación del hilo manejador de escuchas");
								}
							}
						}
					}
				}

				/*-- Conexión con el fileSystem --*/

				if(socketModificado==conectorFS){
					if ((nbytes=recv(conectorFS,mensaje,sizeof(mensaje),MSG_WAITALL))==-1){ //da error
						perror("recv");
						log_error(logger,"Falló el receive");
						exit(-1);
					}
					printf ("Recibi del handshake %d\n",nbytes);
					if(nbytes==0){ //se desconectó
						close(conectorFS);
						FD_CLR(conectorFS,&master);
						log_info(logger,"Se desconectó el FileSystem.");
						exit(1);
					}
					else{
						/* -- el filesystem envío un mensaje a tratar -- */
						if(strncmp(mensaje,"copiar_archivo",14)==0){
							mensaje[14]=0;
							printf ("Handshake: %s\n",mensaje);
							if ((read_size = recv(conectorFS, &combo, sizeof(combo),MSG_WAITALL)) <= 0) {
								perror("recv");
								log_error(logger, "FALLO el Recv de bloque");
								exit(-1);
							}
							printf ("Recibi: %d\n",read_size);
							printf ("Me mandaron un coso de 20MB para el bloque %d\n",combo.n_bloque);
							setBloque(combo.n_bloque,combo.buf_20mb); //esto deberia devolver algo que identifique si salio bien o no para informar al fs si fallo o fue exitosa la copai del bloque en el mdfs
						}
						if(strncmp(mensaje,"obtener bloque", 14) == 0){
							//Recibo un numero de bloque del FS
							bloqueParaFS=malloc(sizeof(int));
							mensaje[14]=0;
							if ((read_size = recv(conectorFS, bloqueParaFS, sizeof(int),0)) <= 0) {
								perror("recv");
								log_error(logger, "FALLO el Recv de bloque");
								exit(-1);
							}
							printf("Voy a obtener el bloque:%d\n",*bloqueParaFS);

							// Envio el contenido del bloque que me pidio el FS
							if (send(conectorFS, getBloque(*bloqueParaFS),BLOCK_SIZE, 0) == -1) {
								perror("send");
								log_error(logger, "FALLO el envio del bloque ");
								exit(-1);
							}
							free(bloqueParaFS);
						}

						if(strncmp(mensaje,"resultado", 9) == 0){
							//Recibo un numero de bloque del FS
							char nombreArchivoResultado[60];
							memset(nombreArchivoResultado,'\0',60);
							char ruta_local[100];
							char buff_resultado[4096];
							char buff_enviar[4096];
							char renglon[512];
							memset(renglon,'\0',512);
							memset(ruta_local,'\0',100);
							strcpy(ruta_local,"/tmp/");
							if ((read_size = recv(conectorFS, nombreArchivoResultado, sizeof(nombreArchivoResultado),0)) <= 0) {
								perror("recv");
								log_error(logger, "FALLO el Recv de bloque");
								exit(-1);
							}
							strcat(ruta_local,nombreArchivoResultado);
							FILE *archivo_resultado = fopen(ruta_local, "r");
							if(archivo_resultado == NULL){
								perror ("fopen archivo resultado");
								exit(1);
							}

							int posicionBuffer=0,posicionInicial,i=0,byteLeido,corta=0;

							while(corta!=1){
								memset(buff_resultado,'\0',4096);
								memset(buff_enviar,'\0',4096);
								if(feof(archivo_resultado)){
									strcpy(buff_enviar,"corta");
									corta=1;
									if(send(conectorFS,buff_enviar,sizeof(buff_enviar),MSG_WAITALL)==-1){
										perror("send");
										log_error(logger,"Se desconecto un nodo");
									}
									break;
								}
								fseek(archivo_resultado,posicionBuffer,SEEK_SET);
								fread(buff_resultado,sizeof(char),sizeof(buff_resultado),archivo_resultado);
								posicionInicial=posicionBuffer;
								i=0;
								for(byteLeido=0;byteLeido<4096;byteLeido++){
									renglon[i]=buff_resultado[byteLeido];
									if(buff_resultado[byteLeido]=='\n'){
										posicionBuffer=posicionInicial+byteLeido+1;
										strcat(buff_enviar,renglon);
										memset(renglon,'\0',512);
										i=-1;
									}
									i++;
									if (buff_resultado[byteLeido]=='\0') break;
								}
								if(send(conectorFS,buff_enviar,sizeof(buff_enviar),MSG_WAITALL)==-1){
									perror("send");
									log_error(logger,"Se desconecto un nodo");
								}
							}
							printf("Archivo resultado enviado correctamente\n");
							fclose(archivo_resultado);
						}
					}
				}

				//-- Conexión con otro nodo --//

				if(estaEnListaNodos(socketModificado)==0){
					if ((nbytes=recv(socketModificado,mensaje,sizeof(mensaje),MSG_WAITALL))==-1){ //da error
						perror("recv");
						log_error(logger,"Falló el receive");
					}
					if(nbytes==0){ //se desconectó
						close(socketModificado);
						FD_CLR(socketModificado,&master);
						log_info(logger,"Se desconectó un Nodo");
					}
					else{
						/* -- el nodo envío un mensaje a tratar -- */

						if(strncmp(mensaje,"Dame renglones",14)==0){ //Me pidio un renglon, espero el nombre de que archivo

							memset(renglones,'\0',TAM_RENGLONES);
							memset(bufferRenglones,'\0',TAM_RENGLONES);
							memset(archivoAPasar,'\0',TAM_NOMFINAL);
							memset(renglonTemporal,'\0',512);
							int posicionInicial;
							int byteLeido;
							int i;
							t_archivoAbierto* archivoPedido;
							if(recv(socketModificado,archivoAPasar,sizeof(archivoAPasar),MSG_WAITALL)<=0){
								perror("recv");
								log_error(logger,"Se desconectó un nodo");
							}
							printf("Me pidio renglones del archivo %s\n",archivoAPasar);

							if((archivoPedido=estaEnListaArchivosAbiertos(archivoAPasar))==NULL){ //Si el archivo no está ya abierto, lo abro y leo un renglon
								t_archivoAbierto* archAbiertoNuevo=malloc(sizeof(t_archivoAbierto));
								memset(archAbiertoNuevo->nombreArchivo,'\0',TAM_NOMFINAL);
								FILE* archParaPasar;

								//Abro el nuevo archivo
								archParaPasar=fopen(archivoAPasar,"r");
								//Agrego al nuevo archivo abierto a la lista de archivos abiertos
								archAbiertoNuevo->archivoAbierto=archParaPasar;
								strcpy(archAbiertoNuevo->nombreArchivo,archivoAPasar);
								archAbiertoNuevo->posicionBuffer=0;
								//Lee 2048 bytes desde la posicion del archivo para pasar
								fseek(archParaPasar,archAbiertoNuevo->posicionBuffer,SEEK_SET);
								fread(bufferRenglones,sizeof(char),TAM_RENGLONES,archParaPasar);
								//va agregando los renglones al "renglon"
								posicionInicial=archAbiertoNuevo->posicionBuffer;
								i=0;
								for(byteLeido=0;byteLeido<TAM_RENGLONES;byteLeido++){
									renglonTemporal[i]=bufferRenglones[byteLeido];
									if(bufferRenglones[byteLeido]=='\n'){
										archAbiertoNuevo->posicionBuffer=posicionInicial+byteLeido+1;
										strcat(renglones,renglonTemporal);
										memset(renglonTemporal,'\0',512);
										i=-1;
									}
									i++;
									if (bufferRenglones[byteLeido]=='\0') break;

								}

//								if(feof(archParaPasar)){
//									fclose(archParaPasar);
//									strcpy(renglones,"Llego al EOF");
//									removerDeListaDeArchivosAbiertos(archParaPasar);
//								}

								list_add(archivosAbiertos,archAbiertoNuevo);

							}
							else{ //si ya esta abierto pregunto si quedo en eof
								if(!feof(archivoPedido->archivoAbierto)){
									//si no quedo en eof, voy a la posicion que tenia y leo 2048, voy concatenando los renglones
									fseek(archivoPedido->archivoAbierto,archivoPedido->posicionBuffer,SEEK_SET);
									fread(bufferRenglones,sizeof(char),TAM_RENGLONES,archivoPedido->archivoAbierto);
									//va agregando los renglones al "renglon"
									posicionInicial=archivoPedido->posicionBuffer;

									i=0;
									for(byteLeido=0;byteLeido<TAM_RENGLONES;byteLeido++){
										renglonTemporal[i]=bufferRenglones[byteLeido];
										if(bufferRenglones[byteLeido]=='\n'){
											archivoPedido->posicionBuffer=posicionInicial+byteLeido+1;
											strcat(renglones,renglonTemporal);
											memset(renglonTemporal,'\0',512);
											i=-1;
										}
										i++;
										if (bufferRenglones[byteLeido]=='\0') break;
									}
								}
								else{
									fclose(archivoPedido->archivoAbierto);
									strcpy(renglones,"Llego al EOF\n");
									removerDeListaDeArchivosAbiertos(archivoPedido->archivoAbierto);
								}
							}

							if(send(socketModificado,renglones,sizeof(renglones),MSG_WAITALL)==-1){
								perror("send");
								log_error(logger,"Se desconecto un nodo");
							}

						}

					}
				}

				//-- Conexión con hilo mapper --//

				if(estaEnListaMappers(socketModificado)==0){
					if ((nbytes=recv(socketModificado,mensaje,sizeof(mensaje),MSG_WAITALL))==-1){ //da error
						perror("recv");
						log_error(logger,"Falló el receive");
						exit(-1);
					}
					if(nbytes==0){ //se desconectó
						close(socketModificado);
						FD_CLR(socketModificado,&master);
//						log_info(logger,"Se fue un mapper");
					}
					else{
						/* -- el mapper envío un mensaje a tratar -- */


					}
				}


				//-- Conexión con hilo reducer --//

				if(estaEnListaReducers(socketModificado)==0){
					if ((nbytes=recv(socketModificado,mensaje,sizeof(mensaje),MSG_WAITALL))==-1){ //da error
						perror("recv");
						log_error(logger,"Falló el receive");
						exit(-1);
					}
					if(nbytes==0){ //se desconectó
						close(socketModificado);
						FD_CLR(socketModificado,&master);
					}
					else{

						/* -- el reducer envío un mensaje a tratar -- */

					}
				}
			}
		}
	}
}

int buscarPosicionDeArchivoAbierto(FILE* archivoABuscar){
	int tamanio=list_size(archivosAbiertos);
	int indice=0;
	t_archivoAbierto* unArchAbierto;
	for(indice=0;indice<tamanio;indice++){
		unArchAbierto=list_get(archivosAbiertos,indice);
		if(unArchAbierto->archivoAbierto==archivoABuscar){
			return unArchAbierto->posicionBuffer;
		}
	}
	return -1;
}

t_archivoAbierto* estaEnListaArchivosAbiertos(char* nombreArchivo){
	int tamanio=list_size(archivosAbiertos);
	int indice=0;
	t_archivoAbierto* unArchAbierto;
	for(indice=0;indice<tamanio;indice++){
		unArchAbierto=list_get(archivosAbiertos,indice);
		if(strcmp(unArchAbierto->nombreArchivo,nombreArchivo)==0){
			return unArchAbierto;
		}
	}
	//No está abierto
	return NULL;
}

void removerDeListaDeArchivosAbiertos(FILE* archivoARemover){
	int tamanio=list_size(archivosAbiertos);
	int indice=0;
	t_archivoAbierto* unArchAbierto;
	for(indice=0;indice<tamanio;indice++){
		unArchAbierto=list_get(archivosAbiertos,indice);
		if(unArchAbierto->archivoAbierto==archivoARemover){
			list_remove_and_destroy_element(archivosAbiertos,indice,(void*)eliminarArchivo);
			break;
		}
	}
}

static void eliminarArchivo(t_archivoAbierto * archivoAbierto) {
    free(archivoAbierto);
}

void ordenarMapper(char* pathMapperTemporal, char* nombreMapperOrdenado){
	int outfd[2];
	int bak,pid,archivo_resultado;
	bak=0;
	char** pathMapperSeparado;
	char* nombreMapperTemporal;
	char* contenidoDelMapper;
//	sem_t terminoSort;
//	sem_init(&terminoSort,0,1);
	nombreMapperTemporal=string_new();
	pathMapperSeparado=string_split(pathMapperTemporal,"/");
	string_append(&nombreMapperTemporal,pathMapperSeparado[1]); // le agrega al nombre mapper lo que hay dps de /tmp
	// si cada nodo tiene su tmp en una carpeta distinta --> string_append(&nombreMapperTemporal,pathMapperSeparado[2]); // le agrega al nombre mapper lo que hay dps de /tmp/nombredir
	pipe(outfd);
	if((pid=fork())==-1){
		perror("fork sort");
	}
	else if(pid==0)
	{
		archivo_resultado=open(nombreMapperOrdenado,O_RDWR|O_CREAT,S_IRWXU|S_IRWXG); //abro file resultado, si no esta lo crea, asigno permisos
		fflush(stdout);
		bak=dup(STDOUT_FILENO);
		dup2(archivo_resultado,STDOUT_FILENO); //STDOUT de este proceso se grabara en el file resultado
		close(archivo_resultado);
		close(STDIN_FILENO);
		dup2(outfd[0], STDIN_FILENO); //STDIN de este proceso será STDOUT del proceso padre
		close(outfd[0]); /* innecesarios para el hijo */
		close(outfd[1]);
		execlp("/usr/bin/sort","sort",NULL);
//	    sem_post(&terminoSort);
	}
	else
	{
		//Se debe escribir el contenido de la rutina Map
		contenidoDelMapper=getFileContent(nombreMapperTemporal);
		//printf("Tamaño del contenido que mando al sort: %d",strlen(contenidoDelMapper));
		write(outfd[1],contenidoDelMapper,strlen(contenidoDelMapper));
		close(outfd[0]); /* Estan siendo usados por el hijo */
		close(outfd[1]);
		free(contenidoDelMapper);
		waitpid(pid,NULL,0);
//		sem_wait(&terminoSort);
		dup2(bak,STDOUT_FILENO);
	}
}


void ejecutarMapper(char *script,int bloque,char *resultado){
	int outfd[2];
	int bak,pid,archivo_resultado;
//	int tamanioAMapear;
	bak=0;
	char *path;
	char *bloqueAMapear;
//	sem_t terminoElMap;
//	sem_init(&terminoElMap,0,1);
	pipe(outfd); /* Donde escribe el padre */
	if((pid=fork())==-1){
		perror("fork mapper");
	}
	else if(pid==0)
	{

	archivo_resultado=open(resultado,O_RDWR|O_CREAT,S_IRWXU|S_IRWXG); //abro file resultado, si no esta lo crea, asigno permisos
	fflush(stdout);
	bak=dup(STDOUT_FILENO);
	dup2(archivo_resultado,STDOUT_FILENO); //STDOUT de este proceso se grabara en el file resultado
	close(archivo_resultado);
	close(STDIN_FILENO);
	dup2(outfd[0], STDIN_FILENO); //STDIN de este proceso será STDOUT del proceso padre
	close(outfd[0]); /* innecesarios para el hijo */
	close(outfd[1]);
	path=string_new();
	string_append(&path,config_get_string_value(configurador,"PATHMAPPERS"));
	string_append(&path,"/");
	string_append(&path,script);
	char *ejecucion []={path,script,NULL};
	if(execvp(ejecucion[0],ejecucion)==-1){
		perror("Ejecucion map:");
		log_error(logger,"Error en la ejecucion de un map");
	}
//	sem_post(&terminoElMap);
	}
	else
	{

	bloqueAMapear=getBloque(bloque);

//	for(tamanioAMapear=BLOCK_SIZE;tamanioAMapear>=0;tamanioAMapear--){
//		if(bloqueAMapear[tamanioAMapear]=='\n'){
//			break;
//		}
//	}

	write(outfd[1],bloqueAMapear,strlen(bloqueAMapear));/* Escribe en el stdin del hijo el contenido del bloque*/

	close(outfd[0]); /* Estan siendo usados por el hijo */
	close(outfd[1]);
	waitpid(pid,NULL,0);
//	sem_wait(&terminoElMap);
	dup2(bak,STDOUT_FILENO);
	}

}

int estaEnListaNodos(int socket){
	int i,tamanio;
	int* nodoDeLaLista;
	tamanio=list_size(listaNodosConectados);
	for(i=0;i<tamanio;i++){
		nodoDeLaLista=list_get(listaNodosConectados,i);
		if(*nodoDeLaLista==socket){
			return 0;
		}
	}
	return -1;
}

int estaEnListaMappers(int socket){
	int i,tamanio;
	int* mapperDeLaLista;
	tamanio=list_size(listaMappersConectados);
	for(i=0;i<tamanio;i++){
		mapperDeLaLista=list_get(listaMappersConectados,i);
		if(*mapperDeLaLista==socket){
			return 0;
		}
	}
	return -1;
}

int estaEnListaReducers(int socket){
	int i,tamanio;
	int* reducerDeLaLista;
	tamanio=list_size(listaReducersConectados);
	for(i=0;i<tamanio;i++){
		reducerDeLaLista=list_get(listaReducersConectados,i);
		if(*reducerDeLaLista==socket){
			return 0;
		}
	}
	return -1;
}


void setBloque(uint32_t numBloque,char* datosAEscribir){
	/*
	* El puntero ubicacionEnElFile, se va a posicionar en el bloque que se desea escribir el archivo
	* datosAEscribir, recibido por parametro, tiene los datos que quiero escribir
	* Con el memcpy a ubicacionEnElFile, escribo en ese bloque
	*/
	//sem_wait(&semBloques[numBloque]);

	char *ubicacionEnElFile;
	//ubicacionEnElFile=malloc(BLOCK_SIZE);
	ubicacionEnElFile=fileDeDatos+(BLOCK_SIZE*(numBloque));
	memcpy(ubicacionEnElFile,datosAEscribir,BLOCK_SIZE); //Copia el valor de BLOCK_SIZE bytes desde la direccion de memoria apuntada por datos a la direccion de memoria apuntada por fileDeDatos
	log_info(logger_archivo,"Se escribió el bloque %d",numBloque);
	//sem_post(&semBloques[numBloque]);

	return;
}

char* getBloque(int numBloque){
	/*
	* El puntero ubicacionEnElFile, se va a posicionar en el bloque de donde deseo leer los datos
	* El puntero datosLeidos, tendrá los datos que lei, y será devuelto por la funcion
	* Con el memcpy a datosLeidos, copio ese bloque
	*/

	//sem_wait(&semBloques[numBloque]);
	//char* datosLeidos;
	char *ubicacionEnElFile;
	//datosLeidos=malloc(BLOCK_SIZE);
	//ubicacionEnElFile=malloc(BLOCK_SIZE);
	ubicacionEnElFile=fileDeDatos+(BLOCK_SIZE*(numBloque));
	//memcpy(datosLeidos,ubicacionEnElFile,BLOCK_SIZE); //Copia el valor de BLOCK_SIZE bytes desde la direccion de memoria apuntada por fileDeDatos a la direccion de memoria apuntada por datosLeidos
	log_info(logger_archivo,"Se leyó el bloque %d",numBloque);
	//sem_post(&semBloques[numBloque]);
	return ubicacionEnElFile;
}

char* getFileContent(char* nombreFile){
	FILE * archivoLocal;
	int i=0;
	char* path;
	char* contenido;
	char buffer[1024];
	memset(buffer,'\0',1024);
	contenido=string_new();
	path = string_new();
	string_append(&path,config_get_string_value(configurador,"DIR_TEMP"));
	string_append(&path,"/");
	string_append(&path,nombreFile);
	archivoLocal = fopen(path,"r");
	fseek(archivoLocal,0,SEEK_SET);
	while (!feof(archivoLocal)){
		fread(buffer,sizeof(char),1024,archivoLocal);
		if(feof(archivoLocal)){
			for(i=1023;i>=0;i--){
				if(buffer[i]=='\n'){
					strncat(contenido,buffer,i+1);
					break;
				}
			}
		}else{
			string_append(&contenido,buffer);
		}
		memset(buffer,'\0',1024);
	}
	fclose(archivoLocal);
	//printf("tamaño de contenido %d\n",strlen(contenido));
	free(path);
	return contenido;
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
	/*Chequeo de apertura del file exitosa*/
		if (fileDescriptor==-1){
			perror("open");
			log_error(logger,"Fallo la apertura del file de datos");
			exit(-1);
		}
	struct stat estadoDelFile; //declaro una estructura que guarda el estado de un archivo
	if(fstat(fileDescriptor,&estadoDelFile)==-1){//guardo el estado del archivo de datos en la estructura
		perror("fstat");
		log_error(logger,"Falló el fstat");
	}
	sizeFileDatos=estadoDelFile.st_size; // guardo el tamaño (necesario para el mmap)

	/*se mapea a memoria,fileDatos apuntará a una direccion en donde empieza el archivo, con permisos de
	lectura escritura y ejecucion, los cambios en las direcciones de memoria a donde apunta se verán reflejados
	 en el archivo*/

	fileDatos=mmap(0,sizeFileDatos,(PROT_WRITE|PROT_READ|PROT_EXEC),MAP_SHARED,fileDescriptor,0);
	/*Chequeo de mmap exitoso*/
		if (fileDatos==MAP_FAILED){
			perror("mmap");
			log_error(logger,"Falló el mmap, no se pudo asignar la direccion de memoria para el archivo de datos");
			exit(-1);
		}
	close(fileDescriptor); //Cierro el archivo
	return fileDatos;
}

void* rutinaMap(int* sckMap){
	pthread_detach(pthread_self());
	char** arrayTiempo;
	int resultado=1;
	t_datosMap datosParaElMap;

	char *resultadoTemporal=string_new();
	char *nombreNuevoMap=string_new(); //será el nombre del nuevo map
	char *tiempo=string_new(); //string que tendrá la hora
	char *pathNuevoMap=string_new();//El path completo del nuevo Map
	FILE* scriptMap;

	pthread_mutex_lock(&mutexNroMap);
	char * stringNroMap=string_itoa(nroMap);
	nroMap++;
	pthread_mutex_unlock(&mutexNroMap);

	if(recv(*sckMap,&datosParaElMap,sizeof(t_datosMap),MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo al recibir los datos para el map");
		pthread_exit((void*)0);
	}

	printf("Se aplicará la rutina mapper en el bloque %d\n",datosParaElMap.bloque);

	//printf("Se guardará el resultado del mapper en el archivo temporal %s\n",datosParaElMap.nomArchTemp);

	//printf("se recibió la rutina mapper:\n%s",datosParaElMap.rutinaMap);

	//Creo el archivo que guarda la rutina de map enviada por el Job
	//Generar un nombre para este script Map
	string_append(&nombreNuevoMap,stringNroMap);
	string_append(&nombreNuevoMap,"mapJob");
	arrayTiempo=string_split(temporal_get_string_time(),":"); //creo array con hora minutos segundos y milisegundos separados
	string_append(&tiempo,arrayTiempo[0]);//Agrego horas
	string_append(&tiempo,arrayTiempo[1]);//Agrego minutos
	string_append(&tiempo,arrayTiempo[2]);//Agrego segundos
	string_append(&tiempo,arrayTiempo[3]);//Agrego milisegundos
	string_append(&nombreNuevoMap,tiempo); //Concateno la fecha en formato hhmmssmmmm al nombre map
	string_append(&nombreNuevoMap,".sh"); //agrego la extensión
	string_append(&pathNuevoMap,config_get_string_value(configurador,"PATHMAPPERS"));
	string_append(&pathNuevoMap,"/");
	string_append(&pathNuevoMap,nombreNuevoMap);
	//Genero nombre para el resultado temporal (luego a este se debera aplicar sort)
	string_append(&resultadoTemporal,datosParaElMap.nomArchTemp);
	//string_append(&resultadoTemporal,tiempo);
	string_append(&resultadoTemporal,".tmp");

	//Meto al nombre map final el ddirectorio temporal

	printf("Nombre del map temporal(antes del sort):%s\n",resultadoTemporal);
	printf("Nombre del map ordenado(luego del sort):%s\n",datosParaElMap.nomArchTemp);


	if((scriptMap=fopen(pathNuevoMap,"w+"))==NULL){ //path donde guardara el script
		perror("fopen");
		log_error(logger,"Fallo al crear el script del mapper");
		pthread_exit((void*)0);
	}
	if(fputs(datosParaElMap.rutinaMap,scriptMap)==EOF){
		perror("fputs");
		log_error(logger,"Fallo el fputs en una rutina map");
		pthread_exit((void*)0);
	}

	// agrego permisos de ejecucion
	if(chmod(pathNuevoMap,S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH)==-1){
		perror("chmod");
		log_error(logger,"Fallo el cambio de permisos para el script de map");
		pthread_exit((void*)0);
	}
	fclose(scriptMap); //cierro el file


	pthread_mutex_lock(&mutexMap);

	ejecutarMapper(nombreNuevoMap,datosParaElMap.bloque,resultadoTemporal);
	ordenarMapper(resultadoTemporal,datosParaElMap.nomArchTemp);

	pthread_mutex_unlock(&mutexMap);

	resultado=0;

	if(send(*sckMap,&resultado,sizeof(int),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envío del resultado al map");
		pthread_exit((void*)0);
	}

	printf("Se envío el resultado:%d \n",0);

	free(arrayTiempo);
	free(resultadoTemporal);
	free(nombreNuevoMap);
	free(tiempo);
	free(pathNuevoMap);

	pthread_exit((void*)0);

}

void* rutinaReduce (int* sckReduce){
	pthread_detach(pthread_self());
	char nombreFinalReduce[TAM_NOMFINAL];
	char rutinaReduce[REDUCE_SIZE];
	int cantidadArchivos;
	int posicionEnListaArchivos=0;
	int indice=0;
	FILE* scriptReduce;
	t_respuestaNodoReduce respuestaParaJob;
	t_list* listaArchivosReduce;
	t_list* archivosEnApareo;
	memset(nombreFinalReduce,'\0',TAM_NOMFINAL);
	memset(rutinaReduce,'\0',REDUCE_SIZE);
	listaArchivosReduce=list_create();
	archivosEnApareo=list_create();
	char** arrayTiempo;
	char *nombreNuevoReduce=string_new(); //será el nombre del nuevo map
	char *tiempo=string_new(); //string que tendrá la hora
	char *pathNuevoReduce=string_new();//El path completo del nuevo Map
	memset(respuestaParaJob.ip_nodoFallido,'\0',20);

	pthread_mutex_lock(&mutexNroReduce);
	char * stringNroReduce=string_itoa(nroReduce);
	nroReduce++;
	pthread_mutex_unlock(&mutexNroReduce);


	if(recv(*sckReduce,&nombreFinalReduce,TAM_NOMFINAL,MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo al recibir el nombre del archivo final del reduce");
		pthread_exit((void*)0);
	}

	printf("El resultado del reduce se guardará en: %s\n",nombreFinalReduce);

	if(recv(*sckReduce,rutinaReduce,sizeof(rutinaReduce),MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo al recibir la rutina reduce");
		pthread_exit((void*)0);
	}

	//printf("Se recibio la rutina reduce:%s\n",rutinaReduce);
	string_append(&nombreNuevoReduce,stringNroReduce);
	string_append(&nombreNuevoReduce,"reduceJob");
	arrayTiempo=string_split(temporal_get_string_time(),":"); //creo array con hora minutos segundos y milisegundos separados
	string_append(&tiempo,arrayTiempo[0]);//Agrego horas
	string_append(&tiempo,arrayTiempo[1]);//Agrego minutos
	string_append(&tiempo,arrayTiempo[2]);//Agrego segundos
	string_append(&tiempo,arrayTiempo[3]);//Agrego milisegundos
	string_append(&nombreNuevoReduce,tiempo); //Concateno la fecha en formato hhmmssmmmm al nombre map
	string_append(&nombreNuevoReduce,".sh"); //agrego la extensión
	string_append(&pathNuevoReduce,config_get_string_value(configurador,"PATHREDUCERS"));
	string_append(&pathNuevoReduce,"/");
	string_append(&pathNuevoReduce,nombreNuevoReduce);

	if((scriptReduce=fopen(pathNuevoReduce,"w+"))==NULL){ //path donde guardara el script
		perror("fopen");
		log_error(logger,"Fallo al crear el script del mapper");
		respuestaParaJob.resultado=1;
		respuestaParaJob.puerto_nodoFallido=config_get_int_value(configurador,"PUERTO_NODO");
		strcpy(respuestaParaJob.ip_nodoFallido,config_get_string_value(configurador,"IP_NODO"));
		if(send(*sckReduce,&respuestaParaJob,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envio de la respues KO al Reduce");
		}
		pthread_exit((void*)0);
	}
	fputs(rutinaReduce,scriptReduce);

	printf("Nombre del nuevo reduce: %s\n",nombreNuevoReduce);
	// agrego permisos de ejecucion
	if(chmod(pathNuevoReduce,S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH)==-1){
		perror("chmod");
		log_error(logger,"Fallo el cambio de permisos para el script de map");
		respuestaParaJob.resultado=1;
		respuestaParaJob.puerto_nodoFallido=config_get_int_value(configurador,"PUERTO_NODO");
		strcpy(respuestaParaJob.ip_nodoFallido,config_get_string_value(configurador,"IP_NODO"));
		if(send(*sckReduce,&respuestaParaJob,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envio de la respues KO al Reduce");
		}
		pthread_exit((void*)0);
	}
	fclose(scriptReduce); //cierro el file

	if(recv(*sckReduce,&cantidadArchivos,sizeof(int),MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo al recibir la cantidad de archivos a aplicar reduce");
		pthread_exit((void*)0);
	}

	printf("Se debe aplicar reduce en %d archivos\n",cantidadArchivos);

	for(indice=0;indice<cantidadArchivos;indice++){
		t_archivosReduce archivoQueRecibo;
		t_archivosReduce *archivoParaReduce;
		archivoParaReduce=malloc(sizeof(t_archivosReduce));

		if(recv(*sckReduce,&archivoQueRecibo,sizeof(t_archivosReduce),MSG_WAITALL)==-1){
			perror("recv");
			log_error(logger,"Fallo al recibir los archivos a aplicar reduce");
			pthread_exit((void*)0);
		}

		strcpy(archivoParaReduce->ip_nodo,archivoQueRecibo.ip_nodo);
		archivoParaReduce->puerto_nodo=archivoQueRecibo.puerto_nodo;
		strcpy(archivoParaReduce->archivoAAplicarReduce,archivoQueRecibo.archivoAAplicarReduce);

		list_add(listaArchivosReduce,archivoParaReduce);
	}

//	printf("Se aplicara reduce en los archivos:\n");
//
//	for(indice=0;indice<list_size(listaArchivosReduce);indice++){
//		t_archivosReduce *archivoPR;
//		archivoPR=list_get(listaArchivosReduce,indice);
//		printf("\t IP Nodo:%s\n",archivoPR->ip_nodo);
//		printf("\t Puerto Nodo:%d\n",archivoPR->puerto_nodo);
//		printf("\t Nombre archivo:%s\n",archivoPR->archivoAAplicarReduce);
//	}

	//RECORRO LA LISTA DE ARCHIVOS Y LLENO OTRA LISTA, CON FILE* SI ES ARCHIVO LOCAL; SOCKET SI ES REMOTO y UN BUF PARA AMBOS DONDE
	//IRIA LA NUEVA LINEA
	for(posicionEnListaArchivos=0;posicionEnListaArchivos<list_size(listaArchivosReduce);posicionEnListaArchivos++){
		t_archivosReduce *unArchivoReduce;
		unArchivoReduce=malloc(sizeof(t_archivosReduce));
		unArchivoReduce=list_get(listaArchivosReduce,posicionEnListaArchivos);
		if(strcmp(unArchivoReduce->ip_nodo,config_get_string_value(configurador,"IP_NODO"))==0  //SI ENTRA ACÁ ES UN ARCHIVO LOCAL
				&& unArchivoReduce->puerto_nodo==config_get_int_value(configurador,"PUERTO_NODO")){
			FILE * nuevoFileStream;
			t_archivoEnApareo *nuevoArchivoEnApareo=malloc(sizeof(t_archivoEnApareo));
			if((nuevoFileStream=fopen(unArchivoReduce->archivoAAplicarReduce,"r"))==NULL){
				perror("fopen:");
				respuestaParaJob.resultado=1;
				respuestaParaJob.puerto_nodoFallido=config_get_int_value(configurador,"PUERTO_NODO");
				strcpy(respuestaParaJob.ip_nodoFallido,config_get_string_value(configurador,"IP_NODO"));
				if(send(*sckReduce,&respuestaParaJob,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger,"Fallo el envio de la respues KO al Reduce");
				}
				pthread_exit((void*)0);
			}
			nuevoArchivoEnApareo->archivo=nuevoFileStream;
			memset(nuevoArchivoEnApareo->buffer,'\0',512);
			memset(nuevoArchivoEnApareo->nombreArchivo,'\0',TAM_NOMFINAL);
			nuevoArchivoEnApareo->socket=-1;
			strcpy(nuevoArchivoEnApareo->nombreArchivo,unArchivoReduce->archivoAAplicarReduce);
			nuevoArchivoEnApareo->endOfFile=0;
			nuevoArchivoEnApareo->posicionRenglon=0;
			memset(nuevoArchivoEnApareo->renglones,'\0',TAM_RENGLONES);
			memset(nuevoArchivoEnApareo->ip_nodo,'\0',20);
			strcpy(nuevoArchivoEnApareo->ip_nodo,config_get_string_value(configurador,"IP_NODO"));
			nuevoArchivoEnApareo->puerto_nodo=config_get_int_value(configurador,"PUERTO_NODO");
			printf("Agrego el archivo local %s\n",unArchivoReduce->archivoAAplicarReduce);
			list_add(archivosEnApareo,nuevoArchivoEnApareo);
		}

		//SI NO ENTRO EN EL IF, ES UN ARCHIVO REMOTO, HAY QUE BUSCARLO EN UN NODO

		else{
			int otroNodoSock;
			struct sockaddr_in nodo_addr;
			char identificacion[BUF_SIZE];
//			char nombreArchivo[TAM_NOMFINAL];
			t_archivoEnApareo *nuevoArchivoEnApareo=malloc(sizeof(t_archivoEnApareo));
//			memset(nombreArchivo,'\0',TAM_NOMFINAL);
			memset(identificacion,'\0',BUF_SIZE);
			if((otroNodoSock=socket(AF_INET,SOCK_STREAM,0))==-1){ //si función socket devuelve -1 es error
				perror("socket");
				log_error(logger,"Fallo la creación del socket (conexión nodo-nodo)");
				respuestaParaJob.resultado=1;
				respuestaParaJob.puerto_nodoFallido=config_get_int_value(configurador,"PUERTO_NODO");
				strcpy(respuestaParaJob.ip_nodoFallido,config_get_string_value(configurador,"IP_NODO"));
				if(send(*sckReduce,&respuestaParaJob,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger,"Fallo el envio de la respues KO al Reduce");
				}
				pthread_exit((void*)0);

			}
			nodo_addr.sin_family=AF_INET;
			nodo_addr.sin_port=htons(unArchivoReduce->puerto_nodo);
			nodo_addr.sin_addr.s_addr=inet_addr(unArchivoReduce->ip_nodo);
			memset(&(nodo_addr.sin_zero),'\0',8);
			if((connect(otroNodoSock,(struct sockaddr *)&nodo_addr,sizeof(struct sockaddr)))==-1){
				perror("connect");
				log_error(logger,"Fallo la conexión con el nodo");
				respuestaParaJob.resultado=1;
				respuestaParaJob.puerto_nodoFallido=unArchivoReduce->puerto_nodo;
				strcpy(respuestaParaJob.ip_nodoFallido,unArchivoReduce->ip_nodo);
				if(send(*sckReduce,&respuestaParaJob,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger,"Fallo el envio de la respues KO al Reduce");
				}
				pthread_exit((void*)0);

			}
			strcpy(identificacion,"soy nodo");
			if(send(otroNodoSock,identificacion,sizeof(identificacion),MSG_WAITALL)==-1){
				perror("send");
				log_error(logger,"Fallo el envío de identificación nodo-nodo");
				respuestaParaJob.resultado=1;
				respuestaParaJob.puerto_nodoFallido=unArchivoReduce->puerto_nodo;
				strcpy(respuestaParaJob.ip_nodoFallido,unArchivoReduce->ip_nodo);
				if(send(*sckReduce,&respuestaParaJob,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger,"Fallo el envio de la respues KO al Reduce");
				}
				pthread_exit((void*)0);
			}
			/*Conexión mapper-nodo establecida*/
			log_info(logger,"Nodo conectado al Nodo con IP: %s,en el Puerto: %d",unArchivoReduce->ip_nodo,unArchivoReduce->puerto_nodo);
//			strcpy(nombreArchivo,unArchivoReduce->archivoAAplicarReduce);
//			if(send(otroNodoSock,nombreArchivo,sizeof(nombreArchivo),MSG_WAITALL)==-1){
//				perror("send");
//				log_error(logger,"Fallo el envío de identificación nodo-nodo");
//			}
//			printf("Agrego el archivo remoto %s\n",nombreArchivo);
			nuevoArchivoEnApareo->archivo=NULL;
			memset(nuevoArchivoEnApareo->buffer,'\0',512);
			memset(nuevoArchivoEnApareo->nombreArchivo,'\0',TAM_NOMFINAL);
			nuevoArchivoEnApareo->socket=otroNodoSock;
			strcpy(nuevoArchivoEnApareo->nombreArchivo,unArchivoReduce->archivoAAplicarReduce);
			nuevoArchivoEnApareo->endOfFile=0;
			nuevoArchivoEnApareo->posicionRenglon=0;
			memset(nuevoArchivoEnApareo->renglones,'\0',TAM_RENGLONES);
			memset(nuevoArchivoEnApareo->ip_nodo,'\0',20);
			strcpy(nuevoArchivoEnApareo->ip_nodo,unArchivoReduce->ip_nodo);
			nuevoArchivoEnApareo->puerto_nodo=unArchivoReduce->puerto_nodo;
			list_add(archivosEnApareo,nuevoArchivoEnApareo);
		}
	}



	pthread_mutex_lock(&mutexReduce);
	ejecutarReduce(archivosEnApareo,nombreNuevoReduce,nombreFinalReduce,sckReduce);
	pthread_mutex_unlock(&mutexReduce);

	//Si llego hasta acá --> El reduce termino OK, enviar respuesta al Job

	respuestaParaJob.resultado=0;
	strcpy(respuestaParaJob.ip_nodoFallido,config_get_string_value(configurador,"IP_NODO")); //NO ES FALLIDO EN REALIDAD
	respuestaParaJob.puerto_nodoFallido=config_get_int_value(configurador,"PUERTO_NODO"); //NO ES FALLIDO EN REALIDAD
	if(send(*sckReduce,&respuestaParaJob,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envio de la respuesta OK al hilo reduce");
		pthread_exit((void*)0);
	}

	pthread_exit((void*)0);
}

void ejecutarReduce(t_list* archivosApareando,char* script,char* resultado, int* sckReduce){
	int posicionLista;
	int posicionDelMenor=0;
	int byteActual=0;
	int cantidadArchivos=list_size(archivosApareando);
	char dameRenglones[BUF_SIZE];
	char bufferMenor[512];
	char renglonTemporal[512];
	memset(renglonTemporal,'\0',512);
	memset(dameRenglones,'\0',BUF_SIZE);
	strcpy(dameRenglones,"Dame renglones");
	//Leer una linea de cada uno, guardar en el buffer
	int outfd[2];
	int bak,pid,archivo_resultado;
	bak=0;
	char *path;
//	sem_t terminoElReduce;
	t_respuestaNodoReduce respuestaNR;
	memset(respuestaNR.ip_nodoFallido,'\0',20);
//	sem_init(&terminoElReduce,0,1);
	pipe(outfd); /* Donde escribe el padre */
	if((pid=fork())==-1){
		perror("fork reduce");
	}
	else if(pid==0)
	{
		archivo_resultado=open(resultado,O_RDWR|O_CREAT,S_IRWXU|S_IRWXG); //abro file resultado, si no esta lo crea, asigno permisos
		fflush(stdout);
		bak=dup(STDOUT_FILENO);
		dup2(archivo_resultado,STDOUT_FILENO); //STDOUT de este proceso se grabara en el file resultado
		close(archivo_resultado);
		close(STDIN_FILENO);
		dup2(outfd[0], STDIN_FILENO); //STDIN de este proceso será STDOUT del proceso padre
		close(outfd[0]); /* innecesarios para el hijo */
		close(outfd[1]);
		path=string_new();
		string_append(&path,config_get_string_value(configurador,"PATHREDUCERS"));
		string_append(&path,"/");
		string_append(&path,script);
		execlp(path,script,NULL); //Ejecuto el script
//		sem_post(&terminoElReduce);
	}
	else
	{
		close(outfd[0]); /* Estan siendo usados por el hijo */

		//--- AGARRO LA PRIMER LINEA DE CADA ARCHIVO ---//

		for(posicionLista=0;posicionLista<cantidadArchivos;posicionLista++){
			t_archivoEnApareo* unArchivo;//=malloc(sizeof(t_archivoEnApareo));
			unArchivo=list_get(archivosApareando,posicionLista);
			if(unArchivo->archivo==NULL){ //Si es un archivo Remoto
				if(send(unArchivo->socket,dameRenglones,sizeof(dameRenglones),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger,"Fallo al enviar \"dame renglones\" a otro nodo");
					respuestaNR.resultado=1;
					respuestaNR.puerto_nodoFallido=unArchivo->puerto_nodo;
					strcpy(respuestaNR.ip_nodoFallido,unArchivo->ip_nodo);
					if(send(*sckReduce,&respuestaNR,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
						perror("send");
						log_error(logger,"Fallo el envio de la respues KO al Reduce");
					}
					pthread_exit((void*)0);
				}
				if(send(unArchivo->socket,unArchivo->nombreArchivo,sizeof(unArchivo->nombreArchivo),MSG_WAITALL)==-1){
					perror("send");
					log_error(logger,"Fallo el envio del nombre del archivo a conseguir renglones hacia otro nodo");
					respuestaNR.resultado=1;
					respuestaNR.puerto_nodoFallido=unArchivo->puerto_nodo;
					strcpy(respuestaNR.ip_nodoFallido,unArchivo->ip_nodo);
					if(send(*sckReduce,&respuestaNR,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
						perror("send");
						log_error(logger,"Fallo el envio de la respues KO al Reduce");
					}
					pthread_exit((void*)0);
				}
				if(recv(unArchivo->socket,unArchivo->renglones,sizeof(unArchivo->renglones),MSG_WAITALL)<=0){
					perror("recv");
					log_info(logger,"Se desconecto el nodo con IP %s cuyo puerto de escucha es %d",unArchivo->ip_nodo,unArchivo->puerto_nodo);
					log_error(logger,"Fallo al recibir los renglones de un archivo desde otro nodo");
					respuestaNR.resultado=1;
					respuestaNR.puerto_nodoFallido=unArchivo->puerto_nodo;
					strcpy(respuestaNR.ip_nodoFallido,unArchivo->ip_nodo);
					if(send(*sckReduce,&respuestaNR,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
						perror("send");
						log_error(logger,"Fallo el envio de la respues KO al Reduce");
					}
					pthread_exit((void*)0);
				}
				if(strcmp(unArchivo->renglones,"Llego al EOF\n")==0){
					unArchivo->endOfFile=1;
				}else{
					//Busco el primer renglon que tenga el buffer desde la posicion en la que está (empieza en 0)
					int posicionActual;
					int i=0;
					posicionActual=unArchivo->posicionRenglon;
					memset(unArchivo->buffer,'\0',512);
					memset(renglonTemporal,'\0',512);
					for(byteActual=posicionActual;byteActual!=TAM_RENGLONES+100;byteActual++){ //1000 sera condicion de corte
						renglonTemporal[i]=unArchivo->renglones[byteActual];
						if(unArchivo->renglones[byteActual]=='\n'){
							strcpy(unArchivo->buffer,renglonTemporal);
							unArchivo->posicionRenglon=byteActual+1;
							byteActual=TAM_RENGLONES+99;
						}
						i++;
//						if(unArchivo->renglones[byteActual]=='\0'){
//							//LLEGO AL FINAL EL BUFFER DE RENGLONES
//						}
					}
				}
				//	printf("Recibi del nodo el renglon: %s",unArchivo->buffer);
			}
			else{ //es un archivo local
				fgets(unArchivo->buffer,512,unArchivo->archivo);
				if(feof(unArchivo->archivo)){
					unArchivo->endOfFile=1;
					fclose(unArchivo->archivo);
				}
				//	printf("El renglon del archivo local es:%s",unArchivo->buffer);
			}
		}

		//Comparar entretodos los buffer el menor mientras que haya algún archivo abierto
		while(list_any_satisfy(archivosApareando,(void*)no_llego_a_eof)){

			memset(bufferMenor,'\0',512);
			t_archivoEnApareo* unArchivo;//=malloc(sizeof(t_archivoEnApareo));
			t_archivoEnApareo* otroArchivo;
			unArchivo=list_find(archivosApareando,(void*)no_llego_a_eof); // Considero como menor al primer archivo
			strcpy(bufferMenor,unArchivo->buffer);
			for(posicionLista=0;posicionLista<cantidadArchivos;posicionLista++){
				otroArchivo=list_get(archivosApareando,posicionLista);
				if(strcmp(unArchivo->nombreArchivo,otroArchivo->nombreArchivo)==0){
					posicionDelMenor=posicionLista;
				}
			}
			for(posicionLista=0;posicionLista<cantidadArchivos;posicionLista++){
				unArchivo=list_get(archivosApareando,posicionLista);
				if (strcmp(unArchivo->buffer,bufferMenor)<0 && unArchivo->endOfFile==0){
					strcpy(bufferMenor,unArchivo->buffer);
					posicionDelMenor=posicionLista;
				}
			}

			//El menor, escribirlo en stdin
			//printf("El menor es: %s",bufferMenor);
			write(outfd[1],bufferMenor,strlen(bufferMenor));/* Escribe en el stdin del hijo el contenido del bloque*/


			//Leer la proxima linea del menor
			//t_archivoEnApareo* unArchivo;
			unArchivo=list_get(archivosApareando,posicionDelMenor);
			if(unArchivo->archivo==NULL){ //Si es un archivo Remoto

				if(unArchivo->renglones[unArchivo->posicionRenglon]=='\0'){ //Leyo todos los renglones, hay que pedir mas
					memset(unArchivo->renglones,'\0',TAM_RENGLONES);
					unArchivo->posicionRenglon=0;
					if(send(unArchivo->socket,dameRenglones,sizeof(dameRenglones),MSG_WAITALL)==-1){
						perror("send");
						log_error(logger,"Fallo al enviar \"dame renglones\" a otro nodo");
						respuestaNR.resultado=1;
						respuestaNR.puerto_nodoFallido=unArchivo->puerto_nodo;
						strcpy(respuestaNR.ip_nodoFallido,unArchivo->ip_nodo);
						if(send(*sckReduce,&respuestaNR,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
							perror("send");
							log_error(logger,"Fallo el envio de la respues KO al Reduce");
						}
						pthread_exit((void*)0);
					}
					if(send(unArchivo->socket,unArchivo->nombreArchivo,sizeof(unArchivo->nombreArchivo),MSG_WAITALL)==-1){
						perror("send");
						log_error(logger,"Fallo el envio del nombre del archivo a conseguir renglones hacia otro nodo");
						respuestaNR.resultado=1;
						respuestaNR.puerto_nodoFallido=unArchivo->puerto_nodo;
						strcpy(respuestaNR.ip_nodoFallido,unArchivo->ip_nodo);
						if(send(*sckReduce,&respuestaNR,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
							perror("send");
							log_error(logger,"Fallo el envio de la respues KO al Reduce");
						}
						pthread_exit((void*)0);
					}
					if(recv(unArchivo->socket,unArchivo->renglones,sizeof(unArchivo->renglones),MSG_WAITALL)<=0){
						perror("recv");
						log_info(logger,"Se desconecto el nodo con IP %s cuyo puerto de escucha es %d",unArchivo->ip_nodo,unArchivo->puerto_nodo);
						log_error(logger,"Fallo al recibir los renglones de un archivo desde otro nodo");
						respuestaNR.resultado=1;
						respuestaNR.puerto_nodoFallido=unArchivo->puerto_nodo;
						strcpy(respuestaNR.ip_nodoFallido,unArchivo->ip_nodo);
						if(send(*sckReduce,&respuestaNR,sizeof(t_respuestaNodoReduce),MSG_WAITALL)==-1){
							perror("send");
							log_error(logger,"Fallo el envio de la respues KO al Reduce");
						}
						pthread_exit((void*)0);
					}
					if(strcmp(unArchivo->renglones,"Llego al EOF\n")==0){
						unArchivo->endOfFile=1;
					}
				}
				int posicionActual;
				int i=0;
				posicionActual=unArchivo->posicionRenglon;
				memset(unArchivo->buffer,'\0',512);
				memset(renglonTemporal,'\0',512);
				for(byteActual=posicionActual;byteActual!=TAM_RENGLONES+100;byteActual++){ //1000 sera condicion de corte
					renglonTemporal[i]=unArchivo->renglones[byteActual];
					if(unArchivo->renglones[byteActual]=='\n'){
						strcpy(unArchivo->buffer,renglonTemporal);
						unArchivo->posicionRenglon=byteActual+1;
						byteActual=TAM_RENGLONES+99;
					}
					i++;
				}
			}
			else{ //es un archivo local
				fgets(unArchivo->buffer,512,unArchivo->archivo);
				//	printf("El renglon del archivo local es:%s",unArchivo->buffer);
				if(feof(unArchivo->archivo)){
					unArchivo->endOfFile=1;
					fclose(unArchivo->archivo);
				}
			}
			//cuando alguno de los archivos sea EOF, se tiene que cerrar (fclose ya sea en el nodo o local)
		}
		close(outfd[1]);
		waitpid(pid,NULL,0);
		dup2(bak,STDOUT_FILENO);
//		sem_wait(&terminoElReduce);
	}


	printf("Llegaron todos al EOF - Termino el reduce \n");


}

int no_llego_a_eof(t_archivoEnApareo* archivo){
	return (archivo->endOfFile==0);
}



//char* crearBloqueFalso(){
//	int posi,j;
//	posi=0;
//	j=0;
//	char* bloqueRetorno;
//	bloqueRetorno=malloc(BLOCK_SIZE);
//	for(j=0;j<262144;j++){ // Mete 262144 veces 80 caracteres , 80 B --> 80*262144 = 20971520 = 20MB
//		//metera lineas separadas de :
//		//Man,Hola,Prueba,H,H,H,H,H,H,H,H,H,Map,H\n
//		//Van,Esta,Antess,H,H,H,H,H,H,H,H,H,Sor,H\n
//		bufFalso[posi]='M';posi++;
//		bufFalso[posi]='a';posi++;
//		bufFalso[posi]='n';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]='o';posi++;
//		bufFalso[posi]='l';posi++;
//		bufFalso[posi]='a';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='P';posi++;
//		bufFalso[posi]='r';posi++;
//		bufFalso[posi]='u';posi++;
//		bufFalso[posi]='e';posi++;
//		bufFalso[posi]='b';posi++;
//		bufFalso[posi]='a';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='M';posi++;
//		bufFalso[posi]='a';posi++;
//		bufFalso[posi]='p';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]='\n';posi++;
//		bufFalso[posi]='V';posi++;
//		bufFalso[posi]='a';posi++;
//		bufFalso[posi]='n';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='E';posi++;
//		bufFalso[posi]='s';posi++;
//		bufFalso[posi]='t';posi++;
//		bufFalso[posi]='a';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='A';posi++;
//		bufFalso[posi]='n';posi++;
//		bufFalso[posi]='t';posi++;
//		bufFalso[posi]='e';posi++;
//		bufFalso[posi]='s';posi++;
//		bufFalso[posi]='s';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='S';posi++;
//		bufFalso[posi]='o';posi++;
//		bufFalso[posi]='r';posi++;
//		bufFalso[posi]=',';posi++;
//		bufFalso[posi]='H';posi++;
//		bufFalso[posi]='\n';posi++;
//	}
//	//pongo los ultimos 40 bytes con \0 (saco la ultima linea)
//	bufFalso[20971480]='\0';posi++;
//	bufFalso[20971481]='\0';posi++;
//	bufFalso[20971482]='\0';posi++;
//	bufFalso[20971483]='\0';posi++;
//	bufFalso[20971484]='\0';posi++;
//	bufFalso[20971485]='\0';posi++;
//	bufFalso[20971486]='\0';posi++;
//	bufFalso[20971487]='\0';posi++;
//	bufFalso[20971488]='\0';posi++;
//	bufFalso[20971489]='\0';posi++;
//	bufFalso[20971490]='\0';posi++;
//	bufFalso[20971491]='\0';posi++;
//	bufFalso[20971492]='\0';posi++;
//	bufFalso[20971493]='\0';posi++;
//	bufFalso[20971494]='\0';posi++;
//	bufFalso[20971495]='\0';posi++;
//	bufFalso[20971496]='\0';posi++;
//	bufFalso[20971497]='\0';posi++;
//	bufFalso[20971498]='\0';posi++;
//	bufFalso[20971499]='\0';posi++;
//	bufFalso[20971500]='\0';posi++;
//	bufFalso[20971501]='\0';posi++;
//	bufFalso[20971502]='\0';posi++;
//	bufFalso[20971503]='\0';posi++;
//	bufFalso[20971504]='\0';posi++;
//	bufFalso[20971505]='\0';posi++;
//	bufFalso[20971506]='\0';posi++;
//	bufFalso[20971507]='\0';posi++;
//	bufFalso[20971508]='\0';posi++;
//	bufFalso[20971509]='\0';posi++;
//	bufFalso[20971510]='\0';posi++;
//	bufFalso[20971511]='\0';posi++;
//	bufFalso[20971512]='\0';posi++;
//	bufFalso[20971513]='\0';posi++;
//	bufFalso[20971514]='\0';posi++;
//	bufFalso[20971515]='\0';posi++;
//	bufFalso[20971516]='\0';posi++;
//	bufFalso[20971517]='\0';posi++;
//	bufFalso[20971518]='\0';posi++;
//	bufFalso[20971519]='\0';posi++;
//	strcpy(bloqueRetorno,bufFalso);
//	return bloqueRetorno;
//}
//
//char* crearBloqueAMediasFalso(){
//	int posi,j;
//	posi=0;
//	j=0;
//	char* bloqueRetorno;
//	bloqueRetorno=malloc(BLOCK_SIZE/2);
//	for(j=0;j<131072;j++){ // Mete 131072 veces 80 caracteres , 80 B --> 80*131072 = 20971520 = 10MB
//		//metera lineas separadas de :
//		//Man,Hola,Prueba,H,H,H,H,H,H,H,H,H,Map,H\n
//		//Van,Esta,Antess,H,H,H,H,H,H,H,H,H,Sor,H\n
//		bufAMediasFalso[posi]='M';posi++;
//		bufAMediasFalso[posi]='a';posi++;
//		bufAMediasFalso[posi]='n';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]='o';posi++;
//		bufAMediasFalso[posi]='l';posi++;
//		bufAMediasFalso[posi]='a';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='P';posi++;
//		bufAMediasFalso[posi]='r';posi++;
//		bufAMediasFalso[posi]='u';posi++;
//		bufAMediasFalso[posi]='e';posi++;
//		bufAMediasFalso[posi]='b';posi++;
//		bufAMediasFalso[posi]='a';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='M';posi++;
//		bufAMediasFalso[posi]='a';posi++;
//		bufAMediasFalso[posi]='p';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]='\n';posi++;
//		bufAMediasFalso[posi]='V';posi++;
//		bufAMediasFalso[posi]='a';posi++;
//		bufAMediasFalso[posi]='n';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='E';posi++;
//		bufAMediasFalso[posi]='s';posi++;
//		bufAMediasFalso[posi]='t';posi++;
//		bufAMediasFalso[posi]='a';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='A';posi++;
//		bufAMediasFalso[posi]='n';posi++;
//		bufAMediasFalso[posi]='t';posi++;
//		bufAMediasFalso[posi]='e';posi++;
//		bufAMediasFalso[posi]='s';posi++;
//		bufAMediasFalso[posi]='s';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='S';posi++;
//		bufAMediasFalso[posi]='o';posi++;
//		bufAMediasFalso[posi]='r';posi++;
//		bufAMediasFalso[posi]=',';posi++;
//		bufAMediasFalso[posi]='H';posi++;
//		bufAMediasFalso[posi]='\n';posi++;
//	}
//	//pongo los ultimos 40 bytes con \0 (saco la ultima linea)
//	bufAMediasFalso[20971480]='\0';posi++;
//	bufAMediasFalso[20971481]='\0';posi++;
//	bufAMediasFalso[20971482]='\0';posi++;
//	bufAMediasFalso[20971483]='\0';posi++;
//	bufAMediasFalso[20971484]='\0';posi++;
//	bufAMediasFalso[20971485]='\0';posi++;
//	bufAMediasFalso[20971486]='\0';posi++;
//	bufAMediasFalso[20971487]='\0';posi++;
//	bufAMediasFalso[20971488]='\0';posi++;
//	bufAMediasFalso[20971489]='\0';posi++;
//	bufAMediasFalso[20971490]='\0';posi++;
//	bufAMediasFalso[20971491]='\0';posi++;
//	bufAMediasFalso[20971492]='\0';posi++;
//	bufAMediasFalso[20971493]='\0';posi++;
//	bufAMediasFalso[20971494]='\0';posi++;
//	bufAMediasFalso[20971495]='\0';posi++;
//	bufAMediasFalso[20971496]='\0';posi++;
//	bufAMediasFalso[20971497]='\0';posi++;
//	bufAMediasFalso[20971498]='\0';posi++;
//	bufAMediasFalso[20971499]='\0';posi++;
//	bufAMediasFalso[20971500]='\0';posi++;
//	bufAMediasFalso[20971501]='\0';posi++;
//	bufAMediasFalso[20971502]='\0';posi++;
//	bufAMediasFalso[20971503]='\0';posi++;
//	bufAMediasFalso[20971504]='\0';posi++;
//	bufAMediasFalso[20971505]='\0';posi++;
//	bufAMediasFalso[20971506]='\0';posi++;
//	bufAMediasFalso[20971507]='\0';posi++;
//	bufAMediasFalso[20971508]='\0';posi++;
//	bufAMediasFalso[20971509]='\0';posi++;
//	bufAMediasFalso[20971510]='\0';posi++;
//	bufAMediasFalso[20971511]='\0';posi++;
//	bufAMediasFalso[20971512]='\0';posi++;
//	bufAMediasFalso[20971513]='\0';posi++;
//	bufAMediasFalso[20971514]='\0';posi++;
//	bufAMediasFalso[20971515]='\0';posi++;
//	bufAMediasFalso[20971516]='\0';posi++;
//	bufAMediasFalso[20971517]='\0';posi++;
//	bufAMediasFalso[20971518]='\0';posi++;
//	bufAMediasFalso[20971519]='\0';posi++;
//	strcpy(bloqueRetorno,bufAMediasFalso);
//	return bloqueRetorno;
//}
//
//void crearArchivoFalso(){
//	FILE* archivoFalso;
//	char* path;
//	char* bloqueFalso;
//	char* bloqueAMediasFalso;
//	bloqueFalso=malloc(BLOCK_SIZE);
//	bloqueAMediasFalso=malloc((BLOCK_SIZE/2));
//	path=string_new();
//	string_append(&path,"/tmp/archivoFalso.txt");
//	archivoFalso=fopen(path,"w+");
//	bloqueFalso=crearBloqueFalso();
//	bloqueAMediasFalso=crearBloqueAMediasFalso();
//	fprintf(archivoFalso,"%s",bloqueFalso);
//	fprintf(archivoFalso,"%s",bloqueFalso);
//	fprintf(archivoFalso,"%s",bloqueAMediasFalso);
//	fclose(archivoFalso);
//}
