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

#define BLOCK_SIZE 20971520 //block size 20MB
#define BUF_SIZE 50

//Declaración de funciones
char* mapearFileDeDatos();
void setBloque(int bloque,char* datos);
char* getBloque(int bloque);
char* getFileContent(char* nombre);
void* manejador_de_escuchas(); //Hilo que va a manejar las conexiones
int estaEnListaNodos(int socket);
int estaEnListaMappers(int socket);
int estaEnListaReducers(int socket);


//Declaración de variables Globales
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
struct sockaddr_in remote_client; //Direccion del cliente que se conectará
char mensaje[BUF_SIZE]; //Mensaje que recivirá de los clientes
t_list* listaNodosConectados; //Lista con los nodos conectados
t_list* listaMappersConectados; //Lista con los mappers conectados
t_list* listaReducersConectados; //lista con los reducers conectados
int* socketNodo; //para identificar los que son nodos conectados
int* socketMapper; //para identificar los que son mappers conectados
int* socketReducer; //para identificar los que son reducers conectados

int main(int argc , char *argv[]){

	//-------------------------- Cuerpo ppal del programa ---------------------------------------
	//------------ Variables locales a la funcion main --------------------

	pthread_t escucha; // Hilo que va a escuchar nuevas conexiones
	configurador= config_create("resources/nodoConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
	logger = log_create("./nodoLog.log", "Nodo", true, LOG_LEVEL_INFO);
	logger_archivo = log_create("./nodoLog.log", "Nodo", false, LOG_LEVEL_INFO);
	fileDeDatos=mapearFileDeDatos();//La siguiente función va a mapear el archivo de datos que esta especificado en el archivo conf a memoria, y asignarle al puntero fileDeDatos la direccion donde arranca el file. Utilizando mmap()
	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	int yes=1; // para setsockopt() SO_REUSEADDR, más abajo
	char identificacion[BUF_SIZE]; //para el mensaje que envie al conectarse para identificarse, puede cambiar
	//char bloquesTotales[2]; //tendra la cantidad de bloques totales del file de datos
	int *bloquesTotales;
	struct sockaddr_in filesystem; //direccion del fs a donde se conectará
	struct sockaddr_in nodoAddr; //direccion del nodo que será servidor
	memset(&filesystem, 0, sizeof(filesystem));
	listaNodosConectados=list_create();
	listaMappersConectados=list_create();
	listaReducersConectados=list_create();

	//sprintf(bloquesTotales,"%d",sizeFileDatos/20971520);
	bloquesTotales=malloc(sizeof(int));
	*bloquesTotales=sizeFileDatos/20971520;

	//Estructura para conexion con FS
	filesystem.sin_family = AF_INET;
	filesystem.sin_addr.s_addr = inet_addr(config_get_string_value(configurador,"IP_FS"));
	filesystem.sin_port = htons(config_get_int_value(configurador,"PUERTO_FS"));
	//-------------------------------

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
			if((send(conectorFS,identificacion,sizeof(identificacion),0))==-1) {
					perror("send");
					log_error(logger,"FALLO el envio del saludo al FS");
					exit(-1);
			}
			//envio cantidad de bloques totales
			if((send(conectorFS,bloquesTotales,sizeof(int),0))==-1){
				perror("send");
				log_error(logger,"FALLO el envío de la cantidad de bloques totales al FS");
				exit(-1);
			}
		}
		else {
			//si el if da falso por nodo existente que se esta reconectando
			strcpy(identificacion,"reconectado");
			if((send(conectorFS,identificacion,sizeof(identificacion),0))==-1) {
					perror("send");
					log_error(logger,"FALLO el envio del saludo al FS");
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
	if (listen(listener, 10) == -1) {
		perror("listen");
		log_error(logger,"FALLO el Listen");
		exit(1);
	}
	FD_SET(listener,&master); //Agrego al listener al conjunto maestro
	fdmax=listener; //el fd máximo hasta el momento es el listener
	printf("Está escuchando conexiones");

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
	int socketModificado,nbytes,newfd,addrlen;
	while(1) {
		read_fds = master;
		if (select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1) {
			perror("select");
			log_error(logger,"FALLO el Select");
			exit(-1);
		}
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
						if((nbytes=recv(newfd,mensaje,sizeof(mensaje),0))<=0){ //error
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
								log_info(logger,"Se conectó el nodo %s",inet_ntoa(remote_client.sin_addr));
							}
							if(nbytes>0 && strncmp(mensaje,"soy mapper",11)==0){
								//se conectó un hilo mapper
								socketMapper=malloc(sizeof(int));
								*socketMapper=newfd;
								list_add(listaMappersConectados,socketMapper); //agrego el nuevo socket a la lista de Mappers conectados
								FD_SET(newfd,&master); //añadir al conjunto maestro
								if(newfd>fdmax){ //actualizar el máximo
									fdmax=newfd;
								}
								log_info(logger,"Se conectó un hilo mapper desde %s",inet_ntoa(remote_client.sin_addr));
							}
							if(nbytes>0 && strncmp(mensaje,"soy reducer",12)==0){
								//se conectó un hilo reducer
								socketReducer=malloc(sizeof(int));
								*socketReducer=newfd;
								list_add(listaReducersConectados,socketReducer); //agrego el nuevo socket a la lista de Reducers conectados
								FD_SET(newfd,&master); //añadir al conjunto maestro
								if(newfd>fdmax){ //actualizar el máximo
									fdmax=newfd;
								}
								log_info(logger,"Se conectó un hilo reducer desde %s",inet_ntoa(remote_client.sin_addr));
							}
						}
					}

					/*-- Conexión con el fileSystem --*/

					if(socketModificado==conectorFS){
						if ((nbytes=recv(conectorFS,mensaje,sizeof(mensaje),0))==-1){ //da error
							perror("recv");
							log_error(logger,"Falló el receive");
							exit(-1);
						}
						if(nbytes==0){ //se desconectó
							close(conectorFS);
							FD_CLR(conectorFS,&master);
						}
						else{

						/* -- el filesystem envío un mensaje a tratar -- */

						}
					}

					//-- Conexión con otro nodo --//

					if(estaEnListaNodos(socketModificado)==0){
						if ((nbytes=recv(socketModificado,mensaje,sizeof(mensaje),0))==-1){ //da error
							perror("recv");
							log_error(logger,"Falló el receive");
							exit(-1);
						}
						if(nbytes==0){ //se desconectó
							close(socketModificado);
							FD_CLR(socketModificado,&master);
						}
						else{

							/* -- el nodo envío un mensaje a tratar -- */

						}
					}

					//-- Conexión con hilo mapper --//

					if(estaEnListaMappers(socketModificado)==0){
						if ((nbytes=recv(socketModificado,mensaje,sizeof(mensaje),0))==-1){ //da error
							perror("recv");
							log_error(logger,"Falló el receive");
							exit(-1);
						}
						if(nbytes==0){ //se desconectó
							close(socketModificado);
							FD_CLR(socketModificado,&master);
						}
						else{

							/* -- el mapper envío un mensaje a tratar -- */

						}
					}

					//-- Conexión con hilo reducer --//

					if(estaEnListaReducers(socketModificado)==0){
						if ((nbytes=recv(socketModificado,mensaje,sizeof(mensaje),0))==-1){ //da error
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
	log_info(logger_archivo,"Se escribió el bloque %d",numBloque);
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
	log_info(logger_archivo,"Se leyó el bloque %d",numBloque);
	return datosLeidos;
}

char* getFileContent(char* nombreFile){
	char* path;
	char* fileMapeado;
	int fileDescriptor;
	struct stat estadoDelFile; //declaro una estructura que guarda el estado de un archivo
	path=strdup("");
	strcpy(path,config_get_string_value(configurador,"DIR_TEMP"));
	strcat(path,"/");
	strcat(path,nombreFile);
	fileDescriptor = open(path,O_RDWR);
		/*Chequeo de apertura del file exitosa*/
			if (fileDescriptor==-1){
				perror("open");
				log_error(logger,"Fallo la apertura del file de datos");
				exit(-1);
			}
	if(fstat(fileDescriptor,&estadoDelFile)==-1){//guardo el estado del archivo de datos en la estructura
			perror("fstat");
			log_error(logger,"Falló el fstat");
		}
	fileMapeado=mmap(0,estadoDelFile.st_size,(PROT_WRITE|PROT_READ|PROT_EXEC),MAP_SHARED,fileDescriptor,0);
	/*Chequeo de mmap exitoso*/
		if (fileMapeado==MAP_FAILED){
			perror("mmap");
			log_error(logger,"Falló el mmap, no se pudo asignar la direccion de memoria para el archivo solicitado");
			exit(-1);
		}
	close(fileDescriptor); //Cierro el archivo
	log_info(logger_archivo,"Fue leído el archivo /tmp/%s",nombreFile);
	return fileMapeado;
}
