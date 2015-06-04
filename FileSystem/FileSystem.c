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
#include "FS_MDFS.h"
#include <commons/log.h>
#include <commons/config.h>
#include <commons/string.h>

//Includes para mongo
#include <bson.h>
#include <mongoc.h>

#define BUF_SIZE 50
#define BLOCK_SIZE 20971520
#define MENSAJE_SIZE 4096
#define MAX_DIRECTORIOS 1024

//Prototipos de Funciones
int Menu();
void DibujarMenu();
void *connection_handler_escucha(); // Esta funcion escucha continuamente si recibo nuevos mensajes
static t_nodo *agregar_nodo_a_lista(int socket,int est,char *ip, int port,int puerto_escucha, int bloques_lib, int bloques_tot);
void modificar_estado_nodo (int socket,char *ip,int port,int estado);
void listar_nodos_conectados(t_list *nodos);
char *obtener_md5(char *archivo);
void formatear_nodos(void);
void FormatearFilesystem ();		//Pame TODAVIA NO DESARROLLADA
void EliminarArchivo();				//DESARROLLADA
void RenombrarArchivo ();			//DESARROLLADA
void MoverArchivo();				//DESARROLLADA
void CrearDirectorio();				//DESARROLLADA, falta persistencia
void EliminarDirectorio();			//DESARROLLADA, falta persistencia
void RenombrarDirectorio();			//DESARROLLADA, falta persistencia
void MoverDirectorio();				//DESARROLLADA, falta persistencia
void CopiarArchivoAMDFS();			//Pame TODAVIA NO DESARROLLADA
void CopiarArchivoDelMDFS();		//Pame TODAVIA NO DESARROLLADA
void MD5DeArchivo();				//Pame TODAVIA NO DESARROLLADA
void VerBloques();					//Andy TODAVIA NO DESARROLLADA
void BorrarBloques();				//Andy TODAVIA NO DESARROLLADA
void CopiarBloques();				//Andy TODAVIA NO DESARROLLADA
void AgregarNodo();					//Andy TODAVIA NO DESARROLLADA
void EliminarNodo();  				//Andy TODAVIA NO DESARROLLADA
uint32_t BuscarArchivoPorNombre (); //DESARROLLADA
uint32_t BuscarPadre ();            //DESARROLLADA
static void eliminar_bloques(t_bloque *bloque);
long ExisteEnLaLista(t_list* listaDirectorios, char* nombreDirectorioABuscar, uint32_t idPadre);
int BuscarMenorIndiceLibre (char indiceDirectorios[]);
static void directorio_destroy(t_dir* self);
static void archivo_destroy(t_archivo* self);

fd_set master; // conjunto maestro de descriptores de fichero
fd_set read_fds; // conjunto temporal de descriptores de fichero para select()
t_log* logger;
t_list *nodos; //lista de nodos conectados al fs
t_list* archivos; //lista de archivos del FS
t_list* directorios; //lista de directorios del FS
t_bloque* bloque; //Un Bloque del Archivo
t_config * configurador;
t_archivo* unArchivo; //Un archivo de la lista de archivos del FS
int fdmax; // número máximo de descriptores de fichero
int listener; // descriptor de socket a la escucha
struct sockaddr_in filesystem; // dirección del servidor
struct sockaddr_in remote_client; // dirección del cliente
char identificacion[BUF_SIZE]; // buffer para datos del cliente
char mensaje[MENSAJE_SIZE];
int cantidad_nodos=0;
int cantidad_nodos_historico=0;
int read_size;
int *bloquesTotales; //tendra la cantidad de bloques totales del file de datos
int marta_presente=0; //Valiable para controlar que solo 1 proceso marta se conecte
int marta_sock;
char indiceDirectorios[MAX_DIRECTORIOS]; //cantidad maxima de directorios
int directoriosDisponibles; //reservo raiz
int j; //variable para recorrer el vector de indices
int *puerto_escucha_nodo;

//Variables para la persistencia con mongo
mongoc_client_t *client;
mongoc_collection_t *collection;
mongoc_cursor_t *cursor;
bson_error_t error;
bson_oid_t oid;
bson_t *doc;


int main(int argc , char *argv[]){

	pthread_t escucha; //Hilo que va a manejar los mensajes recibidos
	int newfd;
	int addrlen;
	int yes=1; // para setsockopt() SO_REUSEADDR, más abajo
	configurador= config_create("resources/fsConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
	logger=log_create("fsLog.log","FileSystem",false,LOG_LEVEL_INFO);
	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	mongoc_init ();
	client = mongoc_client_new ("mongodb://localhost:27017/");
	collection = mongoc_client_get_collection (client, "NODOS", "lista_nodos");
	bson_oid_init (&oid, NULL);

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
	filesystem.sin_family = AF_INET;
	filesystem.sin_addr.s_addr = INADDR_ANY;
	filesystem.sin_port = htons(config_get_int_value(configurador,"PUERTO_LISTEN"));
	memset(&(filesystem.sin_zero), '\0', 8);
	if (bind(listener, (struct sockaddr *)&filesystem, sizeof(filesystem)) == -1) {
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
	// añadir listener al conjunto maestro
	FD_SET(listener, &master);

	// seguir la pista del descriptor de fichero mayor
	fdmax = listener; // por ahora es éste el ultimo socket
	addrlen = sizeof(struct sockaddr_in);
	nodos=list_create(); //Crea la lista de que va a manejar la lista de nodos
	printf ("Esperando las conexiones de los nodos iniciales\n");
	log_info(logger,"Esperando las conexiones de los nodos iniciales");
	while (cantidad_nodos != config_get_int_value(configurador,"CANTIDAD_NODOS")){
		if ((newfd = accept(listener, (struct sockaddr*)&remote_client, (socklen_t*)&addrlen)) == -1) {
			perror ("accept");
			log_error(logger,"FALLO el ACCEPT");
		   	exit (-1);
		}
		if ((read_size = recv(newfd, identificacion , 50 , 0))==-1) {
			perror ("recv");
			log_error(logger,"FALLO el RECV");
			exit (-1);
		}
		if (read_size > 0 && strncmp(identificacion,"nuevo",5)==0){
			cantidad_nodos++;
			cantidad_nodos_historico=cantidad_nodos;
			FD_SET(newfd, &master);
			if (newfd>fdmax) fdmax = newfd;
			bloquesTotales=malloc(sizeof(int));
			//Segundo recv, aca espero recibir la capacidad del nodo
			if ((read_size = recv(newfd, bloquesTotales ,sizeof(int) , 0))==-1) {
				perror ("recv");
				log_error(logger,"FALLO el RECV");
				exit (-1);
			}
			puerto_escucha_nodo=malloc(sizeof(int));
			if ((read_size = recv(newfd, puerto_escucha_nodo ,sizeof(int) , 0))==-1) {
				perror ("recv");
				log_error(logger,"FALLO el RECV");
				exit (-1);
			}
			if (read_size > 0){
				list_add (nodos, agregar_nodo_a_lista(newfd,0,inet_ntoa(remote_client.sin_addr),remote_client.sin_port,*puerto_escucha_nodo,*bloquesTotales,*bloquesTotales));
				printf ("Se conectó un nuevo nodo: %s con %d bloques totales\n",inet_ntoa(remote_client.sin_addr),*bloquesTotales);
				log_info(logger,"Se conectó un nuevo nodo: %s con %d bloques totales",inet_ntoa(remote_client.sin_addr),*bloquesTotales);
			}
		}else {
			close(newfd);
			printf ("Se conecto algo pero no se que fue, lo rechazo\n");
		}
	}
	printf ("Nodos conectados %d\n",list_size(nodos));
	printf ("Informacion del nodo 2\n");
	printf ("Socket %d, ip %s, Estado %d, Nodos Libres %d\n",((t_nodo*)list_get(nodos,1))->socket,((t_nodo*)list_get(nodos,1))->ip,((t_nodo*)list_get(nodos,1))->estado,((t_nodo*)list_get(nodos,1))->bloques_libres);
	//sleep(5);
	//Cuando sale de este ciclo el proceso FileSystem ya se encuentra en condiciones de iniciar sus tareas

	//Este hilo va a manejar las conexiones con los nodos de forma paralela a la ejecucion del proceso
	if( pthread_create( &escucha , NULL ,  connection_handler_escucha , NULL) < 0){
	       perror("could not create thread");
	       log_error(logger,"Falló la creación del hilo que maneja las conexiones");
	       return 1;
	}

	archivos=list_create(); //Crea la lista de archivos
	directorios=list_create(); //crea la lista de directorios
	Menu();
	log_destroy(logger);

	//inicializo los indices para los directorios, 0 si está libre, 1 ocupado.
	for (j =1; j < sizeof(indiceDirectorios); j++){
		indiceDirectorios[j] = 0;
	}
	indiceDirectorios[0]=1; //raiz queda reservado como ocupado
	directoriosDisponibles= MAX_DIRECTORIOS-1;
	return 0;
}

//Consola Menu
void DibujarMenu(void){
	  printf("################################################################\n");
	  printf("# Ingrese una opción para continuar:                           #\n");
	  printf("#  1) Formatear el MDFS                                        #\n");
	  printf("#  2) Eliminar archivos                                        #\n");
	  printf("#  3) Renombrar archivos                                       #\n");
	  printf("#  4) Mover archivos                                           #\n");
	  printf("#  5) Crear directorios                                        #\n");
	  printf("#  6) Eliminar directorios                                     #\n");
	  printf("#  7) Renombrar directorios                                    #\n");
	  printf("#  8) Mover directorios                                        #\n");
	  printf("#  9) Copiar un archivo local al MDFS                          #\n");
	  printf("# 10) Copiar un archivo del MDFS al filesystem local           #\n");
	  printf("# 11) Solicitar el MD5 de un archivo en MDFS                   #\n");
	  printf("# 12) Ver los bloques que componen un archivo                  #\n");
	  printf("# 13) Borrar los bloques que componen un archivo               #\n");
	  printf("# 14) Copiar los bloques que componen un archivo               #\n");
	  printf("# 15) Agregar un nodo de datos                                 #\n");
	  printf("# 16) Eliminar un nodo de datos                                #\n");
	  printf("# 17) Salir                                                    #\n");
	  printf("################################################################\n");
}

int Menu(void){
	char opchar[20];
	int opcion=0;
	while (opcion !=17){
	    sleep(1);
	    DibujarMenu();
	    printf("Ingrese opción: ");
	    scanf ("%s", opchar);
	    opcion = atoi (opchar);
	    switch (opcion){
	      case 1: FormatearFilesystem (); break;
	      case 2: EliminarArchivo(); break;
	      case 3: RenombrarArchivo (); break;
	      case 4: MoverArchivo(); break;
	      case 5: CrearDirectorio(); break;
	      case 6:  EliminarDirectorio(); break;
	      case 7: RenombrarDirectorio(); break;
	      case 8:  MoverDirectorio(); break;
	      case 9:  CopiarArchivoAMDFS(); break;
	      case 10: CopiarArchivoDelMDFS(); break;
	      case 11: MD5DeArchivo(); break;
	      case 12: VerBloques(); break;
	      case 13: BorrarBloques(); break;
	      case 14: CopiarBloques(); break;
	      case 15: AgregarNodo(); break;
	      case 16: EliminarNodo(); break;
	      //case 17: printf("Eligió Salir\n"); break;
	      case 17: listar_nodos_conectados(nodos); break;
	      default: printf("Opción incorrecta. Por favor ingrese una opción del 1 al 17\n");break;
		}
	}
	return 0;
}
static t_nodo *agregar_nodo_a_lista(int socket,int est,char *ip, int port,int puerto_escucha, int bloques_lib, int bloques_tot){
	t_nodo *nodo_temporal = malloc (sizeof(t_nodo));


	//===========================================================================
	//Preparo el nombre que identificara al nodo, esto antes lo hacia una funcion
	//===========================================================================
	char nombre_temporal[10]="nodo";
	char *numero_nodo=malloc(sizeof(int));
	sprintf(numero_nodo,"%d",cantidad_nodos_historico);
	strcat(nombre_temporal,numero_nodo);

	//===========================================================================
	//===========================================================================
	//===========================================================================

	memset(nodo_temporal->nodo_id,'\0',10);
	nodo_temporal->socket = socket;
	strcat(nodo_temporal->nodo_id,nombre_temporal);
	nodo_temporal->estado = est;
	nodo_temporal->ip = strdup (ip);
	nodo_temporal->puerto = port;
	nodo_temporal->bloques_libres = bloques_lib;
	nodo_temporal->bloques_totales = bloques_tot;
	nodo_temporal->puerto_escucha_nodo=puerto_escucha;

	char *tmp_socket=malloc(sizeof(int));
	char *tmp_estado=malloc(sizeof(int));
	char *tmp_puerto=malloc(sizeof(int));
	char *tmp_bl_lib=malloc(sizeof(int));
	char *tmp_bl_tot=malloc(sizeof(int));
	sprintf(tmp_socket,"%d",socket);
	sprintf(tmp_estado,"%d",est);
	sprintf(tmp_puerto,"%d",port);
	sprintf(tmp_bl_lib,"%d",bloques_lib);
	sprintf(tmp_bl_tot,"%d",bloques_tot);
	printf ("%s",tmp_socket);
	//Persistencia del nodo agregado a la base de mongo
	doc = BCON_NEW ("Socket",tmp_socket,"Nodo_ID",nombre_temporal,"Estado",tmp_estado,"IP",ip,"Puerto",tmp_puerto,"Bloques_Libres",tmp_bl_lib,"Bloques_Totales",tmp_bl_tot);
	if (!mongoc_collection_insert (collection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
		printf ("%s\n", error.message);
	}

	return nodo_temporal;
}

char *obtener_md5(char *archivo){
	int fd[2];
	int childpid;
	char *resultado;
	pipe(fd);
	char result[1000];
	memset(result,'\0',1000);
	if ( (childpid = fork() ) == -1){
		fprintf(stderr, "Fallo el FORK");
	} else if( childpid == 0) {
		close(1);
		dup2(fd[1], 1);
		close(fd[0]);
		execlp("/usr/bin/md5sum","md5sum",archivo,NULL);
	}
	wait(NULL);
	read(fd[0], result, sizeof(result));
	printf("%s",result);
	resultado=malloc(sizeof(result));
	strcpy(resultado,result);
	return resultado;
}

void listar_nodos_conectados(t_list *nodos){
	int i,cantidad_nodos;
	t_nodo *elemento;
	cantidad_nodos= list_size(nodos);
	for (i=0;i<=cantidad_nodos;i++){
		elemento = list_get(nodos,i);
		printf ("\n\n");
		printf ("Nodo_ID: %s\nSocket: %d\nEstado: %d\nIP: %s\nPuerto_Origen: %d\nPuerto_Escucha_Nodo: %d\nBloques_Libres: %d\nBloques_Totales: %d", elemento->nodo_id, elemento->socket,elemento->estado,elemento->ip,elemento->puerto,elemento->puerto_escucha_nodo,elemento->bloques_libres,elemento->bloques_totales);
	}
}
void *connection_handler_escucha(void){
	int i,newfd,addrlen;
	while(1) {
		read_fds = master;
		if (select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1) {
			perror("select");
			log_error(logger,"FALLO el Select");
			exit(-1);
		}
		// explorar conexiones existentes en busca de datos que leer
		for(i = 0; i <= fdmax; i++) {
			if (FD_ISSET(i, &read_fds)) { // ¡¡tenemos datos!!
				if (i == listener) {
					// gestionar nuevas conexiones, primero hay que aceptarlas
					addrlen = sizeof(struct sockaddr_in);
					if ((newfd = accept(listener, (struct sockaddr*)&remote_client,(socklen_t*)&addrlen)) == -1) {
						perror("accept");
						log_error(logger,"FALLO el ACCEPT");
						exit(-1);
					} else {//llego una nueva conexion, se acepto y ahora tengo que tratarla
						if ((read_size = recv(newfd, identificacion, sizeof(identificacion), 0)) <= 0) { //si entra aca es porque hubo un error, no considero desconexion porque es nuevo
								perror("recv");
								log_error(logger,"FALLO el Recv");
								exit(-1);
						} else {
							// el nuevo conectado me manda algo, se identifica como nodo nuevo o nodo reconectado
							// luego de que se identifique lo agregare a la lista de nodos si es nodo nuevo
							// si es nodo reconectado hay que cambiarle el estado
							if (read_size > 0 && strncmp(identificacion,"marta",5)==0){
								// Se conecto el proceso Marta, le asigno un descriptor especial y lo agrego al select
								if (marta_presente==0){
									marta_presente=1;
									marta_sock=newfd;
									FD_SET(newfd, &master); // añadir al conjunto maestro
									if (newfd > fdmax) { // actualizar el máximo
										fdmax = newfd;
									}
									strcpy(identificacion,"ok");
									if((send(marta_sock,identificacion,sizeof(identificacion),0))==-1) {
										perror("send");
										log_error(logger,"FALLO el envio del ok a Marta");
										exit(-1);
									}
									printf ("Se conectó el proceso Marta desde la ip %s\n",inet_ntoa(remote_client.sin_addr));
									log_info(logger,"Se conectó el proceso Marta desde la ip %s",inet_ntoa(remote_client.sin_addr));
								}else{
									printf ("Ya existe un proceso marta conectado, no puede haber más de 1\n");
									log_warning(logger,"Ya existe un proceso marta conectado, no puede haber más de 1");
									close (newfd);
								}

							}
							if (read_size > 0 && strncmp(identificacion,"nuevo",5)==0){
								cantidad_nodos++;
								cantidad_nodos_historico=cantidad_nodos;
								FD_SET(newfd, &master); // añadir al conjunto maestro
								if (newfd > fdmax) { // actualizar el máximo
									fdmax = newfd;
								}
								bloquesTotales=malloc(sizeof(int));
								if ((read_size = recv(newfd, bloquesTotales , sizeof(int) , 0))==-1) {
									perror ("recv");
									log_error(logger,"FALLO el RECV");
									exit (-1);
								}
								puerto_escucha_nodo=malloc(sizeof(int));
								if ((read_size = recv(newfd, puerto_escucha_nodo , sizeof(int) , 0))==-1) {
									perror ("recv");
									log_error(logger,"FALLO el RECV");
									exit (-1);
								}

								if (read_size > 0){
									list_add (nodos, agregar_nodo_a_lista(newfd,0,inet_ntoa(remote_client.sin_addr),remote_client.sin_port,*puerto_escucha_nodo,*bloquesTotales,*bloquesTotales));
									printf ("Se conectó un nuevo nodo: %s con %d bloques totales\n",inet_ntoa(remote_client.sin_addr),*bloquesTotales);
									log_info(logger,"Se conectó un nuevo nodo: %s con %d bloques totales",inet_ntoa(remote_client.sin_addr),*bloquesTotales);
								}
							}
							if (read_size > 0 && strncmp(identificacion,"reconectado",11)==0){
								cantidad_nodos++;
								FD_SET(newfd, &master); // añadir al conjunto maestro
								if (newfd > fdmax) { // actualizar el máximo
									fdmax = newfd;
								}
								modificar_estado_nodo (i,inet_ntoa(remote_client.sin_addr),remote_client.sin_port,1); //cambio su estado de la lista a 1 que es activo
								printf ("Se reconectó el nodo %s\n",inet_ntoa(remote_client.sin_addr));
								log_info(logger,"Se reconectó el nodo %s",inet_ntoa(remote_client.sin_addr));
							}
						}
					}

				//.................................................
				//hasta aca, es el tratamiento de conexiones nuevas
				//.................................................

				} else { //si entra aca no es un cliente nuevo, es uno que ya tenia y me esta mandando algo
					// gestionar datos de un cliente
					if ((read_size = recv(i, mensaje, sizeof(mensaje), 0)) <= 0) { //si entra aca es porque se desconecto o hubo un error
						if (read_size == 0) {
							// Un nodo o marta cerro su conexion, actualizo la lista de nodos, reviso quien fue
							if (i==marta_sock){
								marta_presente=0;
								close(i); // ¡Hasta luego!
								FD_CLR(i, &master); // eliminar del conjunto maestro
								printf ("Marta se desconecto\n");
							} else {
								addrlen = sizeof(struct sockaddr_in);
								if ((getpeername(i,(struct sockaddr*)&remote_client,(socklen_t*)&addrlen))==-1){
									perror ("getpeername");
									log_error(logger,"Fallo el getpeername");
									exit(-1);
								}
								modificar_estado_nodo (i,inet_ntoa(remote_client.sin_addr),remote_client.sin_port,0);
								printf ("Se desconecto el nodo %s, %d\n",inet_ntoa(remote_client.sin_addr),remote_client.sin_port);
								close(i); // ¡Hasta luego!
								FD_CLR(i, &master); // eliminar del conjunto maestro
							}
						} else {
							perror("recv");
							log_error(logger,"FALLO el Recv");
							exit(-1);
						}
					} else {
						// tenemos datos de algún cliente
						// ...... Tratamiento del mensaje nuevo
					}
				}
			}
		}
	}
}

//Buscar el id del padre
uint32_t BuscarPadre (char* path){
	t_dir* dir;
	int directorioPadre=0; //seteo a raíz
	int tamanio=list_size(directorios);
	int contadorDirectorio=0;
	int i;
    char** directorio = string_split((char*) path, "/"); //Devuelve un array del path
    //Obtener id del padre del archivo(ante-ultima posición antes de NULL)
   	while(directorio[contadorDirectorio+1]!=NULL){
    	for(i=0;i<tamanio;i++){ //recorro lista de directorios
    		dir=list_get(directorios,i); //agarro primer directorio de la lista de directorios
    		//comparo si el nombre es igual al string del array del path y el padre es igual al padre anterior
        	if(((strcmp(dir->nombre,directorio[contadorDirectorio]))==0)&&(dir->padre==directorioPadre)){
        		directorioPadre=dir->id;
        		contadorDirectorio++;
        		break;
        	}
        	else{
        		if(i==tamanio-1){
        		printf("No se encontró el directorio");
        		directorioPadre=-1;
        		exit(-1);
        		}
        	}
  		}
   	}
    return directorioPadre;
}


//Buscar la posición del nodo de un archivo de la lista t_archivo por el nombre del archivo y el id del padre
uint32_t BuscarArchivoPorNombre (const char *path, uint32_t idPadre){
    unArchivo=malloc(sizeof(t_archivo));
    int i,posicionArchivo;
    char* nombreArchivo;
    int posArchivo = 0;
    int tam = list_size(archivos);
    char** directorio = string_split((char*) path, "/"); //Devuelve un array del path
    //Obtener solo el nombre del archivo(ultima posición antes de NULL)
    for(i=0; directorio[i]!=NULL;i++){
    	if(directorio[i+1]==NULL){
    		nombreArchivo=directorio[i];
    	}
    }
    for(posArchivo=0; posArchivo<tam;posArchivo++){
        unArchivo = list_get(archivos,posArchivo);
        if ((strcmp(unArchivo->nombre,nombreArchivo)==0)&&(unArchivo->padre == idPadre)){
            posicionArchivo = posArchivo;
            break;
        }else{
        	if(i==tam-1){
        	printf("No se encontró el archivo");
        	posicionArchivo=-1;
        	exit(-1);
        	}
        }
    }
    return posicionArchivo;
}



int BuscarMenorIndiceLibre (char indiceDirectorios[]){
	int i;
	while (i < sizeof(indiceDirectorios) && indiceDirectorios[i] == 1)
		i++;
	if (i < sizeof(indiceDirectorios))
			return i; //devuelvo el menor indice libre
	else
		return -1;
}

void modificar_estado_nodo (int socket,char *ip,int port,int estado){
	int i;
	t_nodo *tmp;
	for (i=0;i<list_size(nodos);i++){
		tmp = list_get(nodos,i);
		if (socket==-1){
			if ((strcmp(tmp->ip,ip)==0) && tmp->puerto==port){
				tmp->estado=estado;
				break;
			}
		}else{
			if ((strcmp(tmp->ip,ip)==0) && tmp->puerto==port){
				tmp->estado=estado;
				tmp->socket=socket;
				break;
			}
		}
	}
}
void formatear_nodos(){
	int i;
	for (i=0;i<list_size(nodos);i++)
		list_remove(nodos,i);
}


void FormatearFilesystem (){
	printf("Eligió  Formatear el MDFS\n");
	if (archivos != NULL){
		if (unArchivo->bloques != NULL){
			list_clean(unArchivo->bloques);
		}
		list_clean(archivos);
	}
}

static void archivo_destroy(t_archivo* self) {
    free(self->nombre);
    free(self);
}

static void eliminar_bloques(t_bloque *bloque){
	free(bloque->copias[0].nodo);
	free(bloque->copias[1].nodo);
	free(bloque->copias[2].nodo);
}


void EliminarArchivo(){
    printf("Eligió  Eliminar archivo\n");
    char* path = malloc(1);
    printf ("Ingrese el path del archivo \n");
    scanf ("%s", path);
    uint32_t idPadre = BuscarPadre(path);
    uint32_t posArchivo = BuscarArchivoPorNombre (path,idPadre);
    unArchivo = list_get(archivos,posArchivo);
    //Eliminar bloques del archivo
    while(unArchivo->bloques!=NULL){
    	list_destroy_and_destroy_elements(unArchivo->bloques, (void*)eliminar_bloques);
    	break;

    }
    //Elimnar nodo del archivo t_arhivo
    //list_remove_and_destroy_element(t_list *, int index, void(*element_destroyer)(void*));
    list_remove_and_destroy_element(archivos, posArchivo, (void*) archivo_destroy);
}



void RenombrarArchivo (){
	printf("Eligió Renombrar archivos\n");
    char* path = malloc(1);
    char* nuevoNombre = malloc(1);
    printf ("Ingrese el path del archivo \n");
    scanf ("%s", path);
    uint32_t idPadre = BuscarPadre(path);
    uint32_t posArchivo = BuscarArchivoPorNombre (path,idPadre);
    unArchivo = list_get(archivos,posArchivo);
    printf ("Ingrese el nuevo nombre \n");
    scanf ("%s", nuevoNombre);
    strcpy(unArchivo->nombre, nuevoNombre);

}

void MoverArchivo(){
	 printf("Eligió Mover archivos\n");
	 char* path = malloc(1);
	 char* nuevoPath = malloc(1);
	 printf ("Ingrese el path del archivo \n");
	 scanf ("%s", path);
	 uint32_t idPadre = BuscarPadre(path);
	 uint32_t posArchivo = BuscarArchivoPorNombre (path,idPadre);
	 unArchivo = list_get(archivos,posArchivo);
	 printf ("Ingrese el nuevo path \n");
	 scanf ("%s", nuevoPath);
	 uint32_t idPadreNuevo = BuscarPadre(nuevoPath);
	 unArchivo->padre = idPadreNuevo;
}

long ExisteEnLaLista(t_list* listaDirectorios, char* nombreDirectorioABuscar, uint32_t idPadre){
	t_dir* elementoDeMiLista;
	elementoDeMiLista = malloc(sizeof(t_dir));
	long encontrado = -1; //trae -1 si no lo encuentra, sino trae el id del elemento
	int tamanioLista = list_size(listaDirectorios);
	int i = 0;
	while(encontrado == -1 && i < tamanioLista){
		elementoDeMiLista = list_get(listaDirectorios, i);
		if (strcmp(elementoDeMiLista->nombre, nombreDirectorioABuscar)){
			if (elementoDeMiLista->padre == idPadre){
				encontrado= elementoDeMiLista->id;
			}
		}
		i++;
	}
	return encontrado;
}

void CrearDirectorio(){
	//printf("Eligió Crear directorios\n");
	uint32_t idPadre;
	char* path;
	char** directorioNuevo;
	t_dir* directorioACrear;
	int cantDirACrear=0;
	directorioACrear = malloc(sizeof(t_dir));
	long idAValidar; //uso este tipo para cubrir rango de uint32_t y el -1,  deberia mejorar el nombre de la variable
	printf ("Ingrese el path del directorio desde raíz ejemplo /home/utnso \n");
	scanf ("%s", path);
    directorioNuevo = string_split((char*) path, "/"); //Devuelve un array del path del directorio a crear
    int indiceVectorDirNuevo=1;
    while( directorioNuevo[indiceVectorDirNuevo]!=NULL){ //empiezo desde 1 porque nivel prof 0 es raiz y no existe en la lista
    	//list_find(directorios,(void*) ExisteEnLaLista());  //ver más adelante de usar la función de lcommons
    	if (indiceVectorDirNuevo == 1){
    		idPadre = 0;
    	}
    	idAValidar = ExisteEnLaLista(directorios, directorioNuevo[indiceVectorDirNuevo], idPadre);
    	//quiere decir que existe
    	if (idAValidar != -1){
    		idPadre = (uint32_t) idAValidar;
    		indiceVectorDirNuevo++;
    	}
    	else {
    		int indiceDirectoriosNuevos;
    		for(indiceDirectoriosNuevos=indiceVectorDirNuevo; directorioNuevo[indiceDirectoriosNuevos] != NULL; indiceDirectoriosNuevos++){
    			cantDirACrear++;
    		}
    		if(cantDirACrear <= directoriosDisponibles ){
        		while( directorioNuevo[indiceVectorDirNuevo]!=NULL){
        	          directorioACrear->nombre = directorioNuevo[indiceVectorDirNuevo];
        	          directorioACrear->padre = idPadre;
        	          //persistir en la db: pendiente
        	          int id = BuscarMenorIndiceLibre(indiceDirectorios);
        	          directorioACrear->id = id;
        	          indiceDirectorios[id]=1;
        	          directoriosDisponibles--;
        	          idPadre = directorioACrear->id;
        	          list_add(directorios, directorioACrear);
        	          indiceVectorDirNuevo++;
        		}
    		}
    		else{
    			printf("No se puede crear el directorio ya que sobrepasaría el límite máximo de directorios permitidos: %d\n", MAX_DIRECTORIOS);
    					//No puede pasarse de 1024 directorios
    		}
    	}
    }
}


static void directorio_destroy(t_dir* self) {
    free(self->nombre);
    free(self);
}



void EliminarDirectorio(){
	//printf("Eligió Eliminar directorios\n");
	char* pathAEliminar;
	char** vectorpathAEliminar;
	t_dir* elementoDeMiListaDir;
	elementoDeMiListaDir = malloc(sizeof(t_dir));
	t_archivo* elementoDeMiListaArch;
	elementoDeMiListaArch = malloc(sizeof(t_archivo));
	int tamanioListaDir = list_size(directorios);
	int tamanioListaArch = list_size(archivos);
	int i = 0;
	uint32_t idAEliminar;
	long idEncontrado = 0;
	char encontrePos; //0 si no lo encuentra, 1 si lo encuentra
	char tieneDirOArch; //0 si no tiene, 1 si tiene subdirectorio o archivo
	int posicionElementoAEliminar;
	printf ("Ingrese el path a eliminar desde raíz ejemplo /home/utnso \n");
	scanf ("%s", pathAEliminar);
	vectorpathAEliminar = string_split((char*) pathAEliminar, "/");
	while (vectorpathAEliminar[i] != NULL && idEncontrado != -1){
			if (i == 0){
				idEncontrado = 0; //el primero que cuelga de raiz
			}
			idEncontrado = ExisteEnLaLista(directorios,vectorpathAEliminar[i], idEncontrado);
			i++;
		}
		if (idEncontrado == -1){
			printf ("No existe el directorio para eliminar \n");
		}
		else{
			tieneDirOArch = 0;
			idAEliminar = idEncontrado;
			i = 0;
			while(tieneDirOArch == 0 && i < tamanioListaDir){
				elementoDeMiListaDir = list_get(directorios, i);
				if (elementoDeMiListaDir->padre == idAEliminar){
					tieneDirOArch = 1;
				}
				i++;
			}
			if (tieneDirOArch == 1){
				printf ("El directorio que desea eliminar no puede ser eliminado ya que posee subdirectorios \n");
			}
			else{
				i = 0;
				while(tieneDirOArch == 0 && i < tamanioListaArch){
					elementoDeMiListaArch = list_get(archivos, i);
					if (elementoDeMiListaArch->padre == idAEliminar){
						tieneDirOArch = 1;
					}
					i++;
				}
				if (tieneDirOArch == 1){
					printf ("El directorio que desea eliminar no puede ser eliminado ya que posee archivos \n");
				}
				else{
					i = 0;
					encontrePos = 0; //no lo encontre
					while (encontrePos == 0 && i < tamanioListaDir){
						elementoDeMiListaDir = list_get(directorios, i);
						if (elementoDeMiListaDir->id ==idAEliminar){
							encontrePos = 1;
						}
						i++;
					}
					posicionElementoAEliminar = i -1;
					//list_remove_and_destroy_element(t_list *, int index, void(*element_destroyer)(void*));
					list_remove_and_destroy_element(directorios, posicionElementoAEliminar, (void*) directorio_destroy);
				}
			}
		}


}

void RenombrarDirectorio(){
	//printf("Eligió Renombrar directorios\n");
	char* pathOriginal;
	char** vectorPathOriginal;
	char* pathNuevo;
	t_dir* elementoDeMiLista;
	elementoDeMiLista = malloc(sizeof(t_dir));
	int tamanioLista = list_size(directorios);
	int i = 0;
	uint32_t idARenombrar;
	long idEncontrado = 0;
	char encontrado; //0 si no lo encontro, 1 si lo encontro
    printf ("Ingrese el path del original desde raíz ejemplo /home/utnso \n");
	scanf ("%s", pathOriginal);
	printf ("Ingrese el nuevo nombre de directorio \n");
	scanf ("%s", pathNuevo);
	vectorPathOriginal = string_split((char*) pathOriginal, "/");
	while (vectorPathOriginal[i] != NULL && idEncontrado != -1){
		if (i == 0){
			idEncontrado = 0; //el primero que cuelga de raiz
		}
		idEncontrado = ExisteEnLaLista(directorios,vectorPathOriginal[i], idEncontrado);
		i++;
	}
	if (idEncontrado == -1){
		printf ("No existe el directorio para renombrar \n");
	}
	else{
		i=0;
		encontrado = 0;
		idARenombrar = idEncontrado;
		while(encontrado == 0 && i < tamanioLista){
			elementoDeMiLista = list_get(directorios, i);
			if (elementoDeMiLista->id == idARenombrar){
				encontrado= 1;
				strcpy(elementoDeMiLista->nombre, pathNuevo);
			}
			i++;
		}
	}
}

void MoverDirectorio(){
	//printf("Eligió Mover directorios\n");
	char* pathOriginal;
	char** vectorPathOriginal;
	char* pathNuevo;
	char** vectorPathNuevo;
	char* nombreDirAMover;
	t_dir* elementoDeMiLista;
	elementoDeMiLista = malloc(sizeof(t_dir));
	int tamanioLista = list_size(directorios);
	int i = 0;
	uint32_t idDirAMover;
	uint32_t idNuevoPadre;
	long idEncontrado = 0;
	char encontrado; //0 si no lo encontro, 1 si lo encontro
	printf ("Ingrese el path original desde raíz ejemplo /home/utnso \n");
	scanf ("%s", pathOriginal);
	printf ("Ingrese el path del directorio al que desea moverlo desde raíz ejemplo /home/tp \n");
	scanf ("%s", pathNuevo);
	vectorPathOriginal = string_split((char*) pathOriginal, "/");
	vectorPathNuevo = string_split((char*) pathNuevo, "/");
	while (vectorPathOriginal[i] != NULL && idEncontrado != -1){
		if (i == 0){
			idEncontrado = 0; //el primero que cuelga de raiz
		}
		idEncontrado = ExisteEnLaLista(directorios,vectorPathOriginal[i], idEncontrado);
		i++;
	}
	if (idEncontrado == -1){
		printf ("No existe el path original \n");
	}
	else{
		idDirAMover = idEncontrado;
		strcpy(nombreDirAMover,vectorPathOriginal[i-1]); //revisar, puse -1 porque avancé hasta el NULL.
		idEncontrado = 0;
		i = 0;
		while (vectorPathNuevo[i] != NULL && idEncontrado != -1){
			if (i == 0){
				idEncontrado = 0; //el primero que cuelga de raiz
			}
			idEncontrado = ExisteEnLaLista(directorios,vectorPathNuevo[i], idEncontrado);
			i++;
		}
		if (idEncontrado == -1){
				printf ("No existe el path al que desea moverlo \n");
			}
			else{
				idNuevoPadre = idEncontrado;
				if (ExisteEnLaLista(directorios,nombreDirAMover, idNuevoPadre) == -1){ //ver si el padre no tiene hijos que se llamen igual que el directorio a mover
					i = 0;
					encontrado = 0;
					while(encontrado == 0 && i < tamanioLista){
						elementoDeMiLista = list_get(directorios, i);
						if (elementoDeMiLista->id == idDirAMover){
							encontrado= 1;
							elementoDeMiLista->padre = idNuevoPadre;
						}
						i++;
					}
				}
				else {
					printf ("El directorio no está vacío \n");
				}
			}
	}
}

void CopiarArchivoAMDFS(){
	printf("Eligió Copiar un archivo local al MDFS\n");
}

void CopiarArchivoDelMDFS(){
	printf("Eligió Copiar un archivo del MDFS al filesystem local\n");
}

void MD5DeArchivo(){
	printf("Eligió Solicitar el MD5 de un archivo en MDFS\n");
//    char* path = malloc(1);
//    printf ("Ingrese el path del archivo \n");
//	scanf ("%s", path);
//	uint32_t idPadre = BuscarPadre(path);
//	uint32_t posArchivo = BuscarArchivoPorNombre (path,idPadre);
//	unArchivo = list_get(archivos,posArchivo);
}

void VerBloques(){
	printf("Eligió Ver los bloques que componen un archivo\n");
}

void BorrarBloques(){
	printf("Eligió Borrar los bloques que componen un archivo\n");
}

void CopiarBloques(){
	printf("Eligió Copiar los bloques que componen un archivo\n");
}

void AgregarNodo(){
	printf("Eligió Agregar un nodo de datos\n");
}

void EliminarNodo(){
	printf("Eligió Eliminar un nodo de datos\n");
}
