#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <commons/collections/list.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/bitarray.h>


//Includes para mongo
#include <bson.h>
#include <mongoc.h>
#include "FS_MDFS.h"

//Variables globales
t_list *nodos_temporales;
t_list *archivos_temporales;
t_datos_y_bloque combo;
//uint32_t n_bloque;
fd_set master; // conjunto maestro de descriptores de fichero
fd_set read_fds; // conjunto temporal de descriptores de fichero para select()
t_log* logger;
t_list *nodos; //lista de nodos conectados al fs
t_list* archivos; //lista de archivos del FS
t_list* directorios; //lista de directorios del FS
t_bloque* bloque; //Un Bloque del Archivo
t_config * configurador;
t_archivo* unArchivo; //Un archivo de la lista de archivos del FS
t_bloque *unBloque;
t_copias *unaCopia;
int fdmax; // número máximo de descriptores de fichero
int listener; // descriptor de socket a la escucha
struct sockaddr_in filesystem; // dirección del servidor
struct sockaddr_in remote_client; // dirección del cliente
char identificacion[BUF_SIZE]; // buffer para datos del cliente
char mensaje[MENSAJE_SIZE];
int cantidad_nodos = 0;
int cantidad_nodos_historico = 0;
int read_size;
int *bloquesTotales; //tendra la cantidad de bloques totales del file de datos
int marta_presente = 0; //Valiable para controlar que solo 1 proceso marta se conecte
int marta_sock;
char indiceDirectorios[MAX_DIRECTORIOS]; //cantidad maxima de directorios
int directoriosDisponibles; //reservo raiz
int j; //variable para recorrer el vector de indices
int *puerto_escucha_nodo;
char nodo_id[6];
//t_nodo nodosMasLibres[3];
//char bufBloque[BLOCK_SIZE];
//Variables para la persistencia con mongo
mongoc_client_t *client;
mongoc_collection_t *collection;
mongoc_cursor_t *cursor;
bson_error_t error;
bson_oid_t oid;
bson_t *doc;

int main(int argc, char *argv[]) {

	pthread_t escucha; //Hilo que va a manejar los mensajes recibidos
	int newfd;
	int addrlen;
	int yes = 1; // para setsockopt() SO_REUSEADDR, más abajo
	configurador = config_create("resources/fsConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
	logger = log_create("fsLog.log", "FileSystem", false, LOG_LEVEL_INFO);
	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);

	mongoc_init();
	client = mongoc_client_new("mongodb://localhost:27017/");
	collection = mongoc_client_get_collection(client, "NODOS", "lista_nodos");
	bson_oid_init(&oid, NULL);

	if ((listener = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("socket");
		log_error(logger, "FALLO la creacion del socket");
		exit(-1);
	}
	// obviar el mensaje "address already in use" (la dirección ya se está usando)
	if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
		perror("setsockopt");
		log_error(logger, "FALLO la ejecucion del setsockopt");
		exit(-1);
	}
	// enlazar
	filesystem.sin_family = AF_INET;
	filesystem.sin_addr.s_addr = INADDR_ANY;
	filesystem.sin_port = htons(config_get_int_value(configurador, "PUERTO_LISTEN"));
	memset(&(filesystem.sin_zero), '\0', 8);
	if (bind(listener, (struct sockaddr *) &filesystem, sizeof(filesystem))	== -1) {
		perror("bind");
		log_error(logger, "FALLO el Bind");
		exit(-1);
	}
	// escuchar
	if (listen(listener, 10) == -1) {
		perror("listen");
		log_error(logger, "FALLO el Listen");
		exit(1);
	}
	// añadir listener al conjunto maestro
	FD_SET(listener, &master);

	// seguir la pista del descriptor de fichero mayor
	fdmax = listener; // por ahora es éste el ultimo socket
	addrlen = sizeof(struct sockaddr_in);
	nodos = list_create(); //Crea la lista de que va a manejar la lista de nodos
	printf("Esperando las conexiones de los nodos iniciales\n");
	log_info(logger, "Esperando las conexiones de los nodos iniciales");
	while (cantidad_nodos!= config_get_int_value(configurador, "CANTIDAD_NODOS")) {
		if ((newfd = accept(listener, (struct sockaddr*) &remote_client,(socklen_t*) &addrlen)) == -1) {
			perror("accept");
			log_error(logger, "FALLO el ACCEPT");
			exit(-1);
		}
		if ((read_size = recv(newfd, identificacion, sizeof(identificacion), MSG_WAITALL)) == -1) {
			perror("recv");
			log_error(logger, "FALLO el RECV");
			exit(-1);
		}
		if (read_size > 0 && strncmp(identificacion, "nuevo", 5) == 0) {
			bloquesTotales = malloc(sizeof(int));
			//Segundo recv, aca espero recibir la capacidad del nodo
			if ((read_size = recv(newfd, bloquesTotales, sizeof(int), MSG_WAITALL))== -1) {
				perror("recv");
				log_error(logger, "FALLO el RECV");
				exit(-1);
			}
			puerto_escucha_nodo = malloc(sizeof(int));
			if ((read_size = recv(newfd, puerto_escucha_nodo, sizeof(int), MSG_WAITALL))== -1) {
				perror("recv");
				log_error(logger, "FALLO el RECV");
				exit(-1);
			}
			if ((read_size = recv(newfd, nodo_id, sizeof(nodo_id), MSG_WAITALL)) == -1) {
				perror("recv");
				log_error(logger, "FALLO el RECV");
				exit(-1);
			}
			if (read_size > 0) {
				if (validar_nodo_nuevo(nodo_id) == 0) {
					cantidad_nodos++;
					cantidad_nodos_historico = cantidad_nodos;
					FD_SET(newfd, &master); // añadir al conjunto maestro
					if (newfd > fdmax) { // actualizar el máximo
						fdmax = newfd;
					}
					list_add(nodos,agregar_nodo_a_lista(nodo_id, newfd, 0, 1,inet_ntoa(remote_client.sin_addr),remote_client.sin_port,*puerto_escucha_nodo, *bloquesTotales,*bloquesTotales));
					printf("Se conectó un nuevo nodo: %s con %d bloques totales\n",inet_ntoa(remote_client.sin_addr), *bloquesTotales);
					log_info(logger,"Se conectó un nuevo nodo: %s con %d bloques totales",inet_ntoa(remote_client.sin_addr), *bloquesTotales);
				} else {
					printf("Ya existe un nodo con el mismo id o direccion ip\n");
					close(newfd);
				}
			}
		} else {
			close(newfd);
			printf("Se conecto algo pero no se que fue, lo rechazo\n");
		}
	}
	//Cuando sale de este ciclo el proceso FileSystem ya se encuentra en condiciones de iniciar sus tareas

	//Este hilo va a manejar las conexiones con los nodos de forma paralela a la ejecucion del proceso



	if (pthread_create(&escucha, NULL, connection_handler_escucha, NULL) < 0) {
		perror("could not create thread");
		log_error(logger,"Falló la creación del hilo que maneja las conexiones");
		return 1;
	}


	archivos = list_create(); //Crea la lista de archivos
	directorios = list_create(); //crea la lista de directorios
	//inicializo los indices para los directorios, 0 si está libre, 1 ocupado.
	for (j = 1; j < sizeof(indiceDirectorios); j++) {
		indiceDirectorios[j] = 0;
	}
	indiceDirectorios[0] = 1; //raiz queda reservado como ocupado
	directoriosDisponibles = (MAX_DIRECTORIOS - 1);

	Menu();
	log_destroy(logger);

	return 0;
}

//Consola Menu
void DibujarMenu(void) {
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
	printf("# 12) Ver un bloque                                            #\n");
	printf("# 13) Borrar un bloque                                         #\n");
	printf("# 14) Copiar un bloque                                         #\n");
	printf("# 15) Agregar un nodo de datos                                 #\n");
	printf("# 16) Eliminar un nodo de datos                                #\n");
	printf("# 17) Salir                                                    #\n");
	printf("################################################################\n");
}

int Menu(void) {
	char opchar[2];
	memset(opchar, '\0', 2);
	int opcion = 0;
	while (opcion != 17) {
		sleep(1);
		opcion = 0;
		memset(opchar, '\0', 2);
		DibujarMenu();
		printf("Ingrese opción: ");
		scanf("%s", opchar);
		opcion = atoi(opchar);
		switch (opcion) {
		case 1:
			FormatearFilesystem();	break;
		case 2:
			EliminarArchivo(); break;
		case 3:
			RenombrarArchivo();	break;
		case 4:
			MoverArchivo();	break;
		case 5:
			CrearDirectorio();	break;
		case 6:
			EliminarDirectorio();	break;
		case 7:
			RenombrarDirectorio();	break;
		case 8:
			MoverDirectorio();	break;
		case 9:
			CopiarArchivoAMDFS();	break;
		case 10:
			CopiarArchivoDelMDFS(1,NULL);	break;
		case 11:
			MD5DeArchivo();	break;
		case 12:
			VerBloque(1,NULL,0); break;
		case 13:
			BorrarBloque(); break;
		case 14:
			CopiarBloque(); break;
		case 15:
			AgregarNodo(); break;
		case 16:
			EliminarNodo();	break;
			//case 17: printf("Eligió Salir\n"); break;

		case 17: listar_nodos_conectados(nodos); break;
		//case 17: listar_archivos_subidos(archivos); break;
		//case 17: listar_directorios(); break;
		//case 17: eliminar_listas(archivos,directorios,nodos); break;
		default: printf("Opción incorrecta. Por favor ingrese una opción del 1 al 17\n"); break;
		}
	}
	return 0;
}

void listar_directorios(){
	t_dir *dir=malloc(sizeof(t_dir));
	int i;
	if (list_size(directorios)==0){
		printf ("No hay directorios cargados\n");
		Menu();
	}
	for (i=0;i<list_size(directorios);i++){
		dir=list_get(directorios,i);
		printf ("ID: %d Nombre: %s Padre: %d\n",dir->id,dir->nombre,dir->padre);
	}
}

static t_nodo *agregar_nodo_a_lista(char nodo_id[6], int socket, int est, int est_red, char *ip, int port, int puerto_escucha, int bloques_lib,int bloques_tot) {
	t_nodo *nodo_temporal = malloc(sizeof(t_nodo));
	int i;
	memset(nodo_temporal->nodo_id, '\0', 6);
	strcpy(nodo_temporal->nodo_id, nodo_id);
	nodo_temporal->socket = socket;
	//nodo_temporal->estado = est; por ahora lo comento para forzar el estado a 1
	nodo_temporal->estado = 1;
	nodo_temporal->estado_red = est_red;
	nodo_temporal->ip = strdup(ip);
	nodo_temporal->puerto = port;
	nodo_temporal->bloques_libres = bloques_lib;
	nodo_temporal->bloques_totales = bloques_tot;
	nodo_temporal->puerto_escucha_nodo = puerto_escucha;

	//Creo e inicializo el bitarray del nodo, 0 es bloque libre, 1 es blloque ocupado
	//Como recien se esta conectadno el nodo, todos sus bloques son libres
	for (i = 8; i < bloques_tot; i += 8);
	nodo_temporal->bloques_bitarray = malloc(i / 8);
	nodo_temporal->bloques_del_nodo = bitarray_create(nodo_temporal->bloques_bitarray, i / 8);
	for (i = 0; i < nodo_temporal->bloques_totales; i++)
		bitarray_clean_bit(nodo_temporal->bloques_del_nodo, i);

	//mongo
	char *tmp_socket = string_new();
	char *tmp_estado = string_new();
	char *tmp_puerto = string_new();
	char *tmp_bl_lib = string_new();
	char *tmp_bl_tot = string_new();
	sprintf(tmp_socket, "%d", socket);
	sprintf(tmp_estado, "%d", est);
	sprintf(tmp_puerto, "%d", port);
	sprintf(tmp_bl_lib, "%d", bloques_lib);
	sprintf(tmp_bl_tot, "%d", bloques_tot);
	//Persistencia del nodo agregado a la base de mongo
	doc = BCON_NEW("Socket", tmp_socket, "Nodo_ID", nodo_id, "Estado",tmp_estado, "IP", ip, "Puerto", tmp_puerto, "Bloques_Libres",tmp_bl_lib, "Bloques_Totales", tmp_bl_tot);
	if (!mongoc_collection_insert(collection, MONGOC_INSERT_NONE, doc, NULL,&error)) {
		printf("%s\n", error.message);
	}
	free(tmp_socket);
	free(tmp_estado);
	free(tmp_puerto);
	free(tmp_bl_lib);
	free(tmp_bl_tot);

	return nodo_temporal;
}

int validar_nodo_nuevo(char nodo_id[6]) {
	int i;
	t_nodo *tmp;
	for (i = 0; i < list_size(nodos); i++) {
		tmp = list_get(nodos, i);
		if (strcmp(tmp->nodo_id, nodo_id) == 0)
			return 1;
	}
	return 0;
}
int validar_nodo_reconectado(char nodo_id[6]) {
	int i;
	t_nodo *tmp;
	for (i = 0; i < list_size(nodos); i++) {
		tmp = list_get(nodos, i);
		if (strcmp(tmp->nodo_id, nodo_id) == 0)
			return 0;
	}
	return 1;
}
char *buscar_nodo_id(char *ip, int port) {
	int i;
	char *id_temporal = malloc(6);
	t_nodo *tmp;
	for (i = 0; i < list_size(nodos); i++) {
		tmp = list_get(nodos, i);
		if ((strcmp(tmp->ip, ip) == 0) && (tmp->puerto == port)) {
			strcpy(id_temporal, tmp->nodo_id);
			return id_temporal;
		}
	}
	return NULL;
}

char *obtener_md5(char *bloque) {
	int count,bak0,bak1;
		int fd[2];
		int md[2];
		int childpid;
		pipe(fd);
		pipe(md);
		char result[50];
		char *resultado_md5=malloc(32);
		memset(result,'\0',50);
		if ( (childpid = fork() ) == -1){
			fprintf(stderr, "FORK failed");
		} else if( childpid == 0) {
			bak0=dup(0);
			bak1=dup(1);
			dup2(fd[0],0);
			dup2(md[1],1);
			close(fd[1]);
			close(fd[0]);
			close(md[1]);
			close(md[0]);
			execlp("/usr/bin/md5sum","md5sum",NULL);
		}
		write(fd[1],bloque,strlen(bloque));
	    close(fd[1]);
		close(fd[0]);
		close(md[1]);
		count=read(md[0],result,36);
		close(md[0]);
		dup2(bak0,0);
		dup2(bak1,1);
		if (count>0){
			result[32]=0;
			strcpy(resultado_md5,result);
			return resultado_md5;
		}else{
			printf ("ERROR READ RESULT\n");
			exit(-1);
		}
}

void listar_nodos_conectados(t_list *nodos) {
	int i, j, cantidad_nodos;
	t_nodo *elemento=malloc(sizeof(t_nodo));
	cantidad_nodos = list_size(nodos);
	for (i = 0; i < cantidad_nodos; i++) {
		elemento = list_get(nodos, i);
		printf("\n\n");
		printf("Nodo_ID: %s\nSocket: %d\nEstado: %d\nEstado de Conexion: %d\nIP: %s\nPuerto_Origen: %d\nPuerto_Escucha_Nodo: %d\nBloques_Libres: %d\nBloques_Totales: %d",elemento->nodo_id, elemento->socket, elemento->estado,elemento->estado_red, elemento->ip, elemento->puerto,elemento->puerto_escucha_nodo, elemento->bloques_libres,elemento->bloques_totales);
		printf("\n");
		for (j = 0; j < elemento->bloques_totales; j++)
			printf("%d", bitarray_test_bit(elemento->bloques_del_nodo, j));
	}
	exit(0);
}
void listar_archivos_subidos(t_list *archivos) {
	int i,j,k,cantidad_archivos,cantidad_bloques,cantidad_copias;
	t_archivo *elemento;
	t_bloque *bloque;
	t_copias *copia;
	cantidad_archivos = list_size(archivos);
	if (cantidad_archivos==0){
		printf ("No hay archivos cargados en MDFS\n");
		exit(1);
	}
	for (i = 0; i < cantidad_archivos; i++) {
		elemento = list_get(archivos, i);
		printf("\n\n");
		printf("Archivo: %s\nPadre: %d\nTam: %d\nEstado: %d\n",elemento->nombre,elemento->padre,elemento->tamanio,elemento->estado);
		printf("\n");
		cantidad_bloques=list_size(elemento->bloques);
		for (j = 0; j < cantidad_bloques; j++){
			bloque=list_get(elemento->bloques,j);
			printf ("Numero de bloque: %d\n",j);
			cantidad_copias=list_size(bloque->copias);
			for (k=0;k<cantidad_copias;k++){
				copia=list_get(bloque->copias,k);
				printf ("Copia %d del bloque %d\n",k,j);
				printf ("----------------------\n");
				printf ("	Nodo: %s\n	Bloque: %d\n	MD5: %s\n\n",copia->nodo,copia->bloqueNodo,copia->md5);
			}
		}
	}
	exit(0);
}


void *connection_handler_escucha(void) {
	int i, newfd, addrlen;
	while (1) {
		read_fds = master;
		if (select(fdmax + 1, &read_fds, NULL, NULL, NULL) == -1) {
			perror("select");
			log_error(logger, "FALLO el Select");
			exit(-1);
		}
		// explorar conexiones existentes en busca de datos que leer
		for (i = 0; i <= fdmax; i++) {
			if (FD_ISSET(i, &read_fds)) { // ¡¡tenemos datos!!
				if (i == listener) {
					// gestionar nuevas conexiones, primero hay que aceptarlas
					addrlen = sizeof(struct sockaddr_in);
					if ((newfd = accept(listener,(struct sockaddr*) &remote_client,(socklen_t*) &addrlen)) == -1) {
						perror("accept");
						log_error(logger, "FALLO el ACCEPT");
						exit(-1);
					} else { //llego una nueva conexion, se acepto y ahora tengo que tratarla
						if ((read_size = recv(newfd, identificacion,sizeof(identificacion), MSG_WAITALL)) <= 0) { //si entra aca es porque hubo un error, no considero desconexion porque es nuevo
							perror("recv");
							log_error(logger, "FALLO el Recv");
							exit(-1);
						} else {
							// el nuevo conectado me manda algo, se identifica como nodo nuevo o nodo reconectado
							// luego de que se identifique lo agregare a la lista de nodos si es nodo nuevo
							// si es nodo reconectado hay que cambiarle el estado
							if (read_size > 0 && strncmp(identificacion, "marta", 5) == 0) {
								// Se conecto el proceso Marta, le asigno un descriptor especial y lo agrego al select
								if (marta_presente == 0) {
									marta_presente = 1;
									marta_sock = newfd;
									FD_SET(newfd, &master); // añadir al conjunto maestro
									if (newfd > fdmax) { // actualizar el máximo
										fdmax = newfd;
									}
									strcpy(identificacion, "ok");
									if ((send(marta_sock, identificacion,sizeof(identificacion), MSG_WAITALL)) == -1) {
										perror("send");
										log_error(logger, "FALLO el envio del ok a Marta");
										exit(-1);
									}
									printf("Se conectó el proceso Marta desde la ip %s\n",inet_ntoa(remote_client.sin_addr));
									log_info(logger,"Se conectó el proceso Marta desde la ip %s",inet_ntoa(remote_client.sin_addr));



									int cant_nodos;
									cant_nodos=list_size(nodos);
									if ((send(marta_sock, &cant_nodos,sizeof(int), MSG_WAITALL)) == -1) {
										perror("send");
										log_error(logger, "FALLO el envio del ok a Marta");
										exit(-1);
									}
									for (cant_nodos=0;cant_nodos<list_size(nodos);cant_nodos++){
										t_nodo *nodo_para_marta=malloc(sizeof(t_nodo));
										nodo_para_marta=list_get(nodos,cant_nodos);
										if ((send(marta_sock, nodo_para_marta->nodo_id,sizeof(nodo_para_marta->nodo_id), MSG_WAITALL)) == -1) {
											perror("send");
											log_error(logger, "FALLO el envio del ok a Marta");
											exit(-1);
										}
										if ((send(marta_sock, &nodo_para_marta->estado,sizeof(int), MSG_WAITALL)) == -1) {
											perror("send");
											log_error(logger, "FALLO el envio del ok a Marta");
											exit(-1);
										}
										printf ("%d\n",strlen(nodo_para_marta->ip));
										if ((send(marta_sock, nodo_para_marta->ip,strlen(nodo_para_marta->ip), MSG_WAITALL)) == -1) {
											perror("send");
											log_error(logger, "FALLO el envio del ok a Marta");
											exit(-1);
										}
										if ((send(marta_sock, &nodo_para_marta->puerto_escucha_nodo,sizeof(int), MSG_WAITALL)) == -1) {
											perror("send");
											log_error(logger, "FALLO el envio del ok a Marta");
											exit(-1);
										}

									}
									int cantidad_archivos=list_size(archivos);
									if ((send(marta_sock, &cantidad_archivos,sizeof(int), MSG_WAITALL)) == -1) {
										perror("send");
										log_error(logger, "FALLO el envio del ok a Marta");
										exit(-1);
									}
									for (cantidad_archivos=0;cantidad_archivos<list_size(archivos);cantidad_archivos++){
										t_archivo *unArchivo=malloc(sizeof(t_archivo));
										unArchivo=list_get(archivos,cantidad_archivos);
										if ((send(marta_sock, unArchivo->nombre,sizeof(unArchivo->nombre), MSG_WAITALL)) == -1) {
											perror("send");
											log_error(logger, "FALLO el envio del ok a Marta");
											exit(-1);
										}
										if ((send(marta_sock, &unArchivo->padre,sizeof(uint32_t), MSG_WAITALL)) == -1) {
											perror("send");
											log_error(logger, "FALLO el envio del ok a Marta");
											exit(-1);
										}
										if ((send(marta_sock, &unArchivo->estado,sizeof(uint32_t), MSG_WAITALL)) == -1) {
											perror("send");
											log_error(logger, "FALLO el envio del ok a Marta");
											exit(-1);
										}
										int cantidad_bloques=list_size(unArchivo->bloques);
										if ((send(marta_sock, &cantidad_bloques,sizeof(int), MSG_WAITALL)) == -1) {
											perror("send");
											log_error(logger, "FALLO el envio del ok a Marta");
											exit(-1);
										}

										for (cantidad_bloques=0;cantidad_bloques<list_size(unArchivo->bloques);cantidad_bloques++){
											t_bloque *unBloque=malloc(sizeof(t_bloque));
											unBloque=list_get(unArchivo->bloques,cantidad_bloques);
											int cantidad_copias=list_size(unBloque->copias);
											if ((send(marta_sock, &cantidad_copias,sizeof(int), MSG_WAITALL)) == -1) {
												perror("send");
												log_error(logger, "FALLO el envio del ok a Marta");
												exit(-1);
											}
											for (cantidad_copias=0;cantidad_copias<list_size(unBloque->copias);cantidad_copias++){
												t_copias *unaCopia=malloc(sizeof(t_copias));
												unaCopia=list_get(unBloque->copias,cantidad_copias);
												if ((send(marta_sock, unaCopia->nodo,sizeof(unaCopia->nodo), MSG_WAITALL)) == -1) {
													perror("send");
													log_error(logger, "FALLO el envio del ok a Marta");
													exit(-1);
												}
												if ((send(marta_sock, &unaCopia->bloqueNodo,sizeof(int), MSG_WAITALL)) == -1) {
													perror("send");
													log_error(logger, "FALLO el envio del ok a Marta");
													exit(-1);
												}
											}
										}
									}

								} else {
									printf("Ya existe un proceso marta conectado, no puede haber más de 1\n");
									log_warning(logger,"Ya existe un proceso marta conectado, no puede haber más de 1");
									close(newfd);
								}

							}
							if (read_size > 0 && strncmp(identificacion, "nuevo", 5) == 0) {
								bloquesTotales = malloc(sizeof(int));
								if ((read_size = recv(newfd, bloquesTotales,sizeof(int), MSG_WAITALL)) == -1) {
									perror("recv");
									log_error(logger, "FALLO el RECV");
									exit(-1);
								}
								puerto_escucha_nodo = malloc(sizeof(int));
								if ((read_size = recv(newfd,puerto_escucha_nodo, sizeof(int), MSG_WAITALL)) == -1) {
									perror("recv");
									log_error(logger, "FALLO el RECV");
									exit(-1);
								}
								if ((read_size = recv(newfd, nodo_id,sizeof(nodo_id), MSG_WAITALL)) == -1) {
									perror("recv");
									log_error(logger, "FALLO el RECV");
									exit(-1);
								}
								if (read_size > 0) {
									if (validar_nodo_nuevo(nodo_id) == 0) {
										cantidad_nodos++;
										cantidad_nodos_historico = cantidad_nodos;
										FD_SET(newfd, &master); // añadir al conjunto maestro
										if (newfd > fdmax) { // actualizar el máximo
											fdmax = newfd;
										}
										list_add(nodos,agregar_nodo_a_lista(nodo_id,newfd, 0, 1,inet_ntoa(remote_client.sin_addr),remote_client.sin_port,*puerto_escucha_nodo,*bloquesTotales,*bloquesTotales));
										printf("Se conectó un nuevo nodo: %s con %d bloques totales\n",inet_ntoa(remote_client.sin_addr),*bloquesTotales);
										log_info(logger,"Se conectó un nuevo nodo: %s con %d bloques totales",inet_ntoa(remote_client.sin_addr),*bloquesTotales);
									} else {
										printf("Ya existe un nodo con el mismo id o direccion ip\n");
										close(newfd);
									}
								}
							}
							if (read_size > 0 && strncmp(identificacion, "reconectado",11) == 0) {
								if ((read_size = recv(newfd, nodo_id,sizeof(nodo_id), MSG_WAITALL)) == -1) {
									perror("recv");
									log_error(logger, "FALLO el RECV");
									exit(-1);
								}
								if ((validar_nodo_reconectado(nodo_id)) == 0) {
									cantidad_nodos++;
									FD_SET(newfd, &master); // añadir al conjunto maestro
									if (newfd > fdmax) { // actualizar el máximo
										fdmax = newfd;
									}
									modificar_estado_nodo(nodo_id, newfd,remote_client.sin_port, 0, 1); //cambio su estado de la lista a 1 que es activo
									printf("Se reconectó el nodo %s\n",inet_ntoa(remote_client.sin_addr));
									log_info(logger, "Se reconectó el nodo %s",inet_ntoa(remote_client.sin_addr));
								} else {
									printf("Se reconecto un nodo con datos alterados, se lo desconecta\n");
									close(newfd);
								}

							}
						}
					}

					//.................................................
					//hasta aca, es el tratamiento de conexiones nuevas
					//.................................................

				} else { //si entra aca no es un cliente nuevo, es uno que ya tenia y me esta mandando algo
					// gestionar datos de un cliente

					if (i == marta_sock) {
						if ((read_size = recv(i, mensaje, sizeof(mensaje), MSG_WAITALL)) <= 0) { //si entra aca es porque se desconecto o hubo un error
							if (read_size == 0) {
								marta_presente = 0;
								close(i); // ¡Hasta luego!
								FD_CLR(i, &master); // eliminar del conjunto maestro
								printf("Marta se desconecto\n");
							}else{
								//Marta me manda algo
							}
						}
					}else{
						//Si no es marta, es un nodo
						if ((read_size = recv(i, mensaje, sizeof(mensaje), MSG_PEEK)) <= 0){
							if (read_size == 0) {
								//Se desconecto
								addrlen = sizeof(struct sockaddr_in);
								if ((getpeername(i,(struct sockaddr*) &remote_client,(socklen_t*) &addrlen)) == -1) {
									perror("getpeername");
									log_error(logger, "Fallo el getpeername");
									exit(-1);
								}
								char *id_temporal;
								id_temporal = buscar_nodo_id(inet_ntoa(remote_client.sin_addr),remote_client.sin_port);
								if (id_temporal != NULL) {
									strcpy(nodo_id, id_temporal);
									modificar_estado_nodo(nodo_id, i,remote_client.sin_port, 0, 0);
									printf("Se desconecto el nodo %s, %d\n",inet_ntoa(remote_client.sin_addr),remote_client.sin_port);
									close(i); // ¡Hasta luego!
									FD_CLR(i, &master); // eliminar del conjunto maestro
								} else {
									printf("ALGO SALIO MUY MAL\n");
									exit(-1);
								}
							}else{
								perror("recv");
								log_error(logger, "FALLO el Recv");
								exit(-1);
							}
						}
						//Recibi algo de un nodo
					}
				}
			}
		}
	}
}

//Buscar el id del padre
uint32_t BuscarPadre(char* path) {
	t_dir* dir;
	int directorioPadre = 0,tamanio; //seteo a raíz
	if (( tamanio = list_size(directorios))==0 || string_is_empty(path) || strcmp(path,"/")==0){ //No hay directorios
		//printf("No se encontró el directorio\n");
		directorioPadre = -1;
		return directorioPadre;
	}
	int contadorDirectorio = 0;
	int i;
	char** directorio = string_split(path, "/"); //Devuelve un array del path
	//Obtener id del padre del archivo(ante-ultima posición antes de NULL)
	while (directorio[contadorDirectorio] != NULL) {
		for (i = 0; i < tamanio; i++) { //recorro lista de directorios
			dir = list_get(directorios, i); //agarro primer directorio de la lista de directorios
			//comparo si el nombre es igual al string del array del path y el padre es igual al padre anterior
			if (((strcmp(dir->nombre, directorio[contadorDirectorio])) == 0) && (dir->padre == directorioPadre)) {
				directorioPadre = dir->id;
				contadorDirectorio++;
				break;
			} else {
				if (i == tamanio - 1) {
					//printf("No se encontró el directorio");
					directorioPadre = -1;
					return directorioPadre;
				}
			}
		}
	}
	return directorioPadre;
}

//Buscar la posición del nodo de un archivo de la lista t_archivo por el nombre del archivo y el id del padre
int BuscarArchivoPorNombre(const char *path, uint32_t idPadre) {
	t_archivo* archivo;
	archivo = malloc(sizeof(t_archivo));
	int i, posicionArchivo;
	char* nombreArchivo;
	int posArchivo = 0;
	int tam = list_size(archivos);
	char** directorio = string_split((char*) path, "/"); //Devuelve un array del path
	//Obtener solo el nombre del archivo(ultima posición antes de NULL)
	for (i = 0; directorio[i] != NULL; i++) {
		if (directorio[i + 1] == NULL) {
			nombreArchivo = directorio[i];
		}
	}
	if(tam!=0){
		for (posArchivo = 0; posArchivo < tam; posArchivo++) {
			archivo = list_get(archivos, posArchivo);
			if ((strcmp(archivo->nombre, nombreArchivo) == 0) && (archivo->padre == idPadre)) {
				posicionArchivo = posArchivo;
				break;
			} else {
				if (posArchivo == tam - 1) {
					//printf("No se encontró el archivo");
					posicionArchivo = -1;
					return posicionArchivo;
				}
			}
		}
	}
	if(tam==0){
		posicionArchivo=-1;
		return posicionArchivo;
	}
	return posicionArchivo;
}

int BuscarMenorIndiceLibre(char indiceDirectorios[]) {
	//Tengo un vector donde guardo los indices para ver cuales tengo libres ya que no puedo superar 1024
	int i = 0;
	while (i < MAX_DIRECTORIOS && indiceDirectorios[i] == 1) { //Mientas sea menor a 1024 y esté ocupado, sigo buscando
		i++;
	}

	if (i < MAX_DIRECTORIOS) {
		return i; //devuelvo el menor indice libre
	} else {
		return -1; //no puedo seguir creando, me protejo de esta situación con la variable directoriosDisponibles
	}
}

void modificar_estado_nodo(char nodo_id[6], int socket, int port, int estado, int estado_red) {
	int i;
	t_nodo *tmp;
	for (i = 0; i < list_size(nodos); i++) {
		tmp = list_get(nodos, i);

		if (strcmp(tmp->nodo_id, nodo_id) == 0) {
			if (estado_red == 99) {
				tmp->estado = estado;
				break;
			} else {
				tmp->puerto = port;
				tmp->estado = estado;
				tmp->socket = socket;
				tmp->estado_red = estado_red;
				break;
			}
		}
	}
}

static void eliminar_lista_de_copias (t_copias *self){
	free(self->nodo);
	free(self);
}
static void eliminar_lista_de_nodos (t_nodo *self){
	bitarray_destroy(self->bloques_del_nodo);
	free(self->bloques_bitarray);
	free(self->ip);
	free(self);
}
static void eliminar_lista_de_bloques(t_bloque *self){
	list_destroy(self->copias);
	free(self);
}

static void eliminar_lista_de_archivos (t_archivo *self){
	list_destroy(self->bloques);
	free(self);
}
static void eliminar_lista_de_directorio(t_dir *self){
	free(self->nombre);
	free(self);
}

void FormatearFilesystem() {
	int i,j,k;
	printf("Eligió  Formatear el MDFS\n");
	//=====================================================================
	//======================= FORMATEO PARTE 1 ============================
	//==================ELIMINO LA LISTA DE ARCHIVOS=======================
	//=====================================================================
	t_archivo *archi=malloc(sizeof(t_archivo));
	t_bloque *bloq=malloc(sizeof(t_bloque));

	for (i=0;i<list_size(archivos);i++){
		archi=list_get(archivos,i);
		for (j=0;j<list_size(archi->bloques);j++){
			bloq=list_get(archi->bloques,j);
			for (k=0;k<list_size(bloq->copias);k++){
				list_remove_and_destroy_element(bloq->copias,k,(void*)eliminar_lista_de_copias);
			}
			list_remove_and_destroy_element(archi->bloques,j,(void*)eliminar_lista_de_bloques);
		}
		list_remove_and_destroy_element(archivos,i,(void*)eliminar_lista_de_archivos);
	}

	//=====================================================================
	//======================= FORMATEO PARTE 2 ============================
	//================= VACIO LOS NODOS PARA QUEDE 0KM ====================
	//=====================================================================

	t_nodo *unNodo;
	for (i=0;i<list_size(nodos);i++){
		unNodo=list_get(nodos,i);
		unNodo->estado=0;    //PONGO EL ESTADO EN NO DISPONIBLE
		unNodo->bloques_libres=unNodo->bloques_totales;  //PONGO QUE TODOS LOS BLOQUES ESTAN DISPONIBLES
		for (k = 0; k < unNodo->bloques_totales; k++)
			bitarray_clean_bit(unNodo->bloques_del_nodo, k);   //MARCO EN TODOS LOS BITS DEL BITARRAY QUE LOS BLOQUES ESTAN DISPONIBLES
	}

	//=====================================================================
	//======================= FORMATEO PARTE 3 ============================
	//==================ELIMINO LA LISTA DE DIRECTORIOS====================
	//=====================================================================

	list_clean_and_destroy_elements(directorios,(void*)eliminar_lista_de_directorio);
	//TODO: AVISAR A MARTA QUE VACIE SUS ESTRUCTURAS
}

void EliminarArchivo() {
	t_archivo* archivo=malloc(sizeof(t_archivo));
	t_bloque* bloque=malloc(sizeof(t_bloque));
	t_copias* copia=malloc(sizeof(t_copias));
	t_nodo* nodoBuscado=malloc(sizeof(t_nodo));
	printf("Eligió  Eliminar archivo\n");
	char* path = string_new();
	char* directorio;
	int i,j,m,posicionDirectorio=0;
	char ** directoriosPorSeparado;
	directorio=string_new();
	printf("Ingrese el path del archivo \n");
	scanf("%s", path);
    directoriosPorSeparado=string_split(path,"/");
    while(directoriosPorSeparado[posicionDirectorio+1]!=NULL){
    	string_append(&directorio,"/");
    	string_append(&directorio,directoriosPorSeparado[posicionDirectorio]);
    	posicionDirectorio++;
    }
	uint32_t idPadre = BuscarPadre(directorio);
	if (idPadre==-1){
		printf("El archivo no existe\n");
		Menu();
	}
	uint32_t posArchivo = BuscarArchivoPorNombre(path, idPadre);
	archivo = list_get(archivos, posArchivo);
	for (i=0;i<list_size(archivo->bloques);i++){
		bloque=list_get(archivo->bloques,i);
		for (j=0;j<list_size(bloque->copias);j++){

			copia=list_get(bloque->copias,j);

			for (m=0;m<list_size(nodos);m++){
				nodoBuscado = list_get(nodos,m);
				if (strcmp(nodoBuscado->nodo_id, copia->nodo)==0) {
					nodoBuscado->bloques_libres++;
					bitarray_clean_bit(nodoBuscado->bloques_del_nodo, copia->bloqueNodo);
					//break;
				}
			}
		}

		for (j=0;j<list_size(bloque->copias);j++){
			list_remove_and_destroy_element(bloque->copias,j,(void*)eliminar_lista_de_copias);
		}
	}
	for (i=0;i<list_size(archivo->bloques);i++){
		list_remove_and_destroy_element(archivo->bloques,i,(void*)eliminar_lista_de_bloques);
	}
	list_remove_and_destroy_element(archivos,posArchivo,(void*)eliminar_lista_de_archivos);

	//TODO: ACTUALIZAR EL ARCHIVO ELIMINADO A MARTA
}
void RenombrarArchivo() {
	t_archivo* archivo=malloc(sizeof(t_archivo));
	char ** directoriosPorSeparado;
	char* directorio;
	int posicionDirectorio=0;
	printf("Eligió Renombrar archivos\n");
	char* path = string_new();
	directorio=string_new();
	char* nuevoNombre = string_new();
	printf("Ingrese el path del archivo \n");
	scanf("%s", path);
	directoriosPorSeparado=string_split(path,"/");
	while(directoriosPorSeparado[posicionDirectorio+1]!=NULL){
		string_append(&directorio,"/");
		string_append(&directorio,directoriosPorSeparado[posicionDirectorio]);
		posicionDirectorio++;
	}
	uint32_t idPadre = BuscarPadre(directorio);
	if (idPadre==-1){
		printf("El archivo no existe\n");
		Menu();
	}
	uint32_t posArchivo = BuscarArchivoPorNombre(path, idPadre);
	archivo = list_get(archivos, posArchivo);
	printf("Ingrese el nuevo nombre \n");
	scanf("%s", nuevoNombre);
	strcpy(archivo->nombre, nuevoNombre);

	//TODO: ACTUALIZAR EL NUEVO NOMBRE A MARTA
}
void MoverArchivo() {

	t_archivo* archivo=malloc(sizeof(t_archivo));
	printf("Eligió Mover archivos\n");
	char path[100];
	char nuevoPath[100];
	char **directoriosPorSeparado;
	int posicionDirectorio=0;
	char *directorioDestino=string_new();
	printf("Ingrese el path del archivo \n");
	memset(path,'\0',100);
	scanf("%s", path);

	directoriosPorSeparado=string_split(path,"/");
	while(directoriosPorSeparado[posicionDirectorio+1]!=NULL){
		string_append(&directorioDestino,"/");
		string_append(&directorioDestino,directoriosPorSeparado[posicionDirectorio]);
		posicionDirectorio++;
	}
	int idPadre = BuscarPadre(directorioDestino);
	printf ("Padre: %d\n",idPadre);
	if (idPadre==-1){
		printf ("El path no existe\n");
		Menu();
	}
	int posArchivo = BuscarArchivoPorNombre(path, idPadre);
	if (posArchivo==-1){
		printf ("El archivo no existe\n");
		Menu();
	}
	archivo = list_get(archivos, posArchivo);

	printf("Ingrese el nuevo path \n");
	memset(nuevoPath,'\0',100);
	scanf("%s", nuevoPath);
	int idPadreNuevo = BuscarPadre(nuevoPath);
	if (idPadreNuevo==-1){
		printf ("El path destino no existe\n");
		Menu();
	}
	printf("andre was here\n");
	archivo->padre = idPadreNuevo;
}

long ExisteEnLaLista(t_list* listaDirectorios, char* nombreDirectorioABuscar,uint32_t idPadre) {
	t_dir* elementoDeMiLista;
	elementoDeMiLista = malloc(sizeof(t_dir));
	long encontrado = -1; //trae -1 si no lo encuentra, sino trae el id del elemento
	//uso long para encontrado para cubrir el universo de uint32_t y además el -1 que necesito si no encuentro
	int tamanioLista = list_size(listaDirectorios);
	int i = 0;
	while (encontrado == -1 && i < tamanioLista) {
		elementoDeMiLista = list_get(listaDirectorios, i);
		if (strcmp(elementoDeMiLista->nombre, nombreDirectorioABuscar) == 0) { //está en mi lista un directorio con ese nombre?
			if (elementoDeMiLista->padre == idPadre) { //el que encuentro con el mismo nombre, tiene el mismo padre?
				//considero directorios con mismo nombre pero con distintos padres Ej: /utnso/tp/operativos y /utnso/operativos
				encontrado = elementoDeMiLista->id;
			}
		}
		i++;
	}
	return encontrado;
}

void CrearDirectorio() {
	//printf("Eligió Crear directorios\n");
	uint32_t idPadre;
	char path[200];
	char** directorioNuevo;
	t_dir* directorioACrear;
	int cantDirACrear = 0;
	directorioACrear = malloc(sizeof(t_dir));
	long idAValidar; //uso este tipo para cubrir rango de uint32_t y el -1,  deberia mejorar el nombre de la variable
	printf("Ingrese el path del directorio desde raíz ejemplo /home/utnso \n");
	memset(path,'\0',200);
	scanf("%s", path);
	directorioNuevo = string_split((char*) path, "/"); //Devuelve un array del path del directorio a crear
	//int indiceVectorDirNuevo=1;
	int indiceVectorDirNuevo = 0; //empiezo por el primero del split
	while (directorioNuevo[indiceVectorDirNuevo] != NULL) {
		//list_find(directorios,(void*) ExisteEnLaLista());  //ver más adelante de usar la función de lcommons
		if (indiceVectorDirNuevo == 0) { //el primero del split siempre va a ser hijo de raiz
			idPadre = 0;
		}
		idAValidar = ExisteEnLaLista(directorios,
				directorioNuevo[indiceVectorDirNuevo], idPadre);
		if (idAValidar != -1) {  //quiere decir que existe
			if (directorioNuevo[indiceVectorDirNuevo + 1] == NULL) {
				printf("El directorio ingresado ya existe. No se realizara ninguna accion \n");
			} else {
				idPadre = (uint32_t) idAValidar; //actualizo valor del padre con el que existe y avanzo en split para ver el siguiente directorio
			}
			indiceVectorDirNuevo++;
		} else { //hay que crear directorio
			int indiceDirectoriosNuevos;
			for (indiceDirectoriosNuevos = indiceVectorDirNuevo;directorioNuevo[indiceDirectoriosNuevos] != NULL;indiceDirectoriosNuevos++) {
				cantDirACrear++;
			}
			if (cantDirACrear <= directoriosDisponibles) { //controlo que no supere la cantidad maxima que es 1024
				while (directorioNuevo[indiceVectorDirNuevo] != NULL) {
					directorioACrear = malloc(sizeof(t_dir));
					directorioACrear->nombre = directorioNuevo[indiceVectorDirNuevo];
					directorioACrear->padre = idPadre;
					//persistir en la db: pendiente
					int id = BuscarMenorIndiceLibre(indiceDirectorios); //el nuevo id será el menor libre del vector de indices de directorios, siempre menor a 1024
					directorioACrear->id = id;
					indiceDirectorios[id] = 1; //marco como ocupado el indice correspondiente
					directoriosDisponibles--; //actualizo mi variable para saber cantidad de directorios máximos a crear
					idPadre = directorioACrear->id;
					list_add(directorios, directorioACrear);
					indiceVectorDirNuevo++;

				}
				printf("El directorio se ha creado satisfactoriamente \n");
			} else {
				printf("No se puede crear el directorio ya que sobrepasaría el límite máximo de directorios permitidos: %d\n",MAX_DIRECTORIOS);
				//No puede pasarse de 1024 directorios
			}
		}
	}
}

static void directorio_destroy(t_dir* self) {
	free(self->nombre);
	free(self);
}

void eliminar_listas(t_list *archivos_l, t_list *directorios_l, t_list *nodos_l){
	int i,j,k;
	//=====================================================================
	//======================= LIBERAR PARTE 1 =============================
	//==================ELIMINO LA LISTA DE ARCHIVOS=======================
	//=====================================================================
	if (archivos_l!=NULL){
		t_archivo *archi=malloc(sizeof(t_archivo));
		t_bloque *bloq=malloc(sizeof(t_bloque));

		for (i=0;i<list_size(archivos_l);i++){
			archi=list_get(archivos_l,i);
			for (j=0;j<list_size(archi->bloques);j++){
				bloq=list_get(archi->bloques,j);
				for (k=0;k<list_size(bloq->copias);k++){
					list_remove_and_destroy_element(bloq->copias,k,(void*)eliminar_lista_de_copias);
				}
				list_remove_and_destroy_element(archi->bloques,j,(void*)eliminar_lista_de_bloques);
			}
			list_remove_and_destroy_element(archivos_l,i,(void*)eliminar_lista_de_archivos);
		}
		list_destroy(archivos_l);

	}
	//=====================================================================
	//======================= LIBERAR PARTE 2 =============================
	//====================== LIBERO LOS NODOS =============================
	//=====================================================================

	if (nodos_l!=NULL){
		list_clean_and_destroy_elements(nodos_l,(void*)eliminar_lista_de_nodos);
		list_destroy(nodos_l);
	}
	//=====================================================================
	//======================= FORMATEO PARTE 3 ============================
	//==================ELIMINO LA LISTA DE DIRECTORIOS====================
	//=====================================================================

	if (directorios_l!=NULL){
		list_clean_and_destroy_elements(directorios_l,(void*)eliminar_lista_de_directorio);
		list_destroy(directorios_l);
	}
	if (archivos_l!=NULL && directorios_l!=NULL && nodos_l!=NULL){
		printf ("Adios!\n");
		exit(0);
	}

}

void EliminarDirectorio() {
	//printf("Eligió Eliminar directorios\n");
	char* pathAEliminar =  string_new();
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
	char encontrePos; //0 si no lo encuentra en la lista de directorios, 1 si lo encuentra
	char tieneDirOArch; //0 si no tiene, 1 si tiene subdirectorio o archivo
	int posicionElementoAEliminar;
	printf("Ingrese el path a eliminar desde raíz ejemplo /home/utnso \n");
	scanf("%s", pathAEliminar);
	vectorpathAEliminar = string_split((char*) pathAEliminar, "/");
	while (vectorpathAEliminar[i] != NULL && idEncontrado != -1) {
		if (i == 0) {
			idEncontrado = 0; //el primero que cuelga de raiz
		}
		idEncontrado = ExisteEnLaLista(directorios, vectorpathAEliminar[i],idEncontrado);
		i++;
	}
	if (idEncontrado == -1) {
		printf("No existe el directorio para eliminar \n");
	} else {
		tieneDirOArch = 0;
		idAEliminar = idEncontrado;
		i = 0;
		while (tieneDirOArch == 0 && i < tamanioListaDir) {
			elementoDeMiListaDir = list_get(directorios, i);
			if (elementoDeMiListaDir->padre == idAEliminar) { //Si tengo directorios que tengan como padre al dir que quiero eliminar
				tieneDirOArch = 1;
			}
			i++;
		}
		if (tieneDirOArch == 1) {
			printf(
					"El directorio que desea eliminar no puede ser eliminado ya que posee subdirectorios \n");
		} else {
			i = 0;
			while (tieneDirOArch == 0 && i < tamanioListaArch) {
				elementoDeMiListaArch = list_get(archivos, i);
				if (elementoDeMiListaArch->padre == idAEliminar) {
					tieneDirOArch = 1;
				}
				i++;
			}
			if (tieneDirOArch == 1) {
				printf("El directorio que desea eliminar no puede ser eliminado ya que posee archivos \n");
			} else {
				i = 0;
				encontrePos = 0; //no lo encontre
				while (encontrePos == 0 && i < tamanioListaDir) {
					elementoDeMiListaDir = list_get(directorios, i);
					if (elementoDeMiListaDir->id == idAEliminar) {
						encontrePos = 1;
					}
					i++;
				}
				posicionElementoAEliminar = i - 1;
				//list_remove_and_destroy_element(t_list *, int index, void(*element_destroyer)(void*));
				list_remove_and_destroy_element(directorios,posicionElementoAEliminar, (void*) directorio_destroy);
				indiceDirectorios[idAEliminar] = 0; //Desocupo el indice en vector de indices disponibles para poder usar ese id en el futuro
				directoriosDisponibles++; //Incremento la cantidad de directorios libres
				printf("El directorio se ha eliminado correctamente. \n");
			}
		}
	}
}

void RenombrarDirectorio() {
	//printf("Eligió Renombrar directorios\n");
	//char* pathOriginal = string_new();
	char pathOriginal[200];
	memset(pathOriginal, '\0', 200);
	char** vectorPathOriginal;
	//char* pathNuevo = string_new();
	char pathNuevo[200];
	memset(pathNuevo, '\0', 200);
	t_dir* elementoDeMiLista;
	elementoDeMiLista = malloc(sizeof(t_dir));
	int tamanioLista = list_size(directorios);
	int i = 0;
	uint32_t idARenombrar;
	long idEncontrado = 0;
	char encontrado; //0 si no lo encontro, 1 si lo encontro
	printf("Ingrese el path del original desde raíz ejemplo /home/utnso \n");
	scanf("%s", pathOriginal);
	printf("Ingrese el nuevo nombre de directorio \n");
	scanf("%s", pathNuevo);
	vectorPathOriginal = string_split((char*) pathOriginal, "/");
	while (vectorPathOriginal[i] != NULL && idEncontrado != -1) {
		if (i == 0) {
			idEncontrado = 0; //el primero que cuelga de raiz
		}
		idEncontrado = ExisteEnLaLista(directorios, vectorPathOriginal[i],idEncontrado);
		i++;
	}
	if (idEncontrado == -1) {
		printf("No existe el directorio para renombrar \n");
	} else {
		i = 0;
		encontrado = 0;
		idARenombrar = idEncontrado;
		while (encontrado == 0 && i < tamanioLista) {
			elementoDeMiLista = list_get(directorios, i);
			if (elementoDeMiLista->id == idARenombrar) {
				encontrado = 1;
				strcpy(elementoDeMiLista->nombre, pathNuevo);
			}
			i++;
		}
		printf("El directorio se ha renombrado exitosamente. \n");
	}
}

void MoverDirectorio() {
	//printf("Eligió Mover directorios\n");
	char* pathOriginal = string_new();
	char** vectorPathOriginal;
	char* pathNuevo = string_new();
	char** vectorPathNuevo;
	char* nombreDirAMover = string_new();
	t_dir* elementoDeMiLista;
	elementoDeMiLista = malloc(sizeof(t_dir));
	int tamanioLista = list_size(directorios);
	int i = 0;
	uint32_t idDirAMover;
	uint32_t idNuevoPadre;
	long idEncontrado = 0;
	char encontrado; //0 si no lo encontro, 1 si lo encontro
	printf("Ingrese el path original desde raíz ejemplo /home/utnso \n");
	scanf("%s", pathOriginal);
	printf("Ingrese el path del directorio al que desea moverlo desde raíz ejemplo /home/tp \n");
	scanf("%s", pathNuevo);
	vectorPathOriginal = string_split((char*) pathOriginal, "/");
	vectorPathNuevo = string_split((char*) pathNuevo, "/");
	while (vectorPathOriginal[i] != NULL && idEncontrado != -1) {
		if (i == 0) {
			idEncontrado = 0; //el primero que cuelga de raiz
		}
		idEncontrado = ExisteEnLaLista(directorios, vectorPathOriginal[i],idEncontrado);
		i++;
	}
	if (idEncontrado == -1) {
		printf("No existe el path original \n");
	} else {
		idDirAMover = idEncontrado;
		strcpy(nombreDirAMover, vectorPathOriginal[(i - 1)]); //revisar, puse -1 porque avancé hasta el NULL.
		idEncontrado = 0;
		i = 0;
		while (vectorPathNuevo[i] != NULL && idEncontrado != -1) {
			if (i == 0) {
				idEncontrado = 0; //el primero que cuelga de raiz
			}
			idEncontrado = ExisteEnLaLista(directorios, vectorPathNuevo[i],idEncontrado);
			i++;
		}
		if (idEncontrado == -1) {
			printf("No existe el path al que desea moverlo \n");
		} else {
			idNuevoPadre = idEncontrado;
			if (ExisteEnLaLista(directorios, nombreDirAMover, idNuevoPadre)	== -1) { //ver si el padre no tiene hijos que se llamen igual que el directorio a mover
				i = 0;
				encontrado = 0;
				while (encontrado == 0 && i < tamanioLista) {
					elementoDeMiLista = list_get(directorios, i);
					if (elementoDeMiLista->id == idDirAMover) {
						encontrado = 1;
						elementoDeMiLista->padre = idNuevoPadre;
					}
					i++;
				}
				printf("El directorio se ha movido satisfactoriamente \n");
			} else {
				printf("El directorio no está vacío \n");
			}
		}
	}
}

bool nodos_mas_libres(t_nodo *vacio, t_nodo *mas_vacio) {
	return vacio->bloques_libres > mas_vacio->bloques_libres;
}

int copiar_lista_de_archivos(t_list* destino, t_list* origen){
	int i,j,k;
	for (i=0;i<list_size(origen);i++){
		t_archivo *original=malloc(sizeof(t_archivo));
		t_archivo *copia=malloc(sizeof(t_archivo));
		original=list_get(origen,i);
		copia->bloques=original->bloques;
		copia->estado=original->estado;
		strcpy(copia->nombre,original->nombre);
		copia->padre=original->padre;
		copia->bloques=list_create();
		for (j=0;j<list_size(original->bloques);j++){
			t_bloque *bloque_original=malloc(sizeof(t_bloque));
			t_bloque *bloque_copia=malloc(sizeof(t_bloque));
			bloque_original=list_get(original->bloques,j);
			bloque_copia->copias=list_create();
			for(k=0;k<list_size(bloque_original->copias);k++){
				t_copias *copia_original=malloc(sizeof(t_copias));
				t_copias *copia_copia=malloc(sizeof(t_copias));
				copia_original=list_get(bloque_original->copias,k);
				copia_copia->bloqueNodo=copia_original->bloqueNodo;
				strcpy(copia_copia->md5,copia_original->md5);
				copia_copia->nodo=strdup(copia_original->nodo);
				list_add(bloque_copia->copias,copia_copia);
			}
			list_add(copia->bloques,bloque_copia);
		}
		list_add(destino,copia);
	}
	return 0;
}
int copiar_lista_de_nodos(t_list* destino, t_list* origen){
	int i,k;
	for (i=0;i<list_size(origen);i++){
		t_nodo *original=malloc(sizeof(t_nodo));
		t_nodo *copia=malloc(sizeof(t_nodo));
		original=list_get(origen,i);
		memset(copia->nodo_id, '\0', 6);
		strcpy(copia->nodo_id, original->nodo_id);
		copia->socket = original->socket;
		copia->estado = original->estado;
		copia->estado_red = original->estado_red;
		copia->ip = strdup(original->ip);
		copia->puerto = original->puerto;
		copia->bloques_libres = original->bloques_libres;
		copia->bloques_totales = original->bloques_totales;
		copia->puerto_escucha_nodo = original->puerto_escucha_nodo;

		//Creo e inicializo el bitarray del nodo, 0 es bloque libre, 1 es blloque ocupado
		//Como recien se esta conectadno el nodo, todos sus bloques son libres
		for (k = 8; k < original->bloques_totales; k += 8);
		copia->bloques_bitarray = malloc(i / 8);
		copia->bloques_del_nodo = bitarray_create(copia->bloques_bitarray, i / 8);
		for (k = 0; k < copia->bloques_totales; k++)
			if (!bitarray_test_bit(original->bloques_del_nodo, k))
				bitarray_clean_bit(copia->bloques_del_nodo, k);
			else bitarray_set_bit(copia->bloques_del_nodo, k);
		list_add(destino,copia);

	}
	return 0;
}

int CopiarArchivoAMDFS(){

	printf("Eligió Copiar un archivo local al MDFS\n");
	FILE * archivoLocal;
    char* directorioDestino=string_new();
    char ** directoriosPorSeparado;
    int posicionDirectorio=0;
    char path[100];
    char pathMDFS[100];
    memset(path,'\0',100);
    memset(pathMDFS,'\0',100);
    nodos_temporales=list_create();
	if (copiar_lista_de_nodos(nodos_temporales,nodos)){
	 	printf ("No se pudo crear la copia de la lista de nodos\n");
	 	return -1;
	}
    t_nodo *nodo_temporal;
    t_archivo *archivo_temporal;

    archivos_temporales=list_create();
    if (copiar_lista_de_archivos(archivos_temporales,archivos)){
    	printf ("No se pudo crear la copia de la lista de nodos\n");
    	return -1;
    }

    char handshake[15]="copiar_archivo";
	char ruta[100];
	memset(ruta,'\0',100);
	int indice=0;
	uint32_t cantBytes=0;
	int pos=0;
	int total_enviado;
	int corte=0;
	//char car;
    int j;
	printf("Ingrese el path del archivo local desde raíz, por ejemplo /home/tp/nombreArchivo \n");
	scanf("%99s", path);
	//Validacion de si existe el archivo en el filesystem local
    if((archivoLocal = fopen(path,"r"))==NULL){
    	log_error(logger,"El archivo que quiere copiar no existe en el filesystem local");
    	perror("fopen");
    	Menu();
    }
    printf("Ingrese el path del archivo destino desde raíz, por ejemplo /tmp/nombreArchivo \n");
    scanf("%99s", pathMDFS);
    directoriosPorSeparado=string_split(pathMDFS,"/");
    while(directoriosPorSeparado[posicionDirectorio+1]!=NULL){
    	string_append(&directorioDestino,"/");
    	string_append(&directorioDestino,directoriosPorSeparado[posicionDirectorio]);
    	posicionDirectorio++;
    }
    //Buscar Directorio. Si existe se muestra mensaje de error
    uint32_t idPadre = BuscarPadre(directorioDestino);
    if(idPadre == -1){
      	printf("El directorio no existe. Se debe crear el directorio desde el menú. \n");
       	Menu();
    }
    //Buscar Archivo. Si no existe se muestra mensaje de error y se debe volver al menú para crearlo
    uint32_t posArchivo = BuscarArchivoPorNombre (pathMDFS,idPadre);
    if(!(posArchivo == -1)){
     printf("El archivo ya existe. Se debe especificar un archivo nuevo. \n");
     Menu();
    }
    //Se debe crear un nuevo archivo con el nombre ingresado, cuyo padre sea "idPadre"
    int n_copia=0;
    int bandera;
    //memset(combo.buf_20mb,0,sizeof(combo.buf_20mb));
    memset(combo.buf_20mb,'\0',BLOCK_SIZE);
    archivo_temporal=malloc(sizeof(t_archivo));
    archivo_temporal->bloques=list_create();
    while (fread(&combo.buf_20mb,sizeof(char),sizeof(combo.buf_20mb),archivoLocal) == BLOCK_SIZE){
    		cantBytes+=BLOCK_SIZE;
    		n_copia++;
    		t_bloque *bloque_temporal=malloc(sizeof(t_bloque));
    		bloque_temporal->copias=list_create();
    		if (combo.buf_20mb[BLOCK_SIZE-1]=='\n'){
    			list_sort(nodos_temporales, (void*)nodos_mas_libres);
    			//Copiar el contenido del Buffer en los nodos mas vacios por triplicado
    			bandera=0;
    			for (indice=0;indice<list_size(nodos_temporales);indice++){
    				if (bandera==3) break;
    				nodo_temporal=list_get(nodos_temporales,indice);
    				if (nodo_temporal->estado == 1 && nodo_temporal->bloques_libres > 0){  //controlo que el nodo tenga espacio y este habilitado
    					bandera++;
    					if (send(nodo_temporal->socket, handshake, sizeof(handshake), MSG_WAITALL) == -1) {
    						perror("send handshake en funcion subir archivo");
							log_error(logger, "FALLO el envio del aviso de obtener bloque ");
							exit(-1);
						}
						corte=0;
						for(combo.n_bloque=0;combo.n_bloque<nodo_temporal->bloques_totales;combo.n_bloque++){
							if (!bitarray_test_bit(nodo_temporal->bloques_del_nodo,combo.n_bloque)){
								corte=1;
								break;
							}
						}
						if (corte==0){
							printf ("El nodo %s no tiene bloques libles, se cancela la subida del archivo\n",nodo_temporal->nodo_id);
							return -1;
						}
						printf ("voy a mandar al nodo %s la copia %d del bloque %d y la guardara en el bloque %d\n",nodo_temporal->nodo_id,indice+1,n_copia,combo.n_bloque);
						if (send(nodo_temporal->socket, &combo, sizeof(combo), 0) == -1) {
							perror("send buffer en subir archivo");
							log_error(logger, "FALLO el envio del aviso de obtener bloque ");
							exit(-1);
						}
						bitarray_set_bit(nodo_temporal->bloques_del_nodo,combo.n_bloque);
						nodo_temporal->bloques_libres--;

						//Agrego la copia del bloque a la lista de copias de este bloque particular
						t_copias *copia_temporal=malloc(sizeof(t_copias));
						copia_temporal->bloqueNodo=combo.n_bloque;
						char *cadena_para_md5=malloc(20971520);
						strcpy(cadena_para_md5,combo.buf_20mb);
						memset(copia_temporal->md5,'\0',32);
						strcpy(copia_temporal->md5,obtener_md5(cadena_para_md5));
						copia_temporal->nodo=strdup(nodo_temporal->nodo_id);
						list_add(bloque_temporal->copias,copia_temporal);
						free(cadena_para_md5);
    				}
    			}
    			if (bandera!=3){
    				printf ("No hay suficientes nodos disponibles con espacio libre\n");
    				return -1;
    			}
    			memset(combo.buf_20mb,'\0',BLOCK_SIZE);
    		}else{ //Caso en que el bloque no termina en "\n"
    			int p,aux;
    			for (p=BLOCK_SIZE-1,aux=0;p>=0;p--,aux++){
    				if (combo.buf_20mb[p]=='\n'){
    					pos=p;
    					break;
    				}
    			}
    			for(j=pos+1;j<BLOCK_SIZE;j++) combo.buf_20mb[j]='\0';
    			list_sort(nodos_temporales,(void*)nodos_mas_libres);
    			//Copiar el contenido del Buffer en los nodos mas vacios por triplicado
    			bandera=0;
    			for (indice=0;indice<list_size(nodos_temporales);indice++){
    				if (bandera==3) break;
    				nodo_temporal=list_get(nodos_temporales,indice);
    				if (nodo_temporal->estado == 1 && nodo_temporal->bloques_libres > 0){
    					bandera++;
    					if ((total_enviado=send(nodo_temporal->socket, handshake, sizeof(handshake), MSG_WAITALL)) == -1) {
    						perror("send error del envio de handshake en subir archivo");
    						log_error(logger, "FALLO el envio del aviso de obtener bloque ");
    						exit(-1);
    					}
    					printf ("Lo que mande del handshake: %d\n",total_enviado);
    					corte=0;
    					for(combo.n_bloque=0;combo.n_bloque<nodo_temporal->bloques_totales;combo.n_bloque++){
    						if (!bitarray_test_bit(nodo_temporal->bloques_del_nodo,combo.n_bloque)){
    							corte=1;
    							break;
    						}
    					}
    					if (corte==0){
    						printf ("El nodo %s no tiene bloques libles, se cancela la subida del archivo\n",nodo_temporal->nodo_id);
    						return -1;
    					}
    					printf ("voy a mandar al nodo %s la copia %d del bloque %d y la guardara en el bloque %d\n",nodo_temporal->nodo_id,indice+1,n_copia,combo.n_bloque);
    					if ((total_enviado=send(nodo_temporal->socket, &combo, sizeof(combo), 0)) == -1) {
    						perror("send buffer en subir archivo");
    						log_error(logger, "FALLO el envio del aviso de obtener bloque ");
    						exit(-1);
    					}
    					printf ("Quiero enviar %d y envie %d\n",sizeof(combo),total_enviado);
    					nodo_temporal->bloques_libres--;
    					bitarray_set_bit(nodo_temporal->bloques_del_nodo,combo.n_bloque);


    					//Agrego la copia del bloque a la lista de copias de este bloque particular
    					t_copias *copia_temporal=malloc(sizeof(t_copias));
    					copia_temporal->bloqueNodo=combo.n_bloque;
    					char *cadena_para_md5=malloc(20971520);
    					strcpy(cadena_para_md5,combo.buf_20mb);
    					memset(copia_temporal->md5,'\0',32);
    					strcpy(copia_temporal->md5,obtener_md5(cadena_para_md5));
    					copia_temporal->nodo=strdup(nodo_temporal->nodo_id);
    					list_add(bloque_temporal->copias,copia_temporal);
    					free(cadena_para_md5);
    				}
    			}
    			if (bandera!=3){
    				printf ("No hay suficientes nodos disponibles con espacio libre\n");
    				return -1;
    			}
    			pos = 0; //pos = 0;
    			cantBytes-=aux;
    			fseek(archivoLocal,cantBytes,SEEK_SET);
    			memset(combo.buf_20mb,'\0',BLOCK_SIZE);

    		}
    		list_add(archivo_temporal->bloques,bloque_temporal);
    	}
    	//FIN DEL WHILE
    	if (feof(archivoLocal))
    	{
    		//aca va el fin
    		//si leyo menos lo mando de una porque seguro temina en \n y esta relleno de 0
    		t_bloque *bloque_temporal=malloc(sizeof(t_bloque));
    		bloque_temporal->copias=list_create();
    		n_copia++;
    		list_sort(nodos_temporales, (void*)nodos_mas_libres);
    		//Copiar el contenido del Buffer en los nodos mas vacios por triplicado
    		bandera=0;
    		for (indice=0;indice<list_size(nodos_temporales);indice++){
    			if (bandera==3) break;
    			nodo_temporal=list_get(nodos_temporales,indice);
    			if (nodo_temporal->estado == 1 && nodo_temporal->bloques_libres > 0){
    				bandera++;
    				if ((total_enviado=send(nodo_temporal->socket, handshake, sizeof(handshake), MSG_WAITALL)) == -1) {
    					perror("send handshake en subir archivo");
    					log_error(logger, "FALLO el envio del aviso de obtener bloque ");
    					exit(-1);
    				}
    				printf ("Lo que mande del handshake: %d\n",total_enviado);
    				corte=0;
    				for(combo.n_bloque=0;combo.n_bloque<nodo_temporal->bloques_totales;combo.n_bloque++){
    					if (!bitarray_test_bit(nodo_temporal->bloques_del_nodo,combo.n_bloque)){
    						corte=1;
    						break;
    					}
    				}
    				if (corte==0){
    					printf ("El nodo %s no tiene bloques libles, se cancela la subida del archivo\n",nodo_temporal->nodo_id);
    					return -1;
    				}
    				printf ("voy a mandar al nodo %s la copia %d del bloque %d y la guardara en el bloque %d\n",nodo_temporal->nodo_id,indice+1,n_copia,combo.n_bloque);
    				if ((total_enviado=send(nodo_temporal->socket, &combo, sizeof(combo), 0)) == -1) {
    					perror("send buffer en subir archivo");
    					log_error(logger, "FALLO el envio del aviso de obtener bloque ");
    					exit(-1);
    				}
    				printf ("Esto es lo que quedo, Quiero enviar %d y envie %d\n",sizeof(combo),total_enviado);
    				nodo_temporal->bloques_libres--;
    				bitarray_set_bit(nodo_temporal->bloques_del_nodo,combo.n_bloque);

    				//Agrego la copia del bloque a la lista de copias de este bloque particular
					t_copias *copia_temporal=malloc(sizeof(t_copias));
					copia_temporal->bloqueNodo=combo.n_bloque;
					char *cadena_para_md5=malloc(20971520);
					strcpy(cadena_para_md5,combo.buf_20mb);
					memset(copia_temporal->md5,'\0',32);
					strcpy(copia_temporal->md5,obtener_md5(cadena_para_md5));
					copia_temporal->nodo=strdup(nodo_temporal->nodo_id);
					list_add(bloque_temporal->copias,copia_temporal);
					free(cadena_para_md5);
    			}
    		}
    		if (bandera!=3){
    			printf ("No hay suficientes nodos disponibles con espacio libre\n");
    			return -1;
    		}
    		list_add(archivo_temporal->bloques,bloque_temporal);
    	}
    	strcpy(ruta,pathMDFS);
    	char *nombre_del_archivo;
    	int aux1,aux2=0;
    	char *saveptr;
    	for (aux1=0;aux1<strlen(ruta);aux1++) if (ruta[aux1]=='/') aux2++;
    	nombre_del_archivo = strtok_r(ruta,"/",&saveptr);
    	for (aux1=0;aux1<aux2-1;aux1++) nombre_del_archivo = strtok_r(NULL,"/",&saveptr);
    	memset(archivo_temporal->nombre,'\0',100);
    	strcpy(archivo_temporal->nombre,nombre_del_archivo);
    	archivo_temporal->estado=1;
    	archivo_temporal->padre=idPadre; //modifico al path del archivo en el MDFS
    	archivo_temporal->tamanio=0; //para mi este campo esta al pedo
    	fclose(archivoLocal);

    	//Si llego hasta aca salio tod0 bien, actualizo la lista real de nodos
    	eliminar_listas(NULL,NULL,nodos);
    	nodos=list_create();
    	if (copiar_lista_de_nodos(nodos,nodos_temporales)){
    		printf ("No se pudo crear la copia de la lista de nodos\n");
    		return -1;
    	}
    	eliminar_listas(NULL,NULL,nodos_temporales);

    	//Si llego aca es porque tod0 salio bien y actualizo la lista de archivos
    	list_add(archivos_temporales,archivo_temporal);
    	eliminar_listas(archivos,NULL,NULL);
    	archivos=list_create();
    	if (copiar_lista_de_archivos(archivos,archivos_temporales)){
    		printf ("No se pudo crear la copia de la lista de archivos\n");
    		return -1;
    	}
    	eliminar_listas(archivos_temporales,NULL,NULL);
    	return 0;
}
int CopiarArchivoDelMDFS(int flag, char*unArchivo) {
	char pathArchivo[100];
	memset(pathArchivo,'\0',100);
	FILE* copiaLocal;
	t_archivo *archivo=malloc(sizeof(t_archivo));
	t_bloque *bloque=malloc(sizeof(t_bloque));
	t_copias *copia=malloc(sizeof(t_copias));
	int j,k;
	int bloqueDisponible;
	int socket_nodo;
	char* bloqueParaVer;
	char ruta[100];
	char *nombre_del_archivo=string_new();
	int aux1,aux2=0;
	char *saveptr;
	char ruta_local[100];
	char **directoriosPorSeparado;
	int posicionDirectorio=0;
	char *directorio=string_new();
	if (flag!=99){
		printf("Eligió Copiar un archivo del MDFS al filesystem local\n");
		printf ("Ingrese el archivo a copiar con su path completo, ej. /directorio/archivo.ext\n");
		scanf("%s",pathArchivo);
	}else strcpy(pathArchivo,unArchivo);
	strcpy(ruta,pathArchivo);
	for (aux1=0;aux1<strlen(ruta);aux1++) if (ruta[aux1]=='/') aux2++;
	nombre_del_archivo = strtok_r(ruta,"/",&saveptr);
	for (aux1=0;aux1<aux2-1;aux1++) nombre_del_archivo = strtok_r(NULL,"/",&saveptr);
	strcpy(ruta_local,"/tmp/");
	strcat(ruta_local,nombre_del_archivo);
	directoriosPorSeparado=string_split(pathArchivo,"/");
	while(directoriosPorSeparado[posicionDirectorio+1]!=NULL){
		string_append(&directorio,"/");
		string_append(&directorio,directoriosPorSeparado[posicionDirectorio]);
		posicionDirectorio++;
	}
	int idPadre = BuscarPadre(directorio);
	if (idPadre==-1){
		printf("El directorio no existe\n");
		Menu();
	}
	int posArchivo = BuscarArchivoPorNombre(pathArchivo, idPadre);
	if (posArchivo==-1){
		printf ("El archivo no existe\n");
		Menu();
	}
	archivo = list_get(archivos, posArchivo);
	copiaLocal = fopen(ruta_local, "w");
	for (j=0;j<list_size(archivo->bloques);j++){
		bloque=list_get(archivo->bloques,j);
		bloqueDisponible=0;
		for (k=0;k<list_size(bloque->copias);k++){
			copia=list_get(bloque->copias,k);
			if(obtenerEstadoDelNodo(copia->nodo)){
				bloqueDisponible=1;
				socket_nodo =obtener_socket_de_nodo_con_id(copia->nodo);
				if (socket_nodo == -1){
					log_error(logger, "El nodo ingresado no es valido o no esta disponible\n");
					printf("El nodo ingresado no es valido o no esta disponible\n");
					return -1;
				}
				enviarNumeroDeBloqueANodo(socket_nodo, copia->bloqueNodo);
				bloqueParaVer = recibirBloque(socket_nodo);
				fprintf(copiaLocal,"%s",bloqueParaVer);
				break;
			}
		}
		if (bloqueDisponible==0){
			printf ("El archivo no se puede recuperar, el bloque %d no esta disponible\n",j);
			return -1;
		}
	}
	fclose(copiaLocal);
	return 0;
}

int obtenerEstadoDelNodo(char* nodo){
	t_nodo *unNodo=malloc(sizeof(t_nodo));
	int i;
	for (i=0;i<list_size(nodos);i++){
		unNodo=list_get(nodos,i);
		if ((strcmp(unNodo->nodo_id,nodo)==0) && (unNodo->estado_red==1) && (unNodo->estado=1)) return 1;
	}
	return 0;
}

void MD5DeArchivo() {
	printf("Eligió Solicitar el MD5 de un archivo en MDFS\n");
	int fd[2];
	int childpid;
	pipe(fd);
	char result[1000];
	char *path=string_new();
	char *ruta=string_new();
	char *nombre_del_archivo=string_new();
	char *ruta_local=string_new();
	int aux1,aux2=0;
	char *saveptr;
	printf ("Ingrese el path del archivo en MDFS:\n");
	scanf ("%s",path);

	if(CopiarArchivoDelMDFS(99,path)==-1){
		printf ("El archivo seleccionado no esta disponible\n");
		Menu();
	}

	strcpy(ruta,path);
	for (aux1=0;aux1<strlen(ruta);aux1++) if (ruta[aux1]=='/') aux2++;
	nombre_del_archivo = strtok_r(ruta,"/",&saveptr);
	for (aux1=0;aux1<aux2-1;aux1++) nombre_del_archivo = strtok_r(NULL,"/",&saveptr);
	strcpy(ruta_local,"/tmp/");
	strcat(ruta_local,nombre_del_archivo);

	memset(result,'\0',1000);
	if ( (childpid = fork() ) == -1){
		fprintf(stderr, "FORK failed");
	} else if( childpid == 0) {
		close(1);
		dup2(fd[1], 1);
		close(fd[0]);
		execlp("/usr/bin/md5sum","md5sum",ruta_local,NULL);
	}
	wait(NULL);
	read(fd[0], result, sizeof(result));
	printf("%s",result);

}


void BorrarBloque() {
	//printf("Eligió Borrar un bloque que compone un archivo\n");
	int i, cantNodos, bloque, nodoEncontrado;
	nodoEncontrado = 0;
	t_nodo* nodoBuscado;
	cantNodos = list_size(nodos);
	//char* nodoId = malloc(1);
	char nodoId[6];
	printf("Ingrese el ID del nodo del que desea borrar un bloque:\n");
	scanf("%s", nodoId);
	printf("Ingrese el número de bloque que desea borrar:\n");
	scanf("%d", &bloque);
	i = 0;
	while (i < cantNodos && nodoEncontrado == 0) {
		nodoBuscado = list_get(nodos, i);
		if (strcmp(nodoBuscado->nodo_id, nodoId)==0) {
			nodoEncontrado = 1;
		}
		i++;
	}
	if (nodoEncontrado == 1){
		nodoBuscado->bloques_libres++;
		bitarray_clean_bit(nodoBuscado->bloques_del_nodo, bloque);
		printf("Se ha borrado el bloque correctamente\n");
	}
	else{
		printf("No se puede eliminar el bloque\n");
		}
}


void CopiarBloque() {
	char *nodo_origen=string_new();
	char *nodo_destino=string_new();
	int bloque_origen, bloque_destino;
	char handshake[15]="copiar_archivo";
	int i,j,k;
	char *bloqueParaVer=string_new();
	int origen_encontrado=0, destino_encontrado=0;
	int socket_nodo;
	printf("Eligió Copiar un bloque de un archivo\n");
	printf ("Ingrese el nodo de origen:\n");
	scanf ("%s",nodo_origen);
	printf ("Ingrese bloque de origen:\n");
	scanf("%d",&bloque_origen);
	printf ("Ingrese el nodo de destino:\n");
	scanf ("%s",nodo_destino);
	printf ("Ingrese bloque de destino:\n");
	scanf ("%d",&bloque_destino);

	t_nodo *origen = malloc(sizeof(t_nodo));
	t_nodo *destino = malloc(sizeof(t_nodo));

	if(!obtenerEstadoDelNodo(nodo_origen)){
		printf ("El nodo seleccionado como origen no esta disponible o no existe\n");
		Menu();
	}
	if(!obtenerEstadoDelNodo(nodo_destino)){
		printf ("El nodo seleccionado como destino no esta disponible o no existe\n");
		Menu();
	}
	for (i=0;i<list_size(nodos);i++){
		origen=list_get(nodos,i);
		if (strcmp(origen->nodo_id,nodo_origen)==0){
			if (origen->estado==1 && origen->estado_red==1) origen_encontrado=1;
			if (!bitarray_test_bit(origen->bloques_del_nodo,bloque_origen)){
				printf ("El bloque %d del nodo %s esta vacio, no hay nada que copiar\n",bloque_origen,nodo_origen);
				Menu();
			}
			break;
		}
	}

	for (i=0;i<list_size(nodos);i++){
		destino=list_get(nodos,i);
		if (strcmp(destino->nodo_id,nodo_destino)==0){
			if (destino->estado==1 && destino->estado_red==1) destino_encontrado=1;
			if (bitarray_test_bit(destino->bloques_del_nodo,bloque_destino)){
				printf ("El bloque %d del nodo %s esta ocupado, no se puede copiar\n",bloque_destino,nodo_destino);
				Menu();
			}
			break;
		}
	}
	if (origen_encontrado==0){
		printf ("El nodo origen no existe o no esta disponible\n");
		Menu();
	}
	if (destino_encontrado==0){
		printf ("El nodo destino no existe o no esta disponible\n");
		Menu();
	}
	//obtener primero el bloque del nodo original
	socket_nodo = obtener_socket_de_nodo_con_id(nodo_origen);
	if (socket_nodo == -1){
		log_error(logger, "El nodo ingresado no es valido o no esta disponible\n");
		printf("El nodo ingresado no es valido o no esta disponible\n");
		Menu();
	}
	enviarNumeroDeBloqueANodo(socket_nodo, bloque_origen);
	bloqueParaVer = recibirBloque(socket_nodo);
	memset(combo.buf_20mb,'\0',BLOCK_SIZE);
	strcpy(combo.buf_20mb,bloqueParaVer);

	//copiar el bloque obtenido en el nodo destino en el bloque especificado
	//luego actualizar la estructura de copias del bloque destino del nodo destino
	//no voy a usar copias de estructuras ya que previamente valido todos

	socket_nodo = obtener_socket_de_nodo_con_id(nodo_destino);
	if (send(socket_nodo, handshake, sizeof(handshake), MSG_WAITALL) == -1) {
		perror("send handshake en funcion copiar bloque");
		log_error(logger, "FALLO el envio del aviso de obtener bloque ");
		exit(-1);
	}
	printf ("voy a mandar al nodo %s una copia del bloque %d\n",nodo_destino,bloque_origen);
	combo.n_bloque=bloque_destino;
	if (send(socket_nodo, &combo, sizeof(combo), 0) == -1) {
		perror("send buffer en subir archivo");
		log_error(logger, "FALLO el envio del aviso de obtener bloque ");
		exit(-1);
	}
	bitarray_set_bit(destino->bloques_del_nodo,bloque_destino);
	destino->bloques_libres--;

	//Agrego la copia del bloque a la lista de copias de este bloque particular
	t_copias *copia_temporal=malloc(sizeof(t_copias));
	copia_temporal->bloqueNodo=bloque_destino;
	char *cadena_para_md5=malloc(20971520);
	strcpy(cadena_para_md5,combo.buf_20mb);
	strcpy(copia_temporal->md5,obtener_md5(cadena_para_md5));
	copia_temporal->nodo=strdup(nodo_destino);


	//Busco el bloque en el destino para agregar la nueva copia del bloque
	t_archivo *unArchivo=malloc(sizeof(t_archivo));
	t_bloque *unBloque=malloc(sizeof(t_bloque));
	t_copias *unaCopia=malloc(sizeof(t_copias));
	int bloque_encontrado=0;
	for (i=0;i<list_size(archivos);i++){
		unArchivo=list_get(archivos,i);
		for(j=0;j<list_size(unArchivo->bloques);j++){
			unBloque=list_get(unArchivo->bloques,j);
			for(k=0;k<list_size(unBloque->copias);k++){
				unaCopia=list_get(unBloque->copias,k);
				if (unaCopia->bloqueNodo==bloque_origen && strcmp(unaCopia->nodo,nodo_origen)==0){
					bloque_encontrado=1;
					break;
				}
			}
			if (bloque_encontrado==1){
				list_add(unBloque->copias,copia_temporal);
				break;
			}
		}
		if (bloque_encontrado==1) break;
	}
}

void AgregarNodo(){
	//printf("Eligió Agregar un nodo de datos\n");
	int i,cantNodos, nodoEncontrado;
	nodoEncontrado =0; //0 no lo encontró, 1 lo encontró
	t_nodo* nodoAEvaluar;
	char* nodoID = string_new();
	cantNodos= list_size(nodos);
	for (i=0;i<cantNodos;i++){
		nodoAEvaluar = list_get(nodos,i);
		if (nodoAEvaluar->estado_red == 1 && nodoAEvaluar->estado == 0){
			printf ("\n\n");
			printf ("Nodo_ID: %s\nSocket: %d\nIP: %s\nPuerto_Origen: %d\nPuerto_Escucha_Nodo: %d\nBloques_Libres: %d\nBloques_Totales: %d", nodoAEvaluar->nodo_id, nodoAEvaluar->socket,nodoAEvaluar->ip,nodoAEvaluar->puerto,nodoAEvaluar->puerto_escucha_nodo,nodoAEvaluar->bloques_libres,nodoAEvaluar->bloques_totales);
			printf ("\n");
		}
	}
	printf("Ingrese el ID del nodo que desea agregar:\n");
	scanf("%s", nodoID);
	i = 0;
	while (i < cantNodos && nodoEncontrado == 0){
		nodoAEvaluar = list_get(nodos,i);
		if (strcmp(nodoAEvaluar->nodo_id, nodoID)==0 && nodoAEvaluar->estado_red == 1 && nodoAEvaluar->estado == 0) {
			nodoEncontrado = 1;
		}
		i++;
	}
	if (nodoEncontrado == 1){
		modificar_estado_nodo(nodoAEvaluar->nodo_id, nodoAEvaluar->socket, nodoAEvaluar->puerto, 1, 99); //cambio su estado de la lista a 1 que es activo, invoco con 99 para solo cambiar estado
		printf("Se ha agregado el nodo %s correctamente\n",nodoID);
	}
	else{
		printf("El nodo ingresado no se puede agregar\n");
	}
}

void EliminarNodo(){
	//printf("Eligió Eliminar un nodo de datos\n");
	int i,cantNodos, nodoEncontrado;
	nodoEncontrado =0; //0 no lo encontró, 1 lo encontró
	t_nodo* nodoAEvaluar;
	char* nodoID = string_new();
	cantNodos= list_size(nodos);
	for (i=0;i<cantNodos;i++){
		nodoAEvaluar = list_get(nodos,i);
		if (nodoAEvaluar->estado_red == 1 && nodoAEvaluar->estado == 1){
			printf ("\n\n");
			printf ("Nodo_ID: %s\nSocket: %d\nIP: %s\nPuerto_Origen: %d\nPuerto_Escucha_Nodo: %d\nBloques_Libres: %d\nBloques_Totales: %d", nodoAEvaluar->nodo_id, nodoAEvaluar->socket,nodoAEvaluar->ip,nodoAEvaluar->puerto,nodoAEvaluar->puerto_escucha_nodo,nodoAEvaluar->bloques_libres,nodoAEvaluar->bloques_totales);
			printf ("\n");
		}
	}
	printf("Ingrese el ID del nodo que desea eliminar:\n");
	scanf("%s", nodoID);
	i = 0;
	while (i < cantNodos && nodoEncontrado == 0){
		nodoAEvaluar = list_get(nodos,i);
		if (strcmp(nodoAEvaluar->nodo_id, nodoID)==0 && nodoAEvaluar->estado_red == 1 && nodoAEvaluar->estado == 1) {
		nodoEncontrado = 1;
	}
		i++;
	}
	if (nodoEncontrado == 1){
		modificar_estado_nodo(nodoAEvaluar->nodo_id, nodoAEvaluar->socket, nodoAEvaluar->puerto, 0, 99); //cambio su estado de la lista a 0 que es inactivo, invoco con 99 para solo cambiar estado
		printf("Se ha eliminado el nodo %s correctamente\n",nodoID);
	}
	else{
		printf("El nodo ingresado no se puede eliminar\n");
	}
}

int obtener_socket_de_nodo_con_id(char *id) {
	int i, cantidad_nodos;
	t_nodo *elemento;
	cantidad_nodos = list_size(nodos);
	for (i = 0; i < cantidad_nodos; i++) {
		elemento = list_get(nodos, i);
		if (strcmp(elemento->nodo_id, id) == 0 && elemento->estado == 1	&& elemento->estado_red == 1)
			return elemento->socket;
	}
	return -1;
}

int VerBloque() {
	FILE* archivoParaVerPath;
	char * bloqueParaVer;
	int nroBloque, i, cantNodos;
	int socket_nodo;
	t_nodo *nodoAEvaluar;
	bloqueParaVer = malloc(BLOCK_SIZE);
	printf("Ingrese id de Nodo: ");
	scanf("%s", nodo_id);
	printf("Numero de Bloque que desea ver: ");
	scanf("%d", &nroBloque);
	cantNodos= list_size(nodos);
	for (i=0;i<cantNodos;i++){
		nodoAEvaluar = list_get(nodos,i);
		if(strcmp(nodoAEvaluar->nodo_id,nodo_id) && nodoAEvaluar->estado_red == 1 && nodoAEvaluar->estado == 1){
			if(bitarray_test_bit(nodoAEvaluar->bloques_del_nodo, nroBloque)== 0){
				log_error(logger,"Esta queriendo ver un bloque vacio");
				printf("Esta queriendo ver un bloque vacio");
				return -1;
			}

			socket_nodo =obtener_socket_de_nodo_con_id(nodo_id);
			if (socket_nodo == -1){
				log_error(logger, "El nodo ingresado no es valido o no esta disponible\n");
				printf("El nodo ingresado no es valido o no esta disponible\n");
				Menu();
			}
			enviarNumeroDeBloqueANodo(socket_nodo, nroBloque);
			bloqueParaVer = recibirBloque(socket_nodo);
			archivoParaVerPath = fopen("./archBloqueParaVer.txt", "a+");
			fprintf(archivoParaVerPath, "%s", bloqueParaVer);
			printf ("El md5 del bloque que traje es: %s\n",obtener_md5(bloqueParaVer));
			printf("El bloque se copio en el archivo: ./archBloqueParaVer.txt\n");
			fclose(archivoParaVerPath);
			break;
		}
	}
	return 0;
}

void enviarNumeroDeBloqueANodo( int socket_nodo, int bloque) {
	strcpy(identificacion, "obtener bloque");
	if (send(socket_nodo, identificacion, sizeof(identificacion), MSG_WAITALL) == -1) {
		perror("send");
		log_error(logger, "FALLO el envio del aviso de obtener bloque ");
		exit(-1);
	}
	if (send(socket_nodo, &bloque, sizeof(int), MSG_WAITALL) == -1) {
		perror("send");
		log_error(logger, "FALLO el envio del numero de bloque");
		exit(-1);

	}
}
char *recibirBloque( socket_nodo) {
	char* bloqueAObtener;
		bloqueAObtener = malloc(BLOCK_SIZE);
		if (recv(socket_nodo, bloqueAObtener, BLOCK_SIZE, MSG_WAITALL) == -1) {
			perror("recv");
			log_error(logger, "FALLO el Recv del bloque por parte del nodo");
			exit(-1);
		}
	return bloqueAObtener;
}
