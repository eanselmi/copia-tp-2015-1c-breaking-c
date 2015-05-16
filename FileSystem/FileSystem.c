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
#define MENSAJE_SIZE 4096

//Declaración de Funciones
int Menu();
void DibujarMenu();
void *connection_handler_escucha(); // Esta funcion escucha continuamente si recibo nuevos mensajes
static t_nodo *agregar_nodo_a_lista(int socket,char *ip,int est,int bloques_lib);
void FormatearFilesystem ();
void EliminarArchivo();
void RenombrarArchivo ();
void MoverArchivo();
void CrearDirectorio();
void EliminarDirectorio();
void RenombrarDirectorio();
void MoverDirectorio();
void CopiarArchivoAMDFS();
void CopiarArchivoDelMDFS();
void MD5DeArchivo();
void VerBloques();
void BorrarBloques();
void CopiarBloques();
void AgregarNodo();
void EliminarNodo();




fd_set master; // conjunto maestro de descriptores de fichero
fd_set read_fds; // conjunto temporal de descriptores de fichero para select()
t_log* logger;
t_list *nodos; //lista de nodos conectados al fs
int fdmax; // número máximo de descriptores de fichero
int listener; // descriptor de socket a la escucha
int marta_sock; //Socket exclusivo para marta
struct sockaddr_in filesystem; // dirección del servidor
struct sockaddr_in remote_client; // dirección del cliente
char identificacion[BUF_SIZE]; // buffer para datos del cliente
char mensaje[MENSAJE_SIZE];


int main(int argc , char *argv[]){
	t_config * configurador;
	pthread_t escucha; //Hilo que va a manejar los mensajes recibidos
	t_list* archivos; //lista de archivos del FS
	int newfd;
	int addrlen;
	int yes=1; // para setsockopt() SO_REUSEADDR, más abajo
	int read_size;
	int nodos_iniciales=0;
	configurador= config_create("resources/fsConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);

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
	addrlen = sizeof(struct sockaddr_in);
	nodos=list_create(); //Crea la lista de que va a manejar la lista de nodos
	printf ("Esperando las conexiones de los nodos iniciales\n");
	while (nodos_iniciales != config_get_int_value(configurador,"CANTIDAD_NODOS")){
		if ((newfd = accept(listener, (struct sockaddr*)&remote_client, (socklen_t*)&addrlen)) == -1) {
			perror ("accept");
			log_info(logger,"FALLO el ACCEPT");
		   	exit (-1);
		}
		if ((read_size = recv(newfd, identificacion , 50 , 0))==-1) {
			perror ("recv");
			log_info(logger,"FALLO el RECV");
			exit (-1);
		}
		if (read_size > 0 && strncmp(identificacion,"nuevo",5)==0){
			nodos_iniciales++;
			FD_SET(newfd, &master);
			fdmax = newfd;
			list_add (nodos, agregar_nodo_a_lista(newfd,inet_ntoa(remote_client.sin_addr),1,50));
			printf ("Se conecto el nodo %s\n",inet_ntoa(remote_client.sin_addr));
		}
		else{
			printf ("Marta se quiso conectar antes de tiempo\n");
			close(newfd);
		}
	}
	printf ("Nodos conectados %d\n",list_size(nodos));
	printf ("Informacion del nodo 2\n");
	printf ("Socket %d, ip %s, Estado %d, Nodos Libres %d\n",((t_nodo*)list_get(nodos,1))->socket,((t_nodo*)list_get(nodos,1))->ip,((t_nodo*)list_get(nodos,1))->estado,((t_nodo*)list_get(nodos,1))->bloques_libres);
	sleep(5);
	//Cuando sale de este ciclo el proceso FileSystem ya se encuentra en condiciones de iniciar sus tareas

	//Este hilo va a manejar las conexiones de forma paralela a la ejecucion del proceso
	if( pthread_create( &escucha , NULL ,  connection_handler_escucha , NULL) < 0){
	       perror("could not create thread");
	       return 1;
	}

	//................................................................................

	archivos=list_create(); //Crea la lista de archivos

	/*Desarrollo de un ejemplo para la estructura del fs*/
	t_archivo* archivoDeEjemplo;
	archivoDeEjemplo=malloc(sizeof(t_archivo));
	strcpy(archivoDeEjemplo->nombre,"ArchEjemplo14032015.txt");
	archivoDeEjemplo->padre=0; //sería un archivo en la raíz ("/")
	archivoDeEjemplo->tamanio=41943040; //40 MB --> 2 Bloques
	archivoDeEjemplo->bloques=list_create();
	archivoDeEjemplo->estado=1; //asumiendo que estado 1 sería disponible
	//Creo el bloqueUno y asigno el nodo y bloque del nodo de cada copia
	t_bloque* bloqueUno;
	bloqueUno=malloc(sizeof(t_bloque));
	strcpy(bloqueUno->copias[0].nodo,"NodoA");
	bloqueUno->copias[0].bloqueNodo=30;
	strcpy(bloqueUno->copias[1].nodo,"NodoF");
	bloqueUno->copias[1].bloqueNodo=12;
	strcpy(bloqueUno->copias[2].nodo,"NodoU");
	bloqueUno->copias[2].bloqueNodo=20;
	//Creo el bloqueDos y asigno el nodo y bloque del nodo de cada copia
	t_bloque* bloqueDos;
	bloqueDos=malloc(sizeof(t_bloque));
	strcpy(bloqueDos->copias[0].nodo,"NodoC");
	bloqueDos->copias[0].bloqueNodo=3;
	strcpy(bloqueDos->copias[1].nodo,"NodoD");
	bloqueDos->copias[1].bloqueNodo=34;
	strcpy(bloqueDos->copias[2].nodo,"NodoA");
	bloqueDos->copias[2].bloqueNodo=50;

	list_add(archivoDeEjemplo->bloques,bloqueUno); //Mediante las commons, agrego el bloqueUno a la lista de bloques
	list_add(archivoDeEjemplo->bloques,bloqueDos); //Mediante las commons, agrego el bloqueUno a la lista de bloques
	list_add(archivos,archivoDeEjemplo); //Mediante las commons agrego a la lista de archivos del FS el archivoDeEjemplo

	printf("En la lista de archivos hay: %d archivos\n",list_size(archivos));
	t_archivo* primerArchivoDeLaListaDeArchivos	= list_get(archivos,0);
	printf("El nombre del primer archivo es: %s\n",primerArchivoDeLaListaDeArchivos->nombre);
	printf("En el %s hay: %d bloques\n",primerArchivoDeLaListaDeArchivos->nombre,list_size(primerArchivoDeLaListaDeArchivos->bloques));
	t_bloque* bloqueUnoDeArchivoUno=list_get(primerArchivoDeLaListaDeArchivos->bloques,0);
	printf("El primer bloque del %s, tiene su copia numero 2 en el %s bloque %d\n",primerArchivoDeLaListaDeArchivos->nombre,bloqueUnoDeArchivoUno->copias[1].nodo,bloqueUnoDeArchivoUno->copias[1].bloqueNodo);
	/*Fin del ejemplo de la estructura del FS*/

	Menu();
	log_destroy(logger);
	return 0;
}

//Consola Menu
void DibujarMenu(void){
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
	      case 17: printf("Eligió Salir\n"); break;
	      default: printf("Opción incorrecta. Por favor ingrese una opción del 1 al 17\n");break;
		}
	}
	return 0;
}
static t_nodo *agregar_nodo_a_lista(int socket,char *ip,int est,int bloques_lib){
	t_nodo *nodo_temporal = malloc (sizeof(t_nodo));
	nodo_temporal->socket = socket;
	nodo_temporal->ip = strdup(ip);
	nodo_temporal->estado = est;
	nodo_temporal->bloques_libres = bloques_lib;
	return nodo_temporal;
}

void *connection_handler_escucha(void){
	int i,nbytes,newfd,addrlen;
	while(1) {
		read_fds = master;
		if (select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1) {
			perror("select");
			log_info(logger,"FALLO el Select");
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
						log_info(logger,"FALLO el ACCEPT");
						exit(-1);
					}
					else //llego una nueva conexion, se acepto y ahora tengo que tratarla
					{
						FD_SET(newfd, &master); // añadir al conjunto maestro
						if (newfd > fdmax) { // actualizar el máximo
							fdmax = newfd;
						}
						if ((nbytes = recv(newfd, mensaje, sizeof(mensaje), 0)) <= 0) { //si entra aca es porque hubo un error, no considero desconexion porque es nuevo
							{
								perror("recv");
								log_info(logger,"FALLO el Recv");
								exit(-1);
							}
						} else {
							// el nuevo conectado me manda algo, se identifica como nodo o como marta
							// luego de que se identifique lo agregare a la lista de nodos si es nodo
							// si es marta solo lo acepto y guardo las variables necesarias para despues conectar a marta
						}
					}
					printf("select: conexion desde %s en socket %d\n", inet_ntoa(remote_client.sin_addr),newfd);


				//.................................................
				//hasta aca, es el tratamiento de conexiones nuevas
				//.................................................

				} else { //si entra aca no es un cliente nuevo, es uno que ya tenia y me esta mandando algo
					// gestionar datos de un cliente
					if ((nbytes = recv(i, mensaje, sizeof(mensaje), 0)) <= 0) { //si entra aca es porque se desconecto o hubo un error
						if (nbytes == 0) {
							// conexión cerrada, ver quien fue y si fue un nodo, anular al nodo de la lista
							//............................
							//............................

						} else {
							perror("recv");
							log_info(logger,"FALLO el Recv");
							exit(-1);
						}
						close(i); // ¡Hasta luego!
						FD_CLR(i, &master); // eliminar del conjunto maestro
					} else {
						// tenemos datos de algún cliente
						// ...... Tratamiento del mensaje nuevo
					}
				}
			}
		}
	}
}

//int BuscarArchivo (char nombreArchivo [FILENAME]){
//	t_archivo* ListaArchivos;
//	int pos = 0;
//	while (ListaArchivos->nombre != nombreArchivo){
//		ListaArchivos->siguiente;
//		pos++;
//		if (ListaArchivos->siguiente == NULL){
//			printf("No se encontró el archivo \n");
//			break;
//		}
//	}
//	return pos;
//}

void EliminarArchivo(){
	printf("Eligió  Eliminar archivo\n");
//	char nombreArchivo [FILENAME];
//	printf ("Ingrese el nombre del archivo \n");
//	scanf ("%s", nombreArchivo);
//	int buscar = BuscarArchivo (nombreArchivo);
//	void list_remove_and_destroy_element(t_list *, buscar, void(*element_destroyer)(void*));
//	return 0;
}
void FormatearFilesystem (){
	printf("Eligió  Formatear el MDFS\n");
}

void RenombrarArchivo (){
	printf("Eligió Renombrar archivos\n");
}

void MoverArchivo(){
	 printf("Eligió Mover archivos\n");
}

void CrearDirectorio(){
	printf("Eligió Crear directorios\n");
}

void EliminarDirectorio(){
	printf("Eligió Eliminar directorios\n");
}

void RenombrarDirectorio(){
	printf("Eligió Renombrar directorios\n");
}

void MoverDirectorio(){
	printf("Eligió Mover directorios\n");
}

void CopiarArchivoAMDFS(){
	printf("Eligió Copiar un archivo local al MDFS\n");
}

void CopiarArchivoDelMDFS(){
	printf("Eligió Copiar un archivo del MDFS al filesystem local\n");
}

void MD5DeArchivo(){
	printf("Eligió Solicitar el MD5 de un archivo en MDFS\n");
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
