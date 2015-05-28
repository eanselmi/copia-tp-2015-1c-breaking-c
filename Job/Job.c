#include <stdio.h>
#include <commons/log.h>
#include <commons/config.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include "Job.h"

#define BUF_SIZE 50
#define BUF_ARCH 4096

//Declaración de funciones
void* hilo_mapper(t_mapper*);

//Declaración de variables
t_config* configurador;
t_log* logger;


int main(void){
	configurador= config_create("resources/jobConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
	logger = log_create("./jobLog.log", "Job", true, LOG_LEVEL_INFO); //se crea la instancia de log, que tambien imprimira en pantalla
	//Variables locales a main
	pthread_t mapperThread;
	pthread_t reduceThread;
	int marta_sock; //socket de conexión a MaRTA
	struct sockaddr_in marta_addr;
	char** archivosDelJob;
	char mensajeCombiner[3]; // Mensaje que se enviará a MaRTA con el atributo COMBINER
	int contMensajeArch; //contador para recorrer el array de archivos a los que se aplica el Job
	char mensajeArchivos[BUF_ARCH]; //cadena de caracteres que enviara a MaRTA los archivos a donde se aplica el Job. Formato: ",archivo1,archivo2,archivo3,...,archivo_n"

	/* Se conecta a MaRTA */

	if((marta_sock=socket(AF_INET,SOCK_STREAM,0))==-1){ //si función socket devuelve -1 es error
	       perror("socket");
	       log_error(logger,"Fallo la creación del socket");
	       exit(1);
	}

	marta_addr.sin_family=AF_INET;
	marta_addr.sin_port=htons(config_get_int_value(configurador,"PUERTO_MARTA"));
	marta_addr.sin_addr.s_addr=inet_addr(config_get_string_value(configurador,"IP_MARTA"));
	memset(&(marta_addr.sin_zero),'\0',8);

	if((connect(marta_sock,(struct sockaddr *)&marta_addr,sizeof(struct sockaddr)))==-1){
		perror("connect");
		log_error(logger,"Fallo la conexión con MaRTA");
		exit(1);
	}
	/*Conexión con MaRTA establecida*/

	log_info(logger,"Se conectó a MaRTA. IP: %s, Puerto: %d",config_get_string_value(configurador,"IP_MARTA"),config_get_int_value(configurador,"PUERTO_MARTA")); //se agrega al log en modo de informacion la conexión con MaRTA

	/*Creo un char[] que tenga los nombres de los archivos a trabajar separados con "," (una "," tambien al principio)
	 * De esta forma, del lado de marta voy a recibir el mensaje todo seguido y lo voy a separar con un string_split (commons)
	*/

	archivosDelJob=config_get_array_value(configurador,"ARCHIVOS"); //devuelve un array con todos los archivos, y ultimo un NULL
	for(contMensajeArch=0;archivosDelJob[contMensajeArch]!=NULL;contMensajeArch++){
		strcat(mensajeArchivos,",");
		strcat(mensajeArchivos,archivosDelJob[contMensajeArch]);
	}

	if (send(marta_sock,mensajeArchivos,sizeof(mensajeArchivos),0)==-1){
		perror("send");
		log_error(logger,"Falló el envío a MaRTA de la lista de archivos");
		exit(-1);
	}

	/*
	 * Envío a MaRTA si el Job acepta combiner o no
	*/

	strcat(mensajeCombiner,config_get_string_value(configurador,"COMBINER"));

	if (send(marta_sock,mensajeCombiner,sizeof(mensajeCombiner),0)==-1){
		perror("send");
		log_error(logger,"Falló el envío del atributo COMBINER");
		exit(-1);
	}

	t_mapper* datosMapper;
	datosMapper=malloc(sizeof(t_mapper));
	strcpy(datosMapper->ip_nodo,"127.0.0.1");
	datosMapper->puerto_nodo=6500;
	datosMapper->bloque=3;
	strcpy(datosMapper->nombreArchivoTemporal,"/tmp/map3tmp.txt");


	if(pthread_create(&mapperThread,NULL,hilo_mapper,datosMapper)!=0){
		perror("pthread_create");
		log_error(logger,"Fallo la creación del hilo rutina mapper");
		return 1;
	}

	pthread_join(mapperThread,NULL);
	log_destroy(logger); //se elimina la instancia de log
	config_destroy(configurador);
	return 0;
}


void* hilo_mapper(t_mapper* mapperStruct){
	printf("Se conectara al nodo con ip: %s\n",mapperStruct->ip_nodo);
	printf("En el puerto %d\n",mapperStruct->puerto_nodo);
	printf("Ejecutará la rutina mapper en el bloque %d\n",mapperStruct->bloque);
	printf("Guardará el resultado en el archivo %s\n",mapperStruct->nombreArchivoTemporal);
	//comienzo de conexion con nodo
	struct sockaddr_in nodo_addr;
	int nodo_sock;
	char identificacion[BUF_SIZE];

	if((nodo_sock=socket(AF_INET,SOCK_STREAM,0))==-1){ //si función socket devuelve -1 es error
		perror("socket");
		log_error(logger,"Fallo la creación del socket (conexión mapper-nodo)");
		exit(1);
	}

	nodo_addr.sin_family=AF_INET;
	nodo_addr.sin_port=htons(mapperStruct->puerto_nodo);
	nodo_addr.sin_addr.s_addr=inet_addr(mapperStruct->ip_nodo);
	memset(&(nodo_addr.sin_zero),'\0',8);

	if((connect(nodo_sock,(struct sockaddr *)&nodo_addr,sizeof(struct sockaddr)))==-1){
		perror("connect");
		log_error(logger,"Fallo la conexión con MaRTA");
		exit(1);
	}
	strcpy(identificacion,"soy mapper");
	if(send(nodo_sock,identificacion,sizeof(identificacion),0)==-1){
		perror("send");
		log_error(logger,"Fallo el envío de identificación mapper-nodo");
	}
	/*Conexión mapper-nodo establecida*/
	log_info(logger,"Hilo mapper conectado al Nodo con IP: %s,en el Puerto: %d",mapperStruct->ip_nodo,mapperStruct->puerto_nodo);

}

