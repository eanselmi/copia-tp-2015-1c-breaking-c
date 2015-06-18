#include <stdio.h>
#include <commons/log.h>
#include <commons/config.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <commons/string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <semaphore.h>


#include "Job.h"

//Declaración de variables
t_config* configurador;
t_log* logger;
char bufGetArchivo[MAPPER_SIZE];
sem_t obtenerRutinaMap;


int main(void){
	configurador= config_create("resources/jobConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
	logger = log_create("./jobLog.log", "Job", true, LOG_LEVEL_INFO); //se crea la instancia de log, que tambien imprimira en pantalla
	//Variables locales a main
	pthread_t mapperThread;
	//pthread_t reduceThread;
	int marta_sock; //socket de conexión a MaRTA
	struct sockaddr_in marta_addr;
	char** archivosDelJob;
	int contMensajeArch; //contador para recorrer el array de archivos a los que se aplica el Job
	char mensajeArchivos[BUF_ARCH]; //cadena de caracteres que enviara a MaRTA los archivos a donde se aplica el Job. Formato: ",archivo1,archivo2,archivo3,...,archivo_n"
	t_mapper datosMapper; // Datos para lanzar un hilo Map
	sem_init(&obtenerRutinaMap,0,1);
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
	 * De esta forma, del lado de marta voy a recibir el mensaje tod o seguido y lo voy a separar con un string_split (commons)
	*/

	archivosDelJob=config_get_array_value(configurador,"ARCHIVOS"); //devuelve un array con todos los archivos, y ultimo un NULL
	for(contMensajeArch=0;archivosDelJob[contMensajeArch]!=NULL;contMensajeArch++){
		strcat(mensajeArchivos,",");
		strcat(mensajeArchivos,archivosDelJob[contMensajeArch]);
	}

	if (send(marta_sock,mensajeArchivos,sizeof(mensajeArchivos),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Falló el envío a MaRTA de la lista de archivos");
		exit(-1);
	}

	/*
	 * Envío a MaRTA si el Job acepta combiner o no
	*/

	if (send(marta_sock,config_get_string_value(configurador,"COMBINER"),3,MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Falló el envío del atributo COMBINER");
		exit(-1);
	}


	// VA a recibir los datos sobre donde lanzar hilos Map de Marta

	if(recv(marta_sock,&datosMapper,sizeof(t_mapper),MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo al recibir los datos para el mapper");
		exit(-1);
	}


	pthread_t mapperThread2;
	pthread_t mapperThread3;

	t_mapper* punteroMapper;
	t_mapper* punteroMapper2;
	t_mapper* punteroMapper3;

	punteroMapper=malloc(sizeof(t_mapper));
	punteroMapper2=malloc(sizeof(t_mapper));
	punteroMapper3=malloc(sizeof(t_mapper));

	memset(punteroMapper->nombreArchivoTemporal,'\0',50);
	memset(punteroMapper->ip_nodo,'\0',20);
	strcpy(punteroMapper->ip_nodo,datosMapper.ip_nodo);
	punteroMapper->bloque=datosMapper.bloque;
	punteroMapper->puerto_nodo=datosMapper.puerto_nodo;
	strcpy(punteroMapper->nombreArchivoTemporal,datosMapper.nombreArchivoTemporal);

//		t_mapper* punteroMapper;
//		punteroMapper=malloc(sizeof(t_mapper));
//
//		memset(punteroMapper->nombreArchivoTemporal,'\0',50);
//		memset(punteroMapper->ip_nodo,'\0',20);
//		strcpy(punteroMapper->ip_nodo,"127.0.0.1");
//		punteroMapper->bloque=1;
//		punteroMapper->puerto_nodo=6500;
//		strcpy(punteroMapper->nombreArchivoTemporal,"/tmp/mapBloque1.txt");
	/* Mas Maps Falsos Para probar Job desde acá*/



	memset(punteroMapper2->nombreArchivoTemporal,'\0',50);
	memset(punteroMapper2->ip_nodo,'\0',20);
	strcpy(punteroMapper2->ip_nodo,"127.0.0.1");
	punteroMapper2->bloque=0;
	punteroMapper2->puerto_nodo=6500;
	strcpy(punteroMapper2->nombreArchivoTemporal,"/tmp/mapBloque0.txt");

	memset(punteroMapper3->nombreArchivoTemporal,'\0',50);
	memset(punteroMapper3->ip_nodo,'\0',20);
	strcpy(punteroMapper3->ip_nodo,"127.0.0.1");
	punteroMapper3->bloque=2;
	punteroMapper3->puerto_nodo=6500;
	strcpy(punteroMapper3->nombreArchivoTemporal,"/tmp/mapBloque2.txt");

	/* Hasta Acá */

	if(pthread_create(&mapperThread,NULL,(void*)hilo_mapper,punteroMapper)!=0){
		perror("pthread_create");
		log_error(logger,"Fallo la creación del hilo rutina mapper");
		return 1;
	}
	//sleep(2); //descanso - Map Falso abajo
	if(pthread_create(&mapperThread2,NULL,(void*)hilo_mapper,punteroMapper2)!=0){
		perror("pthread_create");
		log_error(logger,"Fallo la creación del hilo rutina mapper");
		return 1;
	}
	//sleep(2); //descanso - Map falso abajo

	if(pthread_create(&mapperThread3,NULL,(void*)hilo_mapper,punteroMapper3)!=0){
			perror("pthread_create");
			log_error(logger,"Fallo la creación del hilo rutina mapper");
			return 1;
	}

	pthread_join(mapperThread,NULL);
	pthread_join(mapperThread2,NULL); //map falso
	pthread_join(mapperThread3,NULL); //map falso

	printf("Terminaron los 3 map\n");

	log_destroy(logger); //se elimina la instancia de log
	config_destroy(configurador);
	return 0;
}


void* hilo_mapper(t_mapper* mapperStruct){

	printf("Se conectara al nodo con ip: %s\n",(char*)mapperStruct->ip_nodo);
	printf("En el puerto %d\n", mapperStruct->puerto_nodo);
	printf("Ejecutará la rutina mapper en el bloque %d\n",mapperStruct->bloque);
	printf("Guardará el resultado en el archivo %s\n",mapperStruct->nombreArchivoTemporal);

	//comienzo de conexion con nodo
	struct sockaddr_in nodo_addr;
	int nodo_sock;
	int resultado;
	char identificacion[BUF_SIZE];
	t_datosMap datosParaNodo;
	memset(identificacion,'\0',BUF_SIZE);

	datosParaNodo.bloque=mapperStruct->bloque;
	strcpy(datosParaNodo.nomArchTemp,mapperStruct->nombreArchivoTemporal);
	strcpy(datosParaNodo.rutinaMap,getFileContent(config_get_string_value(configurador,"MAPPER")));

	if((nodo_sock=socket(AF_INET,SOCK_STREAM,0))==-1){ //si función socket devuelve -1 es error
		perror("socket");
		log_error(logger,"Fallo la creación del socket (conexión mapper-nodo)");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envío a marta el resultado
		pthread_exit((void*)0);
	}

	nodo_addr.sin_family=AF_INET;
	nodo_addr.sin_port=htons(mapperStruct->puerto_nodo);
	nodo_addr.sin_addr.s_addr=inet_addr(mapperStruct->ip_nodo);
	memset(&(nodo_addr.sin_zero),'\0',8);

	if((connect(nodo_sock,(struct sockaddr *)&nodo_addr,sizeof(struct sockaddr)))==-1){
		perror("connect");
		log_error(logger,"Fallo la conexión con el nodo");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envío a marta el resultado
		pthread_exit((void*)0);
	}

	strcpy(identificacion,"soy mapper");
	if(send(nodo_sock,identificacion,sizeof(identificacion),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envío de identificación mapper-nodo");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envío a marta el resultado
		pthread_exit((void*)0);
	}
	/*Conexión mapper-nodo establecida*/
	log_info(logger,"Hilo mapper conectado al Nodo con IP: %s,en el Puerto: %d",mapperStruct->ip_nodo,mapperStruct->puerto_nodo);

	//Envio al nodo de los datos del Map
	if(send(nodo_sock,&datosParaNodo,sizeof(t_datosMap),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envio de los datos del map hacia el Nodo");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envío a marta el resultado
		pthread_exit((void*)0);
	}

	if(recv(nodo_sock,&resultado,sizeof(int),MSG_WAITALL)==-1){
		perror("recv");
		log_error(logger,"Fallo el recibo del resultado de parte del Nodo");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envío a marta el resultado
		pthread_exit((void*)0);
	}

	printf("Resultado:%d\n",resultado);

	//Se envía el resultado de la operacion Map a Marta//

	//close(nodo_sock);
	pthread_exit((void*)0);

}

char* getFileContent(char* path){
	FILE * archivoLocal;
	int i=0;
	char car;
	memset(bufGetArchivo,'\0',MAPPER_SIZE);
	archivoLocal = fopen(path,"r");
	fseek(archivoLocal,0,SEEK_SET);
	while (!feof(archivoLocal)){
		car = (char) fgetc(archivoLocal);
		if(car!=EOF){
			bufGetArchivo[i]=car;
		}
		i++;
	}
	fclose(archivoLocal);
	return bufGetArchivo;
}
