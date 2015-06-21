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
	char handshake[BUF_SIZE];
	int contMensajeArch; //contador para recorrer el array de archivos a los que se aplica el Job
	char mensajeArchivos[MENSAJE_SIZE]; //cadena de caracteres que enviara a MaRTA los archivos a donde se aplica el Job. Formato: ",archivo1,archivo2,archivo3,...,archivo_n"
	t_mapper datosMapper; // Datos para lanzar un hilo Map
	sem_init(&obtenerRutinaMap,0,1);
	memset(handshake,'\0', BUF_SIZE);
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

	strcpy(handshake,"soy job");
	if(send(marta_sock,handshake, sizeof(handshake),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger, "Fallo el envío de handshake a marta");
		exit(-1);
	}

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
	pthread_t mapperThread4;
	pthread_t mapperThread5;
	pthread_t mapperThread6;
	pthread_t mapperThread7;
	pthread_t mapperThread8;
	pthread_t mapperThread9;
	pthread_t mapperThread10;
	pthread_t mapperThread11;
	pthread_t mapperThread12;
	pthread_t mapperThread13;
	pthread_t mapperThread14;
	pthread_t mapperThread15;

	t_mapper* punteroMapper;
	t_mapper* punteroMapper2;
	t_mapper* punteroMapper3;
	t_mapper* punteroMapper4;
	t_mapper* punteroMapper5;
	t_mapper* punteroMapper6;
	t_mapper* punteroMapper7;
	t_mapper* punteroMapper8;
	t_mapper* punteroMapper9;
	t_mapper* punteroMapper10;
	t_mapper* punteroMapper11;
	t_mapper* punteroMapper12;
	t_mapper* punteroMapper13;
	t_mapper* punteroMapper14;
	t_mapper* punteroMapper15;

	punteroMapper=malloc(sizeof(t_mapper));
	punteroMapper2=malloc(sizeof(t_mapper));
	punteroMapper3=malloc(sizeof(t_mapper));
	punteroMapper4=malloc(sizeof(t_mapper));
	punteroMapper5=malloc(sizeof(t_mapper));
	punteroMapper6=malloc(sizeof(t_mapper));
	punteroMapper7=malloc(sizeof(t_mapper));
	punteroMapper8=malloc(sizeof(t_mapper));
	punteroMapper9=malloc(sizeof(t_mapper));
	punteroMapper10=malloc(sizeof(t_mapper));
	punteroMapper11=malloc(sizeof(t_mapper));
	punteroMapper12=malloc(sizeof(t_mapper));
	punteroMapper13=malloc(sizeof(t_mapper));
	punteroMapper14=malloc(sizeof(t_mapper));
	punteroMapper15=malloc(sizeof(t_mapper));

	memset(punteroMapper->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper->ip_nodo,'\0',20);
	strcpy(punteroMapper->ip_nodo,datosMapper.ip_nodo);
	punteroMapper->bloque=datosMapper.bloque;
	punteroMapper->puerto_nodo=datosMapper.puerto_nodo;
	strcpy(punteroMapper->nombreArchivoTemporal,datosMapper.nombreArchivoTemporal);

//		t_mapper* punteroMapper;
//		punteroMapper=malloc(sizeof(t_mapper));
//
//		memset(punteroMapper->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
//		memset(punteroMapper->ip_nodo,'\0',20);
//		strcpy(punteroMapper->ip_nodo,"127.0.0.1");
//		punteroMapper->bloque=1;
//		punteroMapper->puerto_nodo=6500;
//		strcpy(punteroMapper->nombreArchivoTemporal,"/tmp/mapBloque1.txt");
	/* Mas Maps Falsos Para probar Job desde acá*/



	memset(punteroMapper2->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper2->ip_nodo,'\0',20);
	strcpy(punteroMapper2->ip_nodo,"127.0.0.1");
	punteroMapper2->bloque=0;
	punteroMapper2->puerto_nodo=6500;
	strcpy(punteroMapper2->nombreArchivoTemporal,"/tmp/mapBloque0.txt");

	memset(punteroMapper3->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper3->ip_nodo,'\0',20);
	strcpy(punteroMapper3->ip_nodo,"127.0.0.1");
	punteroMapper3->bloque=2;
	punteroMapper3->puerto_nodo=6500;
	strcpy(punteroMapper3->nombreArchivoTemporal,"/tmp/mapBloque2.txt");

	memset(punteroMapper4->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper4->ip_nodo,'\0',20);
	strcpy(punteroMapper4->ip_nodo,"127.0.0.1");
	punteroMapper4->bloque=3;
	punteroMapper4->puerto_nodo=6500;
	strcpy(punteroMapper4->nombreArchivoTemporal,"/tmp/mapBloque3.txt");

	memset(punteroMapper5->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper5->ip_nodo,'\0',20);
	strcpy(punteroMapper5->ip_nodo,"127.0.0.1");
	punteroMapper5->bloque=4;
	punteroMapper5->puerto_nodo=6510;
	strcpy(punteroMapper5->nombreArchivoTemporal,"/tmp/mapBloque4.txt");

	memset(punteroMapper6->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper6->ip_nodo,'\0',20);
	strcpy(punteroMapper6->ip_nodo,"127.0.0.1");
	punteroMapper6->bloque=5;
	punteroMapper6->puerto_nodo=6520;
	strcpy(punteroMapper6->nombreArchivoTemporal,"/tmp/mapBloque5.txt");

	memset(punteroMapper7->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper7->ip_nodo,'\0',20);
	strcpy(punteroMapper7->ip_nodo,"127.0.0.1");
	punteroMapper7->bloque=6;
	punteroMapper7->puerto_nodo=6500;
	strcpy(punteroMapper7->nombreArchivoTemporal,"/tmp/mapBloque6.txt");

	memset(punteroMapper8->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper8->ip_nodo,'\0',20);
	strcpy(punteroMapper8->ip_nodo,"127.0.0.1");
	punteroMapper8->bloque=7;
	punteroMapper8->puerto_nodo=6510;
	strcpy(punteroMapper8->nombreArchivoTemporal,"/tmp/mapBloque7.txt");

	memset(punteroMapper9->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper9->ip_nodo,'\0',20);
	strcpy(punteroMapper9->ip_nodo,"127.0.0.1");
	punteroMapper9->bloque=8;
	punteroMapper9->puerto_nodo=6520;
	strcpy(punteroMapper9->nombreArchivoTemporal,"/tmp/mapBloque8.txt");

	memset(punteroMapper10->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper10->ip_nodo,'\0',20);
	strcpy(punteroMapper10->ip_nodo,"127.0.0.1");
	punteroMapper10->bloque=9;
	punteroMapper10->puerto_nodo=6500;
	strcpy(punteroMapper10->nombreArchivoTemporal,"/tmp/mapBloque9.txt");

	memset(punteroMapper11->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper11->ip_nodo,'\0',20);
	strcpy(punteroMapper11->ip_nodo,"127.0.0.1");
	punteroMapper11->bloque=10;
	punteroMapper11->puerto_nodo=6510;
	strcpy(punteroMapper11->nombreArchivoTemporal,"/tmp/mapBloque10.txt");

	memset(punteroMapper12->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper12->ip_nodo,'\0',20);
	strcpy(punteroMapper12->ip_nodo,"127.0.0.1");
	punteroMapper12->bloque=11;
	punteroMapper12->puerto_nodo=6520;
	strcpy(punteroMapper12->nombreArchivoTemporal,"/tmp/mapBloque11.txt");

	memset(punteroMapper13->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper13->ip_nodo,'\0',20);
	strcpy(punteroMapper13->ip_nodo,"127.0.0.1");
	punteroMapper13->bloque=12;
	punteroMapper13->puerto_nodo=6500;
	strcpy(punteroMapper13->nombreArchivoTemporal,"/tmp/mapBloque12.txt");

	memset(punteroMapper14->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper14->ip_nodo,'\0',20);
	strcpy(punteroMapper14->ip_nodo,"127.0.0.1");
	punteroMapper14->bloque=13;
	punteroMapper14->puerto_nodo=6510;
	strcpy(punteroMapper14->nombreArchivoTemporal,"/tmp/mapBloque13.txt");

	memset(punteroMapper15->nombreArchivoTemporal,'\0',TAM_NOMFINAL);
	memset(punteroMapper15->ip_nodo,'\0',20);
	strcpy(punteroMapper15->ip_nodo,"127.0.0.1");
	punteroMapper15->bloque=14;
	punteroMapper15->puerto_nodo=6520;
	strcpy(punteroMapper15->nombreArchivoTemporal,"/tmp/mapBloque14.txt");


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


//	pthread_create(&mapperThread4,NULL,(void*)hilo_mapper,punteroMapper4);
//	pthread_create(&mapperThread5,NULL,(void*)hilo_mapper,punteroMapper5);
//	pthread_create(&mapperThread6,NULL,(void*)hilo_mapper,punteroMapper6);
//	pthread_create(&mapperThread7,NULL,(void*)hilo_mapper,punteroMapper7);
//	pthread_create(&mapperThread8,NULL,(void*)hilo_mapper,punteroMapper8);
//	pthread_create(&mapperThread9,NULL,(void*)hilo_mapper,punteroMapper9);
//	pthread_create(&mapperThread10,NULL,(void*)hilo_mapper,punteroMapper10);
//	pthread_create(&mapperThread11,NULL,(void*)hilo_mapper,punteroMapper11);
//	pthread_create(&mapperThread12,NULL,(void*)hilo_mapper,punteroMapper12);
//	pthread_create(&mapperThread13,NULL,(void*)hilo_mapper,punteroMapper13);
//	pthread_create(&mapperThread14,NULL,(void*)hilo_mapper,punteroMapper14);
//	pthread_create(&mapperThread15,NULL,(void*)hilo_mapper,punteroMapper15);

	pthread_t reduceThread;
	t_reduce* reduceDeMarta;
	reduceDeMarta=malloc(sizeof(t_reduce));
	memset(reduceDeMarta->ip_nodoPpal,'\0',20);
	memset(reduceDeMarta->nombreArchivoFinal,'\0',TAM_NOMFINAL);
	strcpy(reduceDeMarta->ip_nodoPpal,"127.0.0.1");
	reduceDeMarta->puerto_nodoPpal=6500;
	strcpy(reduceDeMarta->nombreArchivoFinal,"/tmp/reduceBloques12y3.txt");

	pthread_t reduceThread2;
	t_reduce* reduceDeMarta2;
	reduceDeMarta2=malloc(sizeof(t_reduce));
	memset(reduceDeMarta2->ip_nodoPpal,'\0',20);
	memset(reduceDeMarta2->nombreArchivoFinal,'\0',TAM_NOMFINAL);
	strcpy(reduceDeMarta2->ip_nodoPpal,"127.0.0.1");
	reduceDeMarta2->puerto_nodoPpal=6500;
	strcpy(reduceDeMarta2->nombreArchivoFinal,"/tmp/reduceFinal2.txt");


	pthread_join(mapperThread,NULL);
	pthread_join(mapperThread2,NULL); //map falso
	pthread_join(mapperThread3,NULL); //map falso
	pthread_join(mapperThread4,NULL); //map falso
	pthread_join(mapperThread5,NULL); //map falso
	pthread_join(mapperThread6,NULL); //map falso
	pthread_join(mapperThread7,NULL); //map falso
	pthread_join(mapperThread8,NULL); //map falso
	pthread_join(mapperThread9,NULL); //map falso
	pthread_join(mapperThread10,NULL); //map falso
	pthread_join(mapperThread11,NULL); //map falso
	pthread_join(mapperThread12,NULL); //map falso
	pthread_join(mapperThread13,NULL); //map falso
	pthread_join(mapperThread14,NULL); //map falso
	pthread_join(mapperThread15,NULL); //map falso

	printf("Terminaron los 3 map\n");

	//Recibira la orden "ejecuta reduce" de marta, luego tirará un hilo reduce

	if(pthread_create(&reduceThread,NULL,(void*)hilo_reduce,reduceDeMarta)!=0){
			perror("pthread_create");
			log_error(logger,"Fallo la creación del hilo rutina mapper");
			return 1;
	}


	if(pthread_create(&reduceThread2,NULL,(void*)hilo_reduce,reduceDeMarta2)!=0){
			perror("pthread_create");
			log_error(logger,"Fallo la creación del hilo rutina mapper");
			return 1;
	}

	pthread_join(reduceThread,NULL); //map falso
	pthread_join(reduceThread2,NULL); //map falso

	printf("Terminaron los reduce\n");


	log_destroy(logger); //se elimina la instancia de log
	config_destroy(configurador);
	return 0;
}

void* hilo_reduce(t_reduce* reduceStruct){
	printf("El reduce se va a conectar al nodo con ip:%s\n",reduceStruct->ip_nodoPpal);
	printf("En el puerto %d\n", reduceStruct->puerto_nodoPpal);	struct sockaddr_in nodo_addr;
	int nodo_sock;
	int resultado;
	char identificacion[BUF_SIZE];
	char rutinaReduce[REDUCE_SIZE];
	char * contReduce;
	memset(identificacion,'\0',BUF_SIZE);
	memset(rutinaReduce,'\0',REDUCE_SIZE);
	int indice;

// ACA MARTA ENVÍA LA CANTIDAD DE ARCHIVOS QUE DEBE ESPERAR
// ACA RECIBIRIA UNO POR UNO "N" t_archivosReduce DE MARTA
		int cantArchivos = 3; //lo envia marta en realidad
		t_archivosReduce archivos[cantArchivos];
		for(indice=0;indice<cantArchivos;indice++){
			memset(archivos[indice].ip_nodo,'\0',20);
			memset(archivos[indice].archivoAAplicarReduce,'\0',TAM_NOMFINAL);
		}
		strcpy(archivos[0].ip_nodo,"127.0.0.1");
		archivos[0].puerto_nodo=6500;
		strcpy(archivos[0].archivoAAplicarReduce,"/tmp/mapBloque0.txt");
		strcpy(archivos[1].ip_nodo,"127.0.0.1");
		archivos[1].puerto_nodo=6500;
		strcpy(archivos[1].archivoAAplicarReduce,"/tmp/mapBloque1.txt");
		strcpy(archivos[2].ip_nodo,"127.0.0.1");
		archivos[2].puerto_nodo=6500;
		strcpy(archivos[2].archivoAAplicarReduce,"/tmp/mapBloque2.txt");

	printf("Se aplicará reduce en los archivos:\n");
	for(indice=0;indice<cantArchivos;indice++){
		printf("\tIP Nodo: %s\n",archivos[indice].ip_nodo);
		printf("\tEn el puerto: %d\n", archivos[indice].puerto_nodo);
		printf("\tArchivo: %s\n", archivos[indice].archivoAAplicarReduce);
	}

	if((nodo_sock=socket(AF_INET,SOCK_STREAM,0))==-1){ //si función socket devuelve -1 es error
		perror("socket");
		log_error(logger,"Fallo la creación del socket (conexión mapper-nodo)");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envío a marta el resultado
		pthread_exit((void*)0);
	}

	nodo_addr.sin_family=AF_INET;
	nodo_addr.sin_port=htons(reduceStruct->puerto_nodoPpal);
	nodo_addr.sin_addr.s_addr=inet_addr(reduceStruct->ip_nodoPpal);
	memset(&(nodo_addr.sin_zero),'\0',8);

	if((connect(nodo_sock,(struct sockaddr *)&nodo_addr,sizeof(struct sockaddr)))==-1){
		perror("connect");
		log_error(logger,"Fallo la conexión con el nodo");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envío a marta el resultado
		pthread_exit((void*)0);
	}

	strcpy(identificacion,"soy reducer");
	if(send(nodo_sock,identificacion,sizeof(identificacion),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envío de identificación mapper-nodo");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envío a marta el resultado
		pthread_exit((void*)0);
	}
	/*Conexión reduce-nodo establecida*/
	log_info(logger,"Hilo reduce conectado al Nodo con IP: %s,en el Puerto: %d",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal);

	//Envio el nombre donde el nodo deberá guardar el resultado
	if(send(nodo_sock,&reduceStruct->nombreArchivoFinal,TAM_NOMFINAL,MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envio del nombre del archivo del resultado del reduce al nodo");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envío a marta el resultado
		pthread_exit((void*)0);
	}

	//envio el contenido de la rutina reduce al nodo
	contReduce=getFileContent(config_get_string_value(configurador,"REDUCE"));
	strcpy(rutinaReduce,contReduce);
	if(send(nodo_sock,rutinaReduce,sizeof(rutinaReduce),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envío de la rutina reduce al nodo");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envio a marta el resultado
		pthread_exit((void*)0);
	}

	//Envio la cantidad de archivos a aplicar reduce para que el nodo sepa cuantos esperar
	if(send(nodo_sock,&cantArchivos,sizeof(int),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envío de la cantidad de archivos a aplicar reduce al nodo");
		resultado=1;
		printf("Resultado:%d\n",resultado);
		//envio a marta el resultado
		pthread_exit((void*)0);
	}

	pthread_exit((void*)0);
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
