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
t_log* logger_archivo;
char bufGetArchivo[MAPPER_SIZE];
sem_t obtenerRutinaMap;
int marta_sock; //socket de conexión a MaRTA
char rutinaMap[MAPPER_SIZE];

int main(void){
	configurador= config_create("resources/jobConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
	logger = log_create("./jobLog.log", "Job", true, LOG_LEVEL_INFO); //se crea la instancia de log, que tambien imprimira en pantalla
	logger_archivo=log_create("./jobLog.log","Job",false,LOG_LEVEL_INFO); //Logeará solo en el archivo
	//Variables locales a main
	pthread_t reduceThread;
	pthread_t mapperThread;
	int socketModificado;
	struct sockaddr_in marta_addr;
	fd_set read_fds; // conjunto temporal de descriptores de fichero para select()
	int fdmax;//Numero maximo de descriptores de fichero
	int nbytes;
	int finalizoJob=0;
	int cantArchivos=0; //cantidad de archivos para un reduce
	int indice;
	char** archivosDelJob;
	char handshake[BUF_SIZE];
	char accion[BUF_SIZE];
	char archivoResultado[200];
	int contMensajeArch; //contador para recorrer el array de archivos a los que se aplica el Job
	char mensajeArchivos[MENSAJE_SIZE]; //cadena de caracteres que enviara a MaRTA los archivos a donde se aplica el Job. Formato: ",archivo1,archivo2,archivo3,...,archivo_n"
	t_mapper datosMapper; // Datos para lanzar un hilo Map
	t_mapper* punteroMapper;
	t_reduce datosReduce;
	t_hiloReduce* hiloReduce;
	char * contMapper;
	memset(rutinaMap,'\0',MAPPER_SIZE);
	sem_init(&obtenerRutinaMap,0,1);
	memset(handshake,'\0', BUF_SIZE);
	FD_ZERO(&read_fds);
	memset(archivoResultado,'\0',200);

	contMapper=getFileContent(config_get_string_value(configurador,"MAPPER"));


	strcpy(rutinaMap,contMapper);

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

	FD_SET(marta_sock,&read_fds); //Agrego al conector con Marta al conjunto maestro
	fdmax=marta_sock; //Por ahora el FD más grande es éste

	strcpy(handshake,"soy job");
	if(send(marta_sock,handshake, sizeof(handshake),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger, "Fallo el envío de handshake a marta");
		exit(1);
	}

	/*
	 * Envío a MaRTA si el Job acepta combiner o no
	*/

	if (send(marta_sock,config_get_string_value(configurador,"COMBINER"),3,MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Falló el envío del atributo COMBINER");
		exit(1);
	}

	//Envio el nombre del archivo resultado

	strcpy(archivoResultado,config_get_string_value(configurador,"RESULTADO"));

	if(send(marta_sock,archivoResultado,200,MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Falló el envío del atributo COMBINER");
		exit(1);
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
		exit(1);
	}

	//Queda a la espera de instrucciones de marta
	printf("Job en la espera de solicitudes de maps o reduce de parte de Marta\n");

	while(finalizoJob!=1){
		if (select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1) {
				perror("select");
				log_error(logger,"FALLO el Select");
				exit(-1);
			}
		memset(accion,'\0',BUF_SIZE);
		for(socketModificado = 0; socketModificado <= fdmax; socketModificado++) {
			if (FD_ISSET(socketModificado, &read_fds)) {	// ¡¡tenemos datos!!
				if(socketModificado==marta_sock){
					if ((nbytes=recv(marta_sock,accion,sizeof(accion),MSG_WAITALL))==-1){ //da error
						perror("recv");
						log_error(logger,"Falló el receive");
						exit(-1);
					}
					if(nbytes==0){ //se desconectó
						close(marta_sock);
						FD_CLR(marta_sock,&read_fds);
						log_info(logger,"Se desconectó Marta. IP Marta: %s, Puerto Marta: %d",config_get_string_value(configurador,"IP_MARTA"),config_get_int_value(configurador,"PUERTO_MARTA"));
						exit(1);
					}
					else{
						/* -- marta envío un mensaje a tratar -- */

						//EJECUTAR MAP//

						if(strncmp(accion,"ejecuta map",11)==0){

							printf("Marta me dijo %s\n",accion);
							log_info(logger_archivo,"Marta me dijo %s",accion);


							//inicializo el puntero con los datos que envío al hilo
							punteroMapper=malloc(sizeof(t_mapper));
							memset(punteroMapper->archivoResultadoMap,'\0',TAM_NOMFINAL);
							memset(punteroMapper->ip_nodo,'\0',20);
							//inicializo los datosMapper que recibo de Marta
							memset(datosMapper.archivoResultadoMap,'\0',TAM_NOMFINAL);
							memset(datosMapper.ip_nodo,'\0',20);

							// VA a recibir los datos sobre donde lanzar hilos Map de Marta

							if(recv(marta_sock,&datosMapper,sizeof(t_mapper),MSG_WAITALL)==-1){
								perror("recv");
								log_error(logger,"Fallo al recibir los datos para el mapper");
								exit(-1);
							}

							//Copio los datos que me interesan de lo que envía marta al puntero que enviaré al hilo
							strcpy(punteroMapper->ip_nodo,datosMapper.ip_nodo);
							punteroMapper->bloque=datosMapper.bloque;
							punteroMapper->puerto_nodo=datosMapper.puerto_nodo;
							strcpy(punteroMapper->archivoResultadoMap,datosMapper.archivoResultadoMap);

							//Creo el hilo (que es detach) pasando el puntero como parametro
							if(pthread_create(&mapperThread,NULL,(void*)hilo_mapper,punteroMapper)!=0){
								perror("pthread_create");
								log_error(logger,"Fallo la creación del hilo rutina mapper");
							}

						}

						// EJECUTAR REDUCE //

						if(strncmp(accion,"ejecuta reduce",14)==0){
							//recibe un ejecuta reduce
							printf("Marta me dijo %s\n",accion);
							log_info(logger_archivo,"Marta me dijo %s",accion);


							//Inicializo el puntero que enviare con los datos al Hilo Reduce
							hiloReduce=malloc(sizeof(t_hiloReduce));
							memset(hiloReduce->ip_nodoPpal,'\0',20);
							memset(hiloReduce->nombreArchivoFinal,'\0',TAM_NOMFINAL);
							hiloReduce->listaNodos=list_create();

							//Inicializo la estructura que recibo de Marta con los datos principales del reduce
							memset(datosReduce.ip_nodoPpal,'\0',20);
							memset(datosReduce.nombreArchivoFinal,'\0',TAM_NOMFINAL);

							// VA a recibir los datos sobre donde lanzar hilos Reduce de Marta (IP y Puerto a conectarse y resultado
							// final

							if(recv(marta_sock,&datosReduce,sizeof(t_reduce),MSG_WAITALL)==-1){
								perror("recv");
								log_error(logger,"Fallo al recibir los datos para el reduce");
							}

							//Copio los datos que me interesan de lo que envía marta al puntero que enviaré al hilo
							strcpy(hiloReduce->ip_nodoPpal,datosReduce.ip_nodoPpal);
							strcpy(hiloReduce->nombreArchivoFinal,datosReduce.nombreArchivoFinal);
							hiloReduce->puerto_nodoPpal=datosReduce.puerto_nodoPpal;

							//Recibo de marta la cantidad de archivos a donde voy a tener que hacer reduce
							if(recv(marta_sock,&cantArchivos,sizeof(int),MSG_WAITALL)==-1){
								perror("recv");
								log_error(logger,"Fallo al recibir la cantidad de archivos a donde aplicara reduce");
							}

							//En un ciclo for, recibo uno por uno los archivos a donde voy a aplicar reduce
							//Los agrego a la lista del puntero que enviare al hilo Reduce (en listanodos)
							for(indice=0;indice<cantArchivos;indice++){
								t_archivosReduce* nuevoArch=malloc(sizeof(t_archivosReduce));
								t_archivosReduce archivoDesdeMarta;
								memset(nuevoArch->archivoAAplicarReduce,'\0',TAM_NOMFINAL);
								memset(nuevoArch->ip_nodo,'\0',20);
								memset(archivoDesdeMarta.archivoAAplicarReduce,'\0',TAM_NOMFINAL);
								memset(archivoDesdeMarta.ip_nodo,'\0',20);

								if(recv(marta_sock,&archivoDesdeMarta,sizeof(t_archivosReduce),MSG_WAITALL)==-1){
									perror("recv");
									log_error(logger,"Fallo al recibir uno de los archivos a aplicar reduce");
								}

								strcpy(nuevoArch->ip_nodo,archivoDesdeMarta.ip_nodo);
								nuevoArch->puerto_nodo=archivoDesdeMarta.puerto_nodo;
								strcpy(nuevoArch->archivoAAplicarReduce,archivoDesdeMarta.archivoAAplicarReduce);
								list_add(hiloReduce->listaNodos,nuevoArch);
							}

							//Creo el hilo (que es detach) pasando el puntero como parametro

							if(pthread_create(&reduceThread,NULL,(void*)hilo_reduce,hiloReduce)!=0){
								perror("pthread_create");
								log_error(logger,"Fallo la creación del hilo rutina mapper");
							}
						}

						//EL JOB NO SE PUEDE REALIZAR POR ARCHIVO NO DISPONIBLE//

						if(strncmp(accion,"arch no disp",12)==0){
							log_error(logger,"El archivo a donde se quiere aplicar el Job no está disponible");
							close(marta_sock);
							log_info(logger,"El job se desconectó de Marta. IP Marta: %s, Puerto Marta: %d",config_get_string_value(configurador,"IP_MARTA"),config_get_int_value(configurador,"PUERTO_MARTA"));
							log_destroy(logger); //se elimina la instancia de log
							log_destroy(logger_archivo);
							config_destroy(configurador);
							return 0;
						}

						//FINALIZAR//

						if(strncmp(accion,"finaliza",8)==0){
							log_info(logger_archivo,"Marta me dijo %s",accion);
							finalizoJob=1;
						}

						//ABORTAR//

						if(strncmp(accion,"aborta",6)==0){
							log_error(logger,"Marta pidió abortar el Job porque fallo un reduce");
							close(marta_sock);
							log_info(logger,"El job se desconectó de Marta. IP Marta: %s, Puerto Marta: %d",config_get_string_value(configurador,"IP_MARTA"),config_get_int_value(configurador,"PUERTO_MARTA"));
							log_destroy(logger); //se elimina la instancia de log
							log_destroy(logger_archivo);
							config_destroy(configurador);
							return 0;
						}
					}
				}
			}
		}
	}

	log_info(logger,"El job finalizó exitosamente\n");
	close(marta_sock);
	log_info(logger,"El job se desconectó de Marta. IP Marta: %s, Puerto Marta: %d",config_get_string_value(configurador,"IP_MARTA"),config_get_int_value(configurador,"PUERTO_MARTA"));
	log_destroy(logger); //se elimina la instancia de log
	log_destroy(logger_archivo);
	config_destroy(configurador);
	return 0;
}



void* hilo_reduce(t_hiloReduce* reduceStruct){
	pthread_detach(pthread_self());
	struct sockaddr_in nodo_addr;
	int nodo_sock;
	char identificacion[BUF_SIZE];
	char rutinaReduce[REDUCE_SIZE];
	char* contReduce;
	t_respuestaReduce respuestaParaMarta;
	t_respuestaNodoReduce respuestaNodo;
	memset(identificacion,'\0',BUF_SIZE);
	memset(rutinaReduce,'\0',REDUCE_SIZE);
	int ind;
	int cantidadArchivos=list_size(reduceStruct->listaNodos);


	log_info(logger,"Se creó un hilo con motivo ejecución de un REDUCE.\n\tParametros recibidos:\n\t\tIP del Nodo a conectarse: %s\n\t\tPuerto del Nodo: %d\n\t\tNombre del archivo resultado del reduce: %s",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal,reduceStruct->nombreArchivoFinal);
	log_info(logger,"Se aplicará reduce en los archivos:");

	memset(respuestaNodo.ip_nodoFallido,'\0',20);
	memset(respuestaParaMarta.archivoResultadoReduce,'\0',TAM_NOMFINAL);
	memset(respuestaParaMarta.ip_nodo,'\0',20);
	strcpy(respuestaParaMarta.archivoResultadoReduce,reduceStruct->nombreArchivoFinal);
	strcpy(respuestaParaMarta.ip_nodo,reduceStruct->ip_nodoPpal);
	respuestaParaMarta.puerto_nodo=reduceStruct->puerto_nodoPpal;

	for(ind=0;ind<cantidadArchivos;ind++){
		t_archivosReduce* archReduce;
		archReduce=list_get(reduceStruct->listaNodos,ind);
		log_info(logger,"\tIP Nodo: %s",archReduce->ip_nodo);
		log_info(logger,"\tPuerto Nodo: %d", archReduce->puerto_nodo);
		log_info(logger,"\tArchivo: %s", archReduce->archivoAAplicarReduce);
	}

	if((nodo_sock=socket(AF_INET,SOCK_STREAM,0))==-1){ //si función socket devuelve -1 es error
		perror("socket");
		log_error(logger,"Fallo la creación del socket (conexión mapper-nodo)");
		respuestaParaMarta.resultado=1;
		//envío a marta el resultado
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envío de la respuesta fallida a Marta");
		}
		log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
		pthread_exit((void*)0);
	}

	nodo_addr.sin_family=AF_INET;
	nodo_addr.sin_port=htons(reduceStruct->puerto_nodoPpal);
	nodo_addr.sin_addr.s_addr=inet_addr(reduceStruct->ip_nodoPpal);
	memset(&(nodo_addr.sin_zero),'\0',8);

	if((connect(nodo_sock,(struct sockaddr *)&nodo_addr,sizeof(struct sockaddr)))==-1){
		perror("connect");
		log_error(logger,"Fallo la conexión con el nodo");
		respuestaParaMarta.resultado=1;
		//envío a marta el resultado
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envío de la respuesta fallida a Marta");
		}
		printf("NO PUDE CONECTARME AL NODO PRINCIPAL %s %d\n",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal);
		log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
		pthread_exit((void*)0);
	}

	strcpy(identificacion,"soy reducer");

	if(send(nodo_sock,identificacion,sizeof(identificacion),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envío de identificación mapper-nodo");
		respuestaParaMarta.resultado=1;
		//envío a marta el resultado
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envío de la respuesta fallida a Marta");
		}
		printf("SE DESCONECTO EL NODO PRINCIPAL %s %d\n",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal);

		log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
		pthread_exit((void*)0);
	}
	/*Conexión reduce-nodo establecida*/
	log_info(logger_archivo,"Le dije al nodo %s puerto %d %s\n",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal,identificacion);
	log_info(logger,"Hilo reduce conectado al Nodo con IP: %s,en el Puerto: %d",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal);

	//Envio el nombre donde el nodo deberá guardar el resultado
	if(send(nodo_sock,&reduceStruct->nombreArchivoFinal,TAM_NOMFINAL,MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envio del nombre del archivo del resultado del reduce al nodo");
		respuestaParaMarta.resultado=1;
		//envío a marta el resultado
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envío de la respuesta fallida a Marta");
		}
		printf("SE DESCONECTO EL NODO PRINCIPAL %s %d\n",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal);

		log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
		pthread_exit((void*)0);
	}

	//envio el contenido de la rutina reduce al nodo
	contReduce=getFileContent(config_get_string_value(configurador,"REDUCE"));
	strcpy(rutinaReduce,contReduce);

	if(send(nodo_sock,rutinaReduce,sizeof(rutinaReduce),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envío de la rutina reduce al nodo");
		respuestaParaMarta.resultado=1;
		//envío a marta el resultado
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envío de la respuesta fallida a Marta");
		}
		printf("SE DESCONECTO EL NODO PRINCIPAL %s %d\n",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal);

		log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
		pthread_exit((void*)0);
	}

	//Envio la cantidad de archivos a aplicar reduce para que el nodo sepa cuantos esperar
	if(send(nodo_sock,&cantidadArchivos,sizeof(int),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envío de la cantidad de archivos a aplicar reduce al nodo");
		respuestaParaMarta.resultado=1;
		//envío a marta el resultado
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envío de la respuesta fallida a Marta");
		}
		printf("SE DESCONECTO EL NODO PRINCIPAL %s %d\n",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal);

		log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
		pthread_exit((void*)0);
	}

	for(ind=0;ind<cantidadArchivos;ind++){ //Envio los archivos al nodo
		t_archivosReduce* archReduce;
		archReduce=list_get(reduceStruct->listaNodos,ind);
		if(send(nodo_sock,archReduce,sizeof(t_archivosReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envío de los archivos a aplicar reduce al nodo");
			respuestaParaMarta.resultado=1;
			//envío a marta el resultado
			if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
				perror("send");
				log_error(logger,"Fallo el envío de la respuesta fallida a Marta");
			}
			printf("SE DESCONECTO EL NODO PRINCIPAL %s %d\n",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal);

			log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
			pthread_exit((void*)0);
		}
	}

	//Espero respuesta del nodo
	if(recv(nodo_sock,&respuestaNodo,sizeof(t_respuestaNodoReduce),MSG_WAITALL)<=0){
		perror("recv");
		log_info(logger,"El nodo con ip %s y puerto %d se desconectó",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal);
		log_error(logger,"Fallo al recibir la respuesta del nodo para un reduce");
		respuestaParaMarta.resultado=1;
		//envío a marta el resultado
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envío de la respuesta fallida a Marta");
		}
		printf("SE DESCONECTO EL NODO PRINCIPAL %s %d\n",reduceStruct->ip_nodoPpal,reduceStruct->puerto_nodoPpal);

		log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
		pthread_exit((void*)0);
	}


	respuestaParaMarta.resultado=respuestaNodo.resultado;

	//si respondio KO --> Mando a marta la respuesta KO y el nodo al que no se pudo conectar

	if(respuestaNodo.resultado==1){
		memset(respuestaParaMarta.ip_nodo,'\0',20);
		strcpy(respuestaParaMarta.ip_nodo,respuestaNodo.ip_nodoFallido);
		respuestaParaMarta.puerto_nodo=respuestaNodo.puerto_nodoFallido;
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envio de la respuesta KO de un reduce a marta");
			log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
			pthread_exit((void*)0);
		}
		printf("SE DESCONECTO UNO DE LOS NODOS DEL REDUCE %s %d\n",respuestaNodo.ip_nodoFallido,respuestaNodo.puerto_nodoFallido);

		log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
		pthread_exit((void*)0);
	}


	//si respondio OK --> Mando a marta la respuesta OK

	if(respuestaNodo.resultado==0){
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaReduce),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envio de la respuesta OK de un reduce a marta");
			log_error(logger,"Finalizó un hilo REDUCE.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",reduceStruct->nombreArchivoFinal);
			pthread_exit((void*)0);
		}
	}

	//Si llega hasta acá, el reduce termino OK

	log_info(logger,"Finalizó un hilo REDUCE.\n\tResultado: exitoso\n\tNombre archivo resultado: %s",reduceStruct->nombreArchivoFinal);

	pthread_exit((void*)0);
}


void* hilo_mapper(t_mapper* mapperStruct){
	//comienzo de conexion con nodo
	pthread_detach(pthread_self());
	struct sockaddr_in nodo_addr;
	int nodo_sock;
	int resultadoMap;
	char identificacion[BUF_SIZE];
	t_datosMap datosParaNodo;
	t_respuestaMap respuestaParaMarta;
	memset(identificacion,'\0',BUF_SIZE);

	memset(respuestaParaMarta.archivoResultadoMap,'\0',TAM_NOMFINAL);
	respuestaParaMarta.resultado=2;
	strcpy(respuestaParaMarta.archivoResultadoMap,mapperStruct->archivoResultadoMap);

	log_info(logger,"Se creó un hilo con motivo ejecución de un MAP.\n\tParametros recibidos:\n\t\tIP del Nodo a conectarse: %s.\n\t\tPuerto del Nodo: %d\n\t\tBloque a ejecutar el map: %d\n\t\tNombre del archivo resultado del map: %s",mapperStruct->ip_nodo,mapperStruct->puerto_nodo,mapperStruct->bloque,mapperStruct->archivoResultadoMap);


	datosParaNodo.bloque=mapperStruct->bloque;
	strcpy(datosParaNodo.nomArchTemp,mapperStruct->archivoResultadoMap);
	strcpy(datosParaNodo.rutinaMap,rutinaMap);

	if((nodo_sock=socket(AF_INET,SOCK_STREAM,0))==-1){ //si función socket devuelve -1 es error
		perror("socket");
		log_error(logger,"Fallo la creación del socket (conexión mapper-nodo)");
		respuestaParaMarta.resultado=1;
		//printf("Resultado:%d\n",respuestaParaMarta.resultado);
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaMap),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envio de la respuesta de un map a marta");
		}
		log_info(logger,"Finalizó un hilo MAP.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",mapperStruct->archivoResultadoMap);
		pthread_exit((void*)0);
	}

	nodo_addr.sin_family=AF_INET;
	nodo_addr.sin_port=htons(mapperStruct->puerto_nodo);
	nodo_addr.sin_addr.s_addr=inet_addr(mapperStruct->ip_nodo);
	memset(&(nodo_addr.sin_zero),'\0',8);

	if((connect(nodo_sock,(struct sockaddr *)&nodo_addr,sizeof(struct sockaddr)))==-1){
		perror("connect");
		log_error(logger,"Fallo la conexión con el nodo");
		respuestaParaMarta.resultado=1;
		//printf("Resultado:%d\n",respuestaParaMarta.resultado);
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaMap),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envio de la respuesta de un map a marta");
		}
		log_info(logger,"Finalizó un hilo MAP.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",mapperStruct->archivoResultadoMap);
		pthread_exit((void*)0);
	}

	strcpy(identificacion,"soy mapper");
	if(send(nodo_sock,identificacion,sizeof(identificacion),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envío de identificación mapper-nodo");
		respuestaParaMarta.resultado=1;
		//printf("Resultado:%d\n",respuestaParaMarta.resultado);
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaMap),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envio de la respuesta de un map a marta");
		}
		log_info(logger,"Finalizó un hilo MAP.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",mapperStruct->archivoResultadoMap);
		pthread_exit((void*)0);
	}
	/*Conexión mapper-nodo establecida*/
	log_info(logger_archivo,"Le dije al nodo con IP %s puerto %d %s",mapperStruct->ip_nodo,mapperStruct->puerto_nodo,identificacion);
	log_info(logger,"Hilo mapper conectado al Nodo con IP: %s,en el Puerto: %d",mapperStruct->ip_nodo,mapperStruct->puerto_nodo);

	//Envio al nodo de los datos del Map
	if(send(nodo_sock,&datosParaNodo,sizeof(t_datosMap),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envio de los datos del map hacia el Nodo");
		respuestaParaMarta.resultado=1;
		//printf("Resultado:%d\n",respuestaParaMarta.resultado);
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaMap),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envio de la respuesta de un map a marta");
		}
		log_info(logger,"Finalizó un hilo MAP.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",mapperStruct->archivoResultadoMap);
		pthread_exit((void*)0);
	}

	if(recv(nodo_sock,&resultadoMap,sizeof(int),MSG_WAITALL)<=0){
		perror("recv");
		log_error(logger,"Fallo el recibo del resultado de parte del Nodo");
		respuestaParaMarta.resultado=1;
		//printf("Resultado:%d\n",respuestaParaMarta.resultado);
		if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaMap),MSG_WAITALL)==-1){
			perror("send");
			log_error(logger,"Fallo el envio de la respuesta de un map a marta");
		}
		log_info(logger,"Finalizó un hilo MAP.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",mapperStruct->archivoResultadoMap);
		pthread_exit((void*)0);
	}

	respuestaParaMarta.resultado=resultadoMap;
	//printf("Resultado:%d\n",respuestaParaMarta.resultado);

	if(send(marta_sock,&respuestaParaMarta,sizeof(t_respuestaMap),MSG_WAITALL)==-1){
		perror("send");
		log_error(logger,"Fallo el envio de la respuesta de un map a marta");
		log_info(logger,"Finalizó un hilo MAP.\n\tResultado: fallido\n\tNombre archivo resultado que tendría: %s",mapperStruct->archivoResultadoMap);
		pthread_exit((void*)0);
	}

	//close(nodo_sock);

	log_info(logger,"Finalizó un hilo MAP.\n\tResultado: exitoso\n\tNombre archivo resultado: %s",mapperStruct->archivoResultadoMap);

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
