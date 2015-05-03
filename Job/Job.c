#include <stdio.h>
#include <commons/log.h>
#include <commons/config.h>
#include <sys/socket.h>
#include <netinet/in.h>


//Declaración de funciones
void conectaraMarta();
void desconectarDeMarta();
void configurar();

//Declaración de variables
t_config* configurador;

int main(void){
	configurar();
	conectaraMarta(); //por el momento, solamente se implementa el Log
	desconectarDeMarta(); //solo log
	return 0;
}

void configurar(){
	configurador= config_create("resources/jobConfig.conf"); //se asigna el archivo de configuración especificado en la ruta
	printf("COMBINER=%s\n",config_get_string_value(configurador,"COMBINER")); //para probar que realmente tomó al archivo de configuración
}

void conectaraMarta(){
	int sockfd, numbytes, new_fd;
	struct sockaddr_in their_addr;

	if((sockfd=socket(AF_INET,SOCK_STREAM,0))==-1){ //si función socket devuelve -1 es error
        perror("socket");
        exit(1);
	}
	their_addr.sin_family=AF_INET;
	their_addr.sin_port=htons(config_get_int_value(configurador,"PUERTO_MARTA"));
	their_addr.sin_addr.s_addr=inet_addr(config_get_string_value(configurador,"IP_MARTA"));
	memset(&(their_addr.sin_zero),'\0',8);

	if((new_fd=connect(sockfd,(struct sockaddr *)&their_addr,sizeof(struct sockaddr)))==-1){
		perror("connect");
		exit(1);
	}

	t_log* logger;
	logger = log_create("./jobLog.log", "Job", true, LOG_LEVEL_INFO); //se crea la instancia de log, que tambien imprimira en pantalla
	log_info(logger,"Se conectó a MaRTA. IP: %s, Puerto: %d\n",config_get_string_value(configurador,"IP_MARTA"),config_get_int_value(configurador,"PUERTO_MARTA")); //se agrega al log en modo de informacion la coneccion con MaRTA
	log_destroy(logger); //se elimina la instancia de log
	return;
}

void desconectarDeMarta(){
	t_log* logger;
	logger = log_create("./jobLog.log", "Job", true, LOG_LEVEL_INFO); //se crea la instancia de log, que tambien imprimira en pantalla
	log_info(logger,"Se desconectó de MaRTA. IP: a.x.y.z, Puerto: xxxx\n"); //se agrega al log en modo de informacion la coneccion con MaRTA
	log_destroy(logger); //se elimina la instancia de log
	return;
}
