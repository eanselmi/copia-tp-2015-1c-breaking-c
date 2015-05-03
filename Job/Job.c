#include <stdio.h>
#include <commons/log.h>
#include <commons/config.h>

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
	configurador= config_create("resources/jobConfig.conf");
	printf("COMBINER=%s\n",config_get_string_value(configurador,"COMBINER"));
}

void conectaraMarta(){
	t_log* logger;
	logger = log_create("./jobLog.log", "Job", true, LOG_LEVEL_INFO); //se crea la instancia de log, que tambien imprimira en pantalla
	log_info(logger,"Se conectó a MaRTA. IP: a.x.y.z, Puerto: xxxx\n"); //se agrega al log en modo de informacion la coneccion con MaRTA
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
