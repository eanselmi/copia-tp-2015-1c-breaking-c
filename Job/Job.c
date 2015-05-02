#include <stdio.h>
#include <commons/log.h>

//Declaración de funciones
void conectaraMarta();

int main(void){
	conectaraMarta(); //por el momento, solamente se implementa el Log
	return 0;
}

void conectaraMarta(){
	t_log* logger;
	logger = log_create("./jobLog.log", "Job", true, LOG_LEVEL_INFO); //se crea la instancia de log, que tambien imprimira en pantalla
	log_info(logger,"Se conectó a MaRTA. IP: a.x.y.z, Puerto: xxxx\n"); //se agrega al log en modo de informacion la coneccion con MaRTA
	log_destroy(logger); //se elimina la instancia de log
	return;
}
