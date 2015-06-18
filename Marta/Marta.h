#define BUF_SIZE 15
#define MENSAJE_SIZE 4096

//Estructura para manejar los Jobs

typedef struct estructura_job{
	int socket;
	char* ip;
	int mapperPendientes; //es igual a la cantidad de bloques donde estan divididos los archivos
	int reducePendientes;
	char* combiner;
} t_job;


typedef struct estructura_mapper {
	char ip_nodo[20];
	int puerto_nodo;
	int bloque;
	char nombreArchivoTemporal[50];
} __attribute__((packed)) t_mapper;

//Prototipos de funciones
void *connection_handler_jobs(); // Esta funcion escucha continuamente si recibo nuevos mensajes
