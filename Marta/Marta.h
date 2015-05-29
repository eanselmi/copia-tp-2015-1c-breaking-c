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
	char nombreArchivoTemporal[100];
} __attribute__((packed)) t_mapper;
