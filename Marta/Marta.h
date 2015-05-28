//Estructura para manejar los Jobs

typedef struct estructura_job{
	int socket;
	char* ip;
	int mapperPendientes; //es igual a la cantidad de bloques donde estan divididos los archivos
	int reducePendientes;
	char* combiner;
} t_job;
