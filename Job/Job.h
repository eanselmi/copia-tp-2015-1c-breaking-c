#define BUF_SIZE 15
#define BUF_ARCH 4096
#define MAPPER_SIZE 1024

typedef struct estructura_mapper {
	char ip_nodo[20];
	int puerto_nodo;
	int bloque;
	char nombreArchivoTemporal[50];
} __attribute__((packed)) t_mapper;

typedef struct datos_para_map{
	uint32_t bloque;
	char nomArchTemp[50];
	char rutinaMap[MAPPER_SIZE];
} __attribute__((packed)) t_datosMap;


typedef struct estructura_reduce {
	char* ip_nodoPpal;
	int puerto_nodoPpal;
	t_list* listaNodos; //una lista que tenga los otros nodos y archivos a donde aplicar reduce (lista de t_reduce_otrosnodos)
	char* nombreArchivoFinal;
} t_reduce;

typedef struct lista_nodos_reduce{
	char* ip_nodo;
	int puerto_nodo;
	char* archivoAAplicarReduce;
} __attribute__((packed)) t_archivosReduce;


//Declaración de funciones
void* hilo_mapper(t_mapper*);
void* hilo_reduce(t_reduce*);
char* getFileContent(char*); //Devuelve el contenido de un file, hasta 4096 bytes -> 4 KB (MAPPER_SIZE)
