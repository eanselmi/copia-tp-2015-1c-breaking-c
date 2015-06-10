#define BUF_SIZE 50
#define BUF_ARCH 4096
#define MAPPER_SIZE 4096

typedef struct estructura_mapper {
	char ip_nodo[20];
	int puerto_nodo;
	int bloque;
	char nombreArchivoTemporal[100];
} __attribute__((packed)) t_mapper;

typedef struct estructura_reduce {
	char* ip_nodoPpal;
	int puerto_nodoPpal;
	char* archivoAAplicarReduceLocal;
	t_list* listaNodos; //una lista que tenga los otros nodos y archivos a donde aplicar reduce (lista de t_reduce_otrosnodos)
	char* nombreArchivoFinal;
} t_reduce;

typedef struct lista_nodos_reduce{
	char* ip_nodo;
	int puerto_nodo;
	char* archivoAAplicarReduce;
} t_reduce_otrosnodos;


//Declaración de funciones
void* hilo_mapper(t_mapper*);
char* getFileContent(char*); //Devuelve el contenido de un file, hasta 4096 bytes -> 4 KB (MAPPER_SIZE)
