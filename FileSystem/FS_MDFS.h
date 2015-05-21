//se creara una lista de archivos, que contendra elementos del tipo "t_archivo"

typedef struct estructura_filesystem {
	char* nombre;
	uint32_t padre;
	uint32_t tamanio;
	uint32_t estado;
	t_list *bloques; //Se debe crear una lista de tipo "t_bloque" y agregarla acá
} t_archivo;

typedef struct estructura_copia {
	char* nodo;
	int bloqueNodo;
} t_copias;

typedef struct estructura_bloque {
	t_copias copias[3];
} t_bloque;

typedef struct estructura_manejo_nodos {
	int socket;
	char *nodo_id;
	int estado;
	char *ip;
	int puerto;
	int bloques_libres;
	int bloques_totales;
} t_nodo;

typedef struct estructura_directorio{
	uint32_t id;
	char* nombre;
	uint32_t padre;
}t_dir;


