#define BUF_SIZE 15
#define MENSAJE_SIZE 4096
#define TAM_NOMFINAL 60

//estructura con la información de FileSystem
typedef struct estructura_filesystem {
	char* nombre;
	//char* path; // Job le dira el path completo a Marta, ella se lo pregunta a FS y este le devuelve el padre
	uint32_t padre;
	uint32_t estado;
	t_list *bloques; //Se debe crear una lista de tipo "t_bloque" y agregarla acá
} t_archivo;

typedef struct estructura_copia {
	char* nodo;
	int bloqueNodo;
} t_copias;

typedef struct estructura_bloque {
	t_list *copias;
} t_bloque;

typedef struct estructura_manejo_nodos {
	char nodo_id[6];
	int estado;
	char *ip;
	int puerto_escucha_nodo;
	uint32_t cantMappers;	//cantidad de map corriendo en el nodo
	uint32_t cantReducers;  //cantidad de reduce corriendo en el nodo
} t_nodo;




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
	char nombreArchivoTemporal[TAM_NOMFINAL];
} __attribute__((packed)) t_mapper;

typedef struct estructura_respuesta {
	char nombreArchivoTemporal[TAM_NOMFINAL];
	int resultado; // 0 si salio bien , y 1 si salio mal el map
}__attribute__((packed)) t_respuestaMap;

//Estructura que va a tener marta para poder replanificar
typedef struct estructura_replanificar_map {
	char nombreArchivoTemporal[TAM_NOMFINAL];
	char nombreArchivoDelJob[TAM_NOMFINAL];
	int bloqueArchivo;
	t_list* lista_nodos;
}t_replanificarMap;



//Prototipos de funciones
void *connection_handler_jobs(); // Esta funcion escucha continuamente si recibo nuevos mensajes
void *atenderJob(int*);
t_list *buscarBloques (char*, uint32_t);
void asignarMap(t_list *bloques,int socketJob);
t_nodo* buscarCopiaEnNodos(t_copias *copia);
bool ordenarSegunMapYReduce (t_nodo* menorCarga,t_nodo* mayorCarga);
static void eliminarCopiasNodo(t_list *self);
void sumarCantMapper(t_nodo* nodoASumar);
