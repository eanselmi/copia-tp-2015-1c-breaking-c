#define BUF_SIZE 15
#define MENSAJE_SIZE 4096
#define TAM_NOMFINAL 60

//estructura con la información de FileSystem
typedef struct estructura_filesystem {
	char nombre[200];
	uint32_t padre;
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
	char resultadoDelJob[200];
	int mapperPendientes; //es igual a la cantidad de bloques donde estan divididos los archivos
	int reducePendientes;
	char combiner[3];
	char estado[10];
	int nroJob;
} t_job;


typedef struct estructura_mapper {
	char ip_nodo[20];
	int puerto_nodo;
	int bloque;
	char archivoResultadoMap[TAM_NOMFINAL];
} __attribute__((packed)) t_mapper;

typedef struct estructura_respuesta {
	char archivoResultadoMap[TAM_NOMFINAL];
	int resultado; // 0 si salio bien , y 1 si salio mal el map
}__attribute__((packed)) t_respuestaMap;

typedef struct estructura_respuesta_reduce{
	int resultado;
	char archivoResultadoReduce[TAM_NOMFINAL];
	char ip_nodo[20]; //Puede ser el principal, o uno que fallo
	int puerto_nodo; //Puede ser el principal, o uno que fallo
}__attribute__((packed)) t_respuestaReduce;

//Estructura que va a tener marta para poder replanificar
typedef struct estructura_replanificar_map {
	char archivoResultadoMap[TAM_NOMFINAL];
	char nombreArchivoDelJob[TAM_NOMFINAL];
	int padreArchivoJob;
	int bloqueArchivo;
	char nodoId [6];
	int resultado;
}t_replanificarMap;

typedef struct estructura_reduce {
	char ip_nodoPpal[20];
	int puerto_nodoPpal;
	char nombreArchivoFinal[TAM_NOMFINAL];
} __attribute__((packed)) t_reduce;

typedef struct lista_nodos_reduce{
	char ip_nodo[20];
	int puerto_nodo;
	char archivoAAplicarReduce[TAM_NOMFINAL];
} __attribute__((packed)) t_archivosReduce;




//Prototipos de funciones
void *connection_handler_jobs(); // Esta funcion escucha continuamente si recibo nuevos mensajes
void *atenderJob(int*);
t_list *buscarBloques (char*, uint32_t);
void asignarMap(t_list *bloques,int socketJob);
t_nodo* buscarCopiaEnNodos(t_copias *copia);
bool ordenarSegunMapYReduce (t_nodo* menorCarga,t_nodo* mayorCarga);
//static void eliminarCopiasNodo(t_list *self);
void sumarCantMapper(char* nodoASumar);
bool nodoIdMasRepetido(char*,char*);
void restarCantMapper(char* nodoParaRestar);
void sumarCantReducers(char* idNodoASumar);
void restarCantReducers(char* idNodoARestar);
char* obtenerNombreArchivoReduce ();
bool archivoDisponible(t_archivo* archivo);
bool nodoNoDisponible(t_copias* copia);
t_archivo* buscarArchivo(char* nombre, int padre);
t_nodo* traerNodo(char* idNodo);
t_nodo* buscarNodoPorIPYPuerto(char* ipNodo,int puertoNodo);
int BuscarArchivoPos(char* nombreArch, uint32_t idPadre);
static void eliminarListaCopias (t_copias* self);
static void eliminarListaBloques(t_bloque* self);
static void eliminarListaBloques2(t_bloque* self);
static void eliminarListaArchivos (t_archivo* self);
static void eliminarListaArchivos2 (t_archivo* self);
int cantidadTotalDeBloques(char* archivosJob);
t_list * buscarBloquesTotales(char* nombreArchivo);
void estadoNodos();
static void eliminarCopia(t_copias *self);
void estadoMarta();
void estadoJobs();

