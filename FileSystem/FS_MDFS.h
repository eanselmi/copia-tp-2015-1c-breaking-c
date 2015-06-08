#define BUF_SIZE 50
#define BLOCK_SIZE 20971520
#define MENSAJE_SIZE 4096
#define MAX_DIRECTORIOS 1024


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
	t_list *copias;
} t_bloque;

typedef struct estructura_manejo_nodos {
	int socket;
	char nodo_id[10];
	int estado;
	int estado_red;
	char *ip;
	int puerto;
	int puerto_escucha_nodo;
	char *bloques_bitarray;
	t_bitarray bloques_del_nodo;
	int bloques_libres;
	int bloques_totales;
} t_nodo;

typedef struct estructura_directorio{
	uint32_t id;
	char* nombre;
	uint32_t padre;
}t_dir;


//Prototipos de Funciones
int Menu();
void DibujarMenu();
void *connection_handler_escucha(); // Esta funcion escucha continuamente si recibo nuevos mensajes
static t_nodo *agregar_nodo_a_lista(int socket,int est,int estado_red,char *ip, int port,int puerto_escucha, int bloques_lib, int bloques_tot);
void modificar_estado_nodo (int socket,char *ip,int port,int estado);
void listar_nodos_conectados(t_list *nodos);
char *obtener_md5(char *archivo);
void formatear_nodos(void);
void FormatearFilesystem ();		//Pame TODAVIA NO DESARROLLADA
void EliminarArchivo();				//DESARROLLADA
void RenombrarArchivo ();			//DESARROLLADA
void MoverArchivo();				//DESARROLLADA
void CrearDirectorio();				//DESARROLLADA, falta persistencia
void EliminarDirectorio();			//DESARROLLADA, falta persistencia
void RenombrarDirectorio();			//DESARROLLADA, falta persistencia
void MoverDirectorio();				//DESARROLLADA, falta persistencia
int CopiarArchivoAMDFS();			//Pame TODAVIA NO DESARROLLADA
void CopiarArchivoDelMDFS();		//Pame TODAVIA NO DESARROLLADA
void MD5DeArchivo();				//Pame TODAVIA NO DESARROLLADA
void VerBloques();					//Andy TODAVIA NO DESARROLLADA
void BorrarBloques();				//Andy TODAVIA NO DESARROLLADA
void CopiarBloques();				//Andy TODAVIA NO DESARROLLADA
void AgregarNodo();					//DESARROLLADA
void EliminarNodo();  				//DESARROLLADA
uint32_t BuscarArchivoPorNombre (); //DESARROLLADA
uint32_t BuscarPadre ();            //DESARROLLADA
static void eliminar_bloques(t_copias *bloque);
long ExisteEnLaLista(t_list* listaDirectorios, char* nombreDirectorioABuscar, uint32_t idPadre);
int BuscarMenorIndiceLibre (char indiceDirectorios[]);
static void directorio_destroy(t_dir* self);
static void archivo_destroy(t_archivo* self);
