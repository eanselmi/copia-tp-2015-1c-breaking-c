#define BUF_SIZE 15
#define BLOCK_SIZE 20971520
#define MENSAJE_SIZE 4096
#define MAX_DIRECTORIOS 1024

typedef struct datos_y_bloque{
	uint32_t n_bloque;
	char buf_20mb[BLOCK_SIZE];
} t_datos_y_bloque;

//se creara una lista de archivos, que contendra elementos del tipo "t_archivo"
typedef struct estructura_filesystem {
	char nombre[200];
	uint32_t padre;
	t_list *bloques; //Se debe crear una lista de tipo "t_bloque" y agregarla acá
} t_archivo;

typedef struct estructura_copia {
	char* nodo;
	int bloqueNodo;
	char md5[32];
} t_copias;

typedef struct estructura_bloque {
	t_list *copias;
} t_bloque;

typedef struct estructura_manejo_nodos {
	int socket;
	char nodo_id[6];
	int estado;
	int estado_red;
	char *ip;
	int puerto;
	int puerto_escucha_nodo;
	char *bloques_bitarray;
	t_bitarray *bloques_del_nodo;
	int bloques_libres;
	int bloques_totales;
} t_nodo;

typedef struct estructura_directorio{
	uint32_t id;
	char* nombre;
	uint32_t padre;
}t_dir;


//----------------------------------------------------------------------------------------
//------------------------------ FUNCIONES PRINCIPALES DE CONSOLA ------------------------
//----------------------------------------------------------------------------------------
void FormatearFilesystem ();							//DESARROLLADA                          mandar formatear  OK
void EliminarArchivo();									//DESARROLLADA, falta persistencia      1. mandar elim_arch 2. nombre 3. padre
void RenombrarArchivo();								//DESARROLLADA, falta persistencia      1. mandar renom_arch 2. nombre 3. padre 4. nuevo nombre
void MoverArchivo();									//DESARROLLADA, falta persistencia      1. mov_arch 2. nom 3. padre_viejo  4. padre_nuevo
void CrearDirectorio();									//DESARROLLADA
void EliminarDirectorio();								//DESARROLLADA
void RenombrarDirectorio();								//DESARROLLADA
void MoverDirectorio();									//DESARROLLADA
int CopiarArchivoAMDFS();								//DESARROLLADA					        1. mandar nuevo_arch --- mandar casi igual que al principio
int CopiarArchivoDelMDFS(int flag, char*unArchivo);		//DESARROLLADA
void MD5DeArchivo();									//DESARROLLADA
int VerBloque();										//DESARROLLADA
void BorrarBloque();									//DESARROLLADA, falta persistencia      1. mandar elim_bloque 2. archivo 3. padre 4. n_bloque 5.nodo_id  6.bloque
void CopiarBloque();									//DESARROLLADA, falta persistencia      1. mandar nuevo_bloque 2. archivo 3. padre 4. n_bloque 5.nodo_id_nuevo 6. bloque_nuevo
void AgregarNodo();										//DESARROLLADA
void EliminarNodo();  									//DESARROLLADA
void eliminar_listas(t_list *archivos_l, t_list *directorios_d, t_list *nodos_n);	//DESARROLLADA

//----------------------------------------------------------------------------------------
//------------------------------- FUNCIONES AUXILIARES -----------------------------------
//----------------------------------------------------------------------------------------

int Menu();
void DibujarMenu();
void listar_archivos_subidos(t_list *archivos);
int obtenerEstadoDelBloque(char *nodo,int bloqueNodo);
void listar_directorios(void);
void persistir_archivo(t_archivo *archivo);
int obtenerEstadoDelNodo(char* nodo);
void recuperar_persistencia(void);
void actualizar_persistencia_directorio_eliminado(int idPadre);
void persistir_directorio(t_dir *directorio);
void actualizar_persistencia_directorio_movido(int idPadre, int nuevoPadre);
void actualizar_persistencia_directorio_renombrado(int idPadre, char*nuevoNombre);
void obtenerNodosMasLibres(void);
void listarDirectoriosCreados();
t_list* obtenerHijos(int);
void listarDirectoriosCreadosRecursiva(int id, char path[200]);
char *obtenerPath(char *nombre, int dir_id);
int copiar_lista_de_nodos(t_list *destino,t_list* origen);
int copiar_lista_de_archivos(t_list* destino, t_list* origen);
bool nodos_mas_libres(t_nodo *vacio, t_nodo *mas_vacio);
static void eliminar_lista_de_copias (t_copias *self);
static void eliminar_lista_de_bloques(t_bloque *self);
static void eliminar_lista_de_archivos (t_archivo *self);
static void eliminar_lista_de_directorio(t_dir *self);
void *connection_handler_escucha(); // Esta funcion escucha continuamente si recibo nuevos mensajes
static t_nodo *agregar_nodo_a_lista(char nodo_id[6],int socket,int est,int estado_red,char *ip, int port,int puerto_escucha, int bloques_lib, int bloques_tot);
void modificar_estado_nodo (char nodo_id[6],int socket,int port,int estado,int estado_red);
void listar_nodos_conectados(t_list *nodos);
char *obtener_md5(char *bloque);
int validar_nodo_nuevo (char nodo_id[6]);
int validar_nodo_reconectado (char nodo_id[6]);
char *buscar_nodo_id(char *ip, int port);
char *obtener_id_nodo(char *ip);
void formatear_nodos(void);
int BuscarArchivoPorNombre ();
uint32_t BuscarPadre (char* path);            			//Devuelve el idPadre en caso de éxito, devuelve -1 si no lo encuentra
//static void eliminar_bloques(t_copias *bloque);
long ExisteEnLaLista(t_list* listaDirectorios, char* nombreDirectorioABuscar, uint32_t idPadre);
int BuscarMenorIndiceLibre (char indiceDirectorios[]);
static void directorio_destroy(t_dir* self);
//static void archivo_destroy(t_archivo* self);
int obtener_socket_de_nodo_con_id(char*id);
void enviarNumeroDeBloqueANodo(int socket_nodo, int bloque);
char *recibirBloque(int socket_nodo);//recibe un bloque de un nodo
