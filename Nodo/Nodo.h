#define BLOCK_SIZE 20971520 //block size 20MB
#define BUF_SIZE 15
#define MAPPER_SIZE 1024
#define REDUCE_SIZE 4096
#define TAM_NOMFINAL 60


typedef struct datos_y_bloque{
	uint32_t n_bloque;
	char buf_20mb[BLOCK_SIZE];
} t_datos_y_bloque;

typedef struct datos_para_map{
	uint32_t bloque;
	char nomArchTemp[TAM_NOMFINAL];
	char rutinaMap[MAPPER_SIZE];
} __attribute__((packed)) t_datosMap;

typedef struct lista_nodos_reduce{
	char ip_nodo[20];
	int puerto_nodo;
	char archivoAAplicarReduce[TAM_NOMFINAL];
} __attribute__((packed)) t_archivosReduce;


//Declaración de funciones
char* mapearFileDeDatos();
void setBloque(uint32_t bloque,char* datos);
char* getBloque(int bloque);
char* getFileContent(char* nombre); //Devuelve el file sin el EOF. Hasta 20971520 bytes --> 20 MB
void* manejador_de_escuchas(); //Hilo que va a manejar las conexiones
int estaEnListaNodos(int socket);
int estaEnListaMappers(int socket);
int estaEnListaReducers(int socket);
void ejecutarMapper(char *script,int bloque,char *resultado);
void ordenarMapper(char *nombreMapperTemporal, char* nombreMapperOrdenado);
void* rutinaMap(int *socketMap); //Hilo encargado de ejecutar una rutina Map
void* rutinaReduce(int *socketReduce); //Hilo encargado de ejecutar una rutina Reduce
char* crearBloqueFalso(); //Solo para uso interno, crea un bloque de 20MB
char* crearBloqueAMediasFalso(); // Solo para uso interno, crea un bloque de 10MB
void crearArchivoFalso();//Solo para uso interno, crea un archivo de 50MB en /tmp/archivoPrueba.txt (se puede regular el tamaño en multiplos de 10MB)

//Para probar crearBloqueFalso y grabar en un bloque del nodo hacer lo siguiente
/*Generacion de datos para probar el funcionamiento de la funcion setBloque*/
	//char* datosAEscribir;
	//datosAEscribir=malloc(BLOCK_SIZE);
	//datosAEscribir=crearBloqueFalso();
	//printf("Bloque de tamaño=%d\n",strlen(datosAEscribir)); //me va a decir el tamaño del bloque falso
	//int bloqueAEscribir=0;
//

// Grabará los datos enviados en el bloque solicitado
	//setBloque(bloqueAEscribir,datosAEscribir);

/*Generación de datos para probar la funcion getBloque*/

	//char* datosLeidos;
	//datosLeidos=malloc(BLOCK_SIZE);
	//int bloqueALeer=0;
//

	//datosLeidos=getBloque(bloqueALeer); // Devolverá el contenido del bloque solicitado
	//printf("El bloque leído tiene un tamaño de:%d\n",strlen(datosLeidos));
