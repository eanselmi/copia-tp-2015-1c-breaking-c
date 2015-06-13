#define BLOCK_SIZE 20971520 //block size 20MB
#define BUF_SIZE 50
#define MAPPER_SIZE 4096
#define PATHMAPPERS "./RutinasMap/" //Donde guardará las rutinas Map que lleguen
#define PATHTP "/home/utnso/TP" //Donde se hace el git clone

//Declaración de funciones
char* mapearFileDeDatos();
void setBloque(int bloque,char* datos);
char* getBloque(int bloque);
char* getFileContent(char* nombre); //Devuelve el file sin el EOF. Hasta 20971520 bytes --> 20 MB
void* manejador_de_escuchas(); //Hilo que va a manejar las conexiones
int estaEnListaNodos(int socket);
int estaEnListaMappers(int socket);
int estaEnListaReducers(int socket);
void ejecutarMapper(char *script,int bloque,char *resultado);
void ordenarMapper(char *nombreMapperTemporal, char* nombreMapperOrdenado);
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
