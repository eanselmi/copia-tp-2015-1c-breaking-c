#define BLOCK_SIZE 20971520 //block size 20MB
#define BUF_SIZE 50
#define MAPPER_SIZE 4096
#define PATHMAPPERS "./RutinasMap/"

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

