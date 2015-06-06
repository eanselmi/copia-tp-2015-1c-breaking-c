#define BLOCK_SIZE 20971520 //block size 20MB
#define BUF_SIZE 50
#define MAPPER_SIZE 4096

//Declaración de funciones
char* mapearFileDeDatos();
void setBloque(int bloque,char* datos);
char* getBloque(int bloque);
char* getFileContent(char* nombre);
void* manejador_de_escuchas(); //Hilo que va a manejar las conexiones
void crearmapper(char*);
int estaEnListaNodos(int socket);
int estaEnListaMappers(int socket);
int estaEnListaReducers(int socket);
void ejecutarScript(char *path,char *argumento,char *resultado);

