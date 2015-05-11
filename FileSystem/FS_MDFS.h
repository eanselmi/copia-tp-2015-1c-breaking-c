#define FILENAME 50

typedef uint32_t t_bloque;

typedef struct estructura_filesystem {
unsigned char NomArchivo[FILENAME];
uint32_t DirectorioPadre;
uint32_t Tamanio;
uint32_t EstadoArchivo;
t_bloque * Bloques;
struct estructura_filesystem * Anterior;
struct estructura_filesystem * Siguiente;
} mdfs_struct;

typedef struct estructura_bloque {
// uint32_t Nodo_Bloque [3][3];
uint32_t IdNodo[3];
uint32_t IdBloque[3];
struct estructura_bloque * Anterior;
struct estructura_bloque * Siguiente;
} BStruct;
