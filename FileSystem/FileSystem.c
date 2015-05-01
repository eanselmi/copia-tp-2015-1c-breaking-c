#include <stdio.h>

//Prototipos
int Menu();
void DibujarMenu();


int main(void){
	Menu();
	return 0;
}


//Consola Menu
void DibujarMenu(void){
	printf("1) Formatear el MDFS\n");
	printf("2) Eliminar, Renombrar o Mover archivos\n");
	printf("3) Crear, Eliminar, Renombrar o Mover directorios\n");
	printf("4) Copiar un archivo local al MDFS\n");
	printf("5) Copiar un archivo del MDFS al filesystem local\n");
	printf("6) Solicitar el MD5 de un archivo en MDFS\n");
	printf("7) Ver, Borrar, Copiar los bloques que componen un archivo\n");
	printf("8) Agregar un nodo de datos\n");
	printf("9) Eliminar un nodo de datos\n");
	printf("10) Salir\n");
}

int Menu(void){
	int opcion=0;
	DibujarMenu();
	printf("Ingrese la opción deseada:");
	while (opcion !=10){
		scanf("%d", &opcion);
		switch (opcion){
			case 1: printf("Eligió  Formatear el MDFS\n"); break;
			case 2: printf("Eligió Eliminar, Renombrar o Mover archivos\n"); break;
			case 3: printf("Eligió Crear, Eliminar, Renombrar o Mover directorios\n"); break;
			case 4: printf("Eligió Copiar un archivo local al MDFS\n"); break;
			case 5: printf("Eligió Copiar un archivo del MDFS al filesystem local\n"); break;
			case 6: printf("Eligió Solicitar el MD5 de un archivo en MDFS\n"); break;
			case 7: printf("Eligió Ver, Borrar, Copiar los bloques que componen un archivo\n"); break;
			case 8: printf("Eligió Agregar un nodo de datos\n"); break;
			case 9: printf("Eligió Eliminar un nodo de datos\n"); break;
			case 10: printf("Eligió Salir\n"); break;
			default: printf("Ingrese una opción del 1 al 10\n");break;
		}
	}
	return 0;
}



