#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>

//Defino puerto donde va a estar escuchando procesos Job
#define PUERTO_JOBS 5000


//Declaración de funciones
void escucharConeccionesJob();

int main(){
	escucharConeccionesJob();
	return 0;
}

void escucharConeccionesJob(){
	int sockfd, new_fd;  // Escuchar sobre sock_fd, nuevas conexiones sobre new_fd
	struct sockaddr_in my_addr;    // información sobre mi dirección
	struct sockaddr_in their_addr; // información sobre la dirección del cliente
	unsigned int sin_size;

	if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) { //si función socket devuelve -1 es error
	            perror("socket");
	            exit(1);
	        }

	my_addr.sin_family = AF_INET;         		// Ordenación de bytes de la máquina
	my_addr.sin_port = htons(PUERTO_JOBS);     	// Puerto de escucha de Jobs. short, ordenación de bytes de la red
	my_addr.sin_addr.s_addr = inet_addr("127.0.0.1"); 	// Mi dirección IP
	memset(&(my_addr.sin_zero), '\0', 8); 				// Poner a cero el resto de la estructura

	if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof(struct sockaddr))== -1) { //Si la función bind devuelve -1 es error
	            perror("bind");
	            exit(1);
	        }
	if (listen(sockfd, 10) == -1) { //Si la función listen devuelve -1 es error
	            perror("listen");
	            exit(1);
	        }

	sin_size = sizeof(struct sockaddr_in);
	new_fd = accept(sockfd, (struct sockaddr *)&their_addr,&sin_size);
	if (new_fd == -1) {   //si accept devuelve -1 es error
		perror("accept");
	}
	printf("server: got connection from IP:%d\n",inet_ntoa(their_addr.sin_addr));

}
