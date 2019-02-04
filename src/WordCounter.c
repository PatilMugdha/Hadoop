/*

 ============================================================================
 Name        : WordCounter.c
 Author      : mugdha93
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <malloc.h>
#include "mapreduce.h"

void takes_a_function(void (*f)(void *), void *);
//declared a typedef pointer
typedef void (*P)(void *data);

void takes_a_function(P ptr, void* data) {
	ptr(data);
}
struct node {
	char* key;
	int val;
	struct node *next;
};

struct table {
	int size;
	struct node **list;
};

typedef struct map_launcher_input {
	Mapper map;
	const char* fileset[30];
} Map_launcher_input;

struct table *t;

pthread_t tid[10];
int counter;
pthread_mutex_t lock;
// External functions: these are what you must define

void MR_Emit(char *key, char *value) {
	//complete
	printf("\ntoken: %s", key);
}
typedef void (*Launcher)(Map_launcher_input *mli);
void Launch_mappers(Map_launcher_input *mli);
int c = 0;

void Map(char *file_name) {
printf("Inside map");
	FILE *fp = fopen(file_name, "r");
	//assert(fp != NULL);

	char *line = NULL;
	size_t size = 0;

	while (getline(&line, &size, fp) != -1) {

		char *token, *dummy = line;
		printf("\n%s", dummy);
		token = strtok(dummy, " \t\n\r");
		while (token) {
			printf("\ntoken: %s", token);
			token = strtok(NULL, " ");
			//MR_Emit(token, "1");
		}
	}
	free(line);
	fclose(fp);
}

void Launch_mappers(Map_launcher_input *mli) {

	printf("\nInside launcher mappers %d", c++);
	int i = 0;
	int n = (int)(sizeof(mli->fileset) / sizeof(mli->fileset[0]));
	pthread_t p;
	//printf("n: %d",n);
	while (i < n) {
		int error = pthread_create(&p, NULL, mli->map, mli->fileset[i]);
		if (error != 0)
			printf("\nThread can't be created :[%s]", strerror(error));
		//pthread_join(p, NULL);
		i++;
	}


}


Map_launcher_input *mli;
int MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
		int num_reducers, Partitioner partition) {

	Launcher launcher = &Launch_mappers;
	//create launcher threads
	char *filename[30];
	int i = 0, k = 0;
	for (k = 1; k < argc; ++k) {

		filename[k - 1] = malloc(256 * sizeof *filename[k - 1]);
		strcpy(filename[k - 1], argv[k]);
	}

	int files_per_thread = (argc - 1) / num_mappers;
	int start = 0, itr = 0, total_files;
	k = 0;
	total_files = files_per_thread;

	char *fileset[30];
	while (i < num_mappers) {

		for (itr = start; itr < total_files; itr++) {
			fileset[itr] = malloc(256 * sizeof *fileset[itr]);
			strcpy(fileset[itr], filename[k++]);
			printf("%s",fileset[itr]);
		}
		start = itr;
		total_files += files_per_thread;

		mli=malloc(sizeof(*mli));

		strcpy(mli->fileset, fileset);
        printf("My file: %s",mli->fileset[0]);
		mli->map = &Map;
		int error = pthread_create(&(tid[i]), NULL, launcher, mli);
		if (error != 0)
			printf("\nThread can't be created :[%s]", strerror(error));
		i++;
	}

	i = 0;
	while (i < num_mappers) {
		pthread_join(tid[i], NULL);
		i++;
	}
	return 0;
}

//earlier
int MR_Run_test(int argc, char *argv[], Mapper map, int num_mappers,
		Reducer reduce, int num_reducers, Partitioner partition) {

	int i = 0;
	int error;
	//complete
	if (pthread_mutex_init(&lock, NULL) != 0) {
		printf("\n mutex init has failed\n");
		return 1;
	}

	char *filename[30];
	for (int k = 1; k < argc; ++k) {

		filename[k - 1] = malloc(256 * sizeof *filename[k - 1]);
		strcpy(filename[k - 1], argv[k]);
	}

	while (i < argc - 1) {
		error = pthread_create(&(tid[i]), NULL, map, filename[i]);
		if (error != 0)
			printf("\nThread can't be created :[%s]", strerror(error));
		i++;
	}

	i = 0;
	while (i < argc - 1) {
		pthread_join(tid[i], NULL);
		i++;
	}

	pthread_mutex_destroy(&lock);
	return 0;
}

char get_next(char *key, int partition_number) {
	//complete
	return '\0';
}

char* strsep(char** dummy, char* separator) {
	//complete
	return strtok(*dummy, separator);
}

void Reduce(char *key, Getter get_next, int partition_number) {
	int count = 0;
	char *value;
	while ((value = get_next(key, partition_number)) != NULL)
		count++;
	printf("%s %d\n", key, count);
}

void* test(void *arg) {
	char* filename = (char*) arg;
	pthread_mutex_lock(&lock);

	printf("\n Job %d has started\n", counter);

	printf("\nfile again: %s", filename);
	//Map(filename[k]);

	printf("\n Job %d has finished\n", counter);

	pthread_mutex_unlock(&lock);

	return NULL;
}

int main(int argc, char* argv[]) {

	Mapper map = &Map;
	Reducer reduce = &Reduce;
	Partitioner partitioner = &MR_DefaultHashPartition;

	MR_Run(argc, argv, map, 10, reduce, 10, partitioner);
	return 0;
}

*/
