/*
 ============================================================================
 Name        : WordCounter.c
 Author      : mugdha93
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */
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

struct table *t;

pthread_t tid[2];
int counter;
pthread_mutex_t lock;
// External functions: these are what you must define

void MR_Emit(char *key, char *value) {
	//complete
	printf("\ntoken: %s", key);
	// Partitioner partitioner = &MR_DefaultHashPartition;

	//printf("Token:%s, value:%s",key,value);
}

int MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
		int num_reducers, Partitioner partition) {

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
		//printf("\nfile: %s", filename[k - 1]);
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

void Map(char *file_name) {
	pthread_mutex_lock(&lock);
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
	pthread_mutex_unlock(&lock);
}

void Reduce(char *key, Getter get_next, int partition_number) {
	int count = 0;
	char *value;
	while ((value = get_next(key, partition_number)) != NULL)
		count++;
	printf("%s %d\n", key, count);
}

/*void* trythis(void *arg) {
 char** filename = (char**) arg;
 pthread_mutex_lock(&lock);

 printf("\n Job %d has started\n", counter);
 unsigned long i = 0;
 counter += 1;
 for (i = 0; i < 10; i++) {
 printf("\nCounter: %d", counter);
 counter++;
 }

 for (int k = 0; k < 2; ++k) {
 printf("\nfile again: %s", filename[k]);
 //Map(filename[k]);
 }

 printf("\n Job %d has finished\n", counter);

 pthread_mutex_unlock(&lock);

 return NULL;
 }

 int main(int argc, char* argv[]) {
 int i = 0;
 int error;

 if (pthread_mutex_init(&lock, NULL) != 0) {
 printf("\n mutex init has failed\n");
 return 1;
 }

 char *filename[30];
 for (int k = 1; k < argc; ++k) {

 filename[k] = malloc(256 * sizeof *filename[k]);
 strcpy(filename[k], argv[k]);
 printf("\nfile: %s", filename[k]);
 }

 while (i < 2) {
 error = pthread_create(&(tid[i]), NULL, &trythis, &filename);
 if (error != 0)
 printf("\nThread can't be created :[%s]", strerror(error));
 i++;
 }

 pthread_join(tid[0], NULL);
 pthread_join(tid[1], NULL);
 pthread_mutex_destroy(&lock);

 return 0;
 }*/

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

