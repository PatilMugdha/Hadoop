/*
 * MR.c
 *
 *  Created on: Feb 2, 2019
 *      Author: patil
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <malloc.h>
#include "mapreduce.h"

#define NUM_REDUCERS 4
#define NUM_MAPPERS 4

typedef struct map_launcher_input {
	Mapper map;
	char* fileset[30];
	pthread_t tid;

} Map_launcher_input;

typedef struct s_words {
	char* str;
	struct s_words* next;
} words;

words* create_words(char* word) {
	words* newWords = malloc(sizeof(words));
	if (NULL != newWords) {
		newWords->str = word;
		newWords->next = NULL;
	}
	return newWords;
}

words* add_word(words* wordList, char* word) {
	words* newWords = create_words(word);
	if (NULL != newWords) {
		newWords->next = wordList;
	}
	return newWords;
}

pthread_t tid[10];
int counter;
pthread_mutex_t lock;
words *table[NUM_REDUCERS];
void MR_Emit(char *key, char *value) {
	//complete
	printf("\ntoken: %s", key);
	int hash = MR_DefaultHashPartition(key, 4);
	printf("\nHash %d", hash);

	if (table[hash] == NULL) {
		table[hash] = create_words(key);
		printf("\nInserted %s for first time", table[hash]->str);
	} else {
		table[hash] = add_word(table[hash]->next, key);
		printf("\nInserted %s in the list", table[hash]->str);
	}

}

void Map(char *file_name) {
	printf("\nInside map");
	FILE *fp = fopen(file_name, "r");
	//assert(fp != NULL);

	char *line = NULL;
	size_t size = 0;

	while (getline(&line, &size, fp) != -1) {

		char *token, *dummy = line;
		//printf("\n%s", dummy);
		token = strtok(dummy, " \t\n\r");
		while (token) {
			//printf("\ntoken: %s", token);
			token = strtok(NULL, " ");
			if (token != NULL)
				MR_Emit(token, "1");
		}
	}
	free(line);
	fclose(fp);
}

typedef void (*Launcher)(Map_launcher_input *mli);
void Launch_mappers(Map_launcher_input *mli);
void Launch_mappers(Map_launcher_input *mli) {

	printf("Inside launcher");
	int i = 0;
	int n = (int) (sizeof(mli->fileset) / sizeof(mli->fileset[0]));
	pthread_t p;

	while (i < n) {
		printf("\n %s", mli->fileset[i++]);
		/*int error = pthread_create(&p, NULL, mli->map, mli->fileset[i]);
		 if (error != 0)
		 printf("\nThread can't be created :[%s]", strerror(error));*/
		i++;
	}
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

void *create_mapper(Map_launcher_input *mli);
void *create_mapper(Map_launcher_input *mli) {
	printf("\nInside struct %s", mli->fileset[0]);

	int i = 0;
	int n = (int) (sizeof(mli->fileset) / sizeof(mli->fileset[0]));
	pthread_t p;

	while (i < n) {
		printf("\n %s", mli->fileset[i++]);
		int error = pthread_create(&p, NULL, mli->map, mli->fileset[i]);
		if (error != 0)
			printf("\nThread can't be created :[%s]", strerror(error));
	}

}

int MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
		int num_reducers, Partitioner partition) {

	printf("Total_files:%d", argc - 1);
	Launcher launcher = &create_mapper;
	//create launcher threads
	char *filename[30], *fileset[30];
	int i = 0, k = 0, start = 0, total_files = 0;
	int files_per_thread = 0;
	if ((argc - 1) >= num_mappers) {
		files_per_thread = (argc - 1) / num_mappers;
		printf("\nFiles per thread: %d", files_per_thread);
	} else {
		files_per_thread = (argc - 1);
	}

	for (k = 1; k < argc; k++) {

		filename[k - 1] = malloc(256 * sizeof *filename[k - 1]);
		strcpy(filename[k - 1], argv[k]);
		printf("\n%s", filename[k - 1]);
	}

	total_files = files_per_thread;

	int j = 0;
	for (i = 0; i < num_mappers; i++) {
		printf("\nFor thread %d", i);
		k = 0;

		Map_launcher_input *mli = malloc(sizeof(*mli));
		for (j = start; j < total_files; j++) {
			mli->map = &Map;
			mli->fileset[k] = malloc(256 * sizeof *fileset[j]);

			strcpy(mli->fileset[k++], filename[j]);
		}
		printf("\nStart: %d total_files: %d j: %d", start, total_files, j);
		start = j;
		total_files = j + files_per_thread;
		printf("\nStart: %d total_files: %d j: %d", start, total_files, j);

		//create thread here
		int error = pthread_create(&(tid[i]), NULL, map, mli->fileset[0]);
		if (error != 0)
			printf("\nThread can't be created :[%s]", strerror(error));

		printf("\nloop %d is over", i);
		if (total_files > argc - 1) //handle edge case
			break;
	}

	i = 0;
	while (i < num_mappers) {
		pthread_join(tid[i], NULL);
		i++;
	}

	return 0;
}

int main(int argc, char* argv[]) {

	Mapper map = &Map;
	Reducer reduce = &Reduce;
	Partitioner partitioner = &MR_DefaultHashPartition;

	MR_Run(argc, argv, map, NUM_MAPPERS, reduce, NUM_REDUCERS, partitioner);
	return 0;
}

