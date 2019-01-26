/*
 * mapreduce.h
 *
 *  Created on: Jan 22, 2019
 *      Author: patil
 */

#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

int addition(int i, int j);

//int (* ptr)(int);

int (*v)(int, int);
void passing_function(void *ptr);

// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// External functions: these are what you must define
void MR_Emit(char *key, char *value);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

int MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
		int num_reducers, Partitioner partition);

char get_next(char *key, int partition_number);

#endif /* MAPREDUCE_H_ */
