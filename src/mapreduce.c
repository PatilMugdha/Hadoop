/*
 * mapreduce.c
 *
 *  Created on: Jan 22, 2019
 *      Author: patil
 */

#include <stdio.h>
#include <stdlib.h>

int addition(int i, int j) {
	int total;
	total = i + j;
	return total;
}

void passing_function(void *ptr) {
	int* i = ptr;
	printf("%d", *i);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
	unsigned long hash = 5381;
	int c;

	while ((c = *key++) != '\0')
		hash = hash * 33 + c;
	return hash % num_partitions;
}
