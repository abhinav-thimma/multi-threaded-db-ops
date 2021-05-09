#include<pthread.h>
#include "Database.h"


struct instruction
{
	int operation; // 1- put , 2 - get
	struct colNode arr[5];
	int rowNum; int n; int version;
	struct instruction* next;
};

struct instruction * front=NULL;
struct instruction * back=NULL;

//concurrent queue
struct instruction *queue = NULL;		pthread_mutex_t queuePosLock;
//int front = 0, back = 0;


void enqueue(struct instruction *inst)
{
	pthread_mutex_lock(&queuePosLock);

	struct instruction * newNode = (struct instruction*)malloc(sizeof(struct instruction));
	newNode->n = inst->n;
	newNode->operation = inst->operation;
	newNode->rowNum = inst->rowNum;
	newNode->version = inst->version;
	newNode->next = NULL;

	for (int i = 0; i < newNode->n; i++)
	{
		newNode->arr[i] = inst->arr[i];
	}

	if (back == NULL)
	{
		front = newNode;
		back = newNode;
	}
	else
	{
		back->next = newNode;
	}


	//queue[back++] = *(i);


	pthread_mutex_unlock(&queuePosLock);
}

struct instruction* dequeue()
{
	pthread_mutex_lock(&queuePosLock);


	struct instruction *inst;//= &queue[front++];

	if (front == NULL)
		return NULL;
	else
	{
		inst = front;
		front = front->next;

		if (front == NULL)
		{
			back = NULL;
		}
	}

	pthread_mutex_unlock(&queuePosLock);

	return inst;
}


void printQueue()
{
	struct instruction* inst;

	inst = front;

	while (inst)
	{
		printf("%d\t", inst->operation);
		inst = inst->next;
	}
}