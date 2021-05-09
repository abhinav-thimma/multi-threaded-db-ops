#include<stdio.h>
#include<stdlib.h>
#include<pthread.h>
#include<windows.h>
#include "Database.h" 
//#include "ConcurrentQueue.h"
#define POOL_SIZE 4


char namesArr[10][100] = {"abhi","nis","sha","aru","nik","aruhi","hanish","shiva","jos","some"};
pthread_t pool[POOL_SIZE];
int isbusy[POOL_SIZE] = { 0 };



struct instruction
{
	int operation; // 1- put , 2 - get
	struct colNode arr[5];
	int rowNum; int n; int version;
	struct instruction* next;
};



//concurrent queue
struct instruction queue[20];		pthread_mutex_t queuePosLock;		pthread_mutex_t  writeLock;
int front = 0, back = 0;

int getThread()
{
	for (int i = 0; i < POOL_SIZE; i++)
		if (isbusy[i] == 0)
			return i;

	return NULL;
}

void freeThread(int i)
{
	isbusy[i] = 0;
}



//database table
struct row* table;







/*-----------------------------------------------------------------------*/
void* putCaller(void* data)
{
	struct instruction * inst = (struct instruction*)data;
	printf("---------------------------PUT----------------------\n row = %d \n", inst->rowNum);
	for (int i = 0; i < inst->n; i++)
	{
		printf("\t\t%s", inst->arr[i].val);
	}
	printf("\n");

	put(table, inst->rowNum, inst->arr, inst->n, inst->version);

	return NULL;
}


void* getCaller(void* data)
{

	struct instruction* inst = (struct instruction*)data;
	printf("---------------------------GET----------------------\n row = %d\n", inst->rowNum);
	char** result = get(table, 3, inst->rowNum);

	for (int i = 0; i < 3; i++)
		printf("\t\t%s", result[i]);
	printf("\n");



	return (void*)result;
}
/*-----------------------------------------------------------------------*/



void* scheduler(void* data)
{

	printf("----------------scheduler started--------------\n");
	while (1)
	{
		if (front != back)
		{
			//pthread_mutex_lock(&queuePosLock);

			struct instruction copy = queue[front++];// *(dequeue());
			int type = copy.operation;

			if (type == 1)
			{
				//put

				int  i = getThread();
				pthread_create(&pool[i], NULL, putCaller, &copy);
				//pthread_join(pool[i],NULL);

				freeThread(i);

			}
			else if (type == 2)
			{
				//get
				int  i = getThread();
				pthread_create(&pool[i], NULL, getCaller, &copy);
				//pthread_join(pool[i], NULL);

				freeThread(i);
			}


			//pthread_mutex_unlock(&queuePosLock);

		}
	}
}




void scheduleInstruction(struct instruction *i)
{
	int type = i->operation;
	//enqueue(i);
	pthread_mutex_lock(&queuePosLock);
	queue[back++] = *i;
	pthread_mutex_unlock(&queuePosLock);
	/*if (type == 1)
	{
	//put

	pthread_t t;
	pthread_create(&t, NULL, putCaller, i);
	}
	else if (type == 2)
	{
	//get
	pthread_t t;
	pthread_create(&t, NULL, getCaller, i);

	}*/
}



void* clientPutThread(void* a)
{

	
	int r = 1;
	while (r < 5)
	{

		Sleep(1000);
		struct instruction *i = (struct instruction*)malloc(sizeof(struct instruction));
		i->n = 3;

		i->arr[0].colNum = 0;
		i->arr[0].val = namesArr[rand()%10];//"1";

		i->arr[1].colNum = 1;
		i->arr[1].val = namesArr[rand() % 10];// "2";

		i->arr[2].colNum = 2;
		i->arr[2].val = namesArr[rand() % 10];// "3";



		i->rowNum = r++;
		i->operation = 1;
		i->version = 1;

		pthread_mutex_lock(&writeLock);


		scheduleInstruction(i);

		pthread_mutex_unlock(&writeLock);
	}

	

	return NULL;
}



void* clientGetThread(void* a)
{
	int r = 1;
	while (r < 5)
	{

		Sleep(3000);

		struct instruction *i = (struct instruction*)malloc(sizeof(struct instruction));
		i->n = 3;
		i->rowNum = r++;
		i->operation = 2;
		i->version = 1;

		i->arr[0].colNum = 0;
		i->arr[0].val = namesArr[rand() % 10];//"1";

		i->arr[1].colNum = 1;
		i->arr[1].val = namesArr[rand() % 10];// "2";

		i->arr[2].colNum = 2;
		i->arr[2].val = namesArr[rand() % 10];


		scheduleInstruction(i);
	}

	return NULL;
}







int main()
{
	
	table = createTable(3);
	pthread_t readClient;
	pthread_t writeClient;
	pthread_t schedulerThread;


	//runnning the scheduler
	pthread_create(&schedulerThread, NULL, scheduler,NULL);


	pthread_mutex_init(&queuePosLock, NULL);
	pthread_mutex_init(&writeLock, NULL);

	pthread_create(&readClient, NULL, clientGetThread, NULL);
	pthread_create(&writeClient, NULL, clientPutThread, NULL);


	pthread_join(readClient,NULL);
	pthread_join(writeClient,NULL);

	Sleep(2000);
	printf("\n---------------results from queue-----------------\n");
	printf("\n front = %d back = %d",front,back);

	pthread_mutex_destroy(&queuePosLock);
	pthread_mutex_destroy(&writeLock);
	//table = createTable(3);
	

	/*struct colNode* arr = (struct colNode*)malloc(sizeof(struct colNode) * 3);
	arr[0].colNum = 0;
	arr[0].val = "1";

	arr[1].colNum = 1;
	arr[1].val = "2";

	arr[2].colNum = 2;
	arr[2].val = "3";

	put(table, 0, arr, 3, 0);

	//put(table, 0, arr, 2, 1);

	char** data = get(table,2,0);

	for (int i = 0; i < 2; i++)
		printf("%s\n", data[i]);*/

	getchar();

	return 0;
}