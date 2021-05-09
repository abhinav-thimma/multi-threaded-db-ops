#include<stdio.h>
#include<string.h>
#include<stdlib.h>


//structure for the row head;
struct row
{
	int commit;
	int curr;
	int *colPresent;
	struct colNode* right;
};


//structure for node values
struct colNode
{
	struct colNode* right;
	int colNum;
	char* val;
	int version;
	struct colNode* bottom;
};



//int maxNoOfRows = 50;
//maximum no of cols in a given table


//puts the given arr data in respective positions
void put(struct row* table, int rowNum, struct colNode* arr, int n, int version)
{
	
	/*if (version == table[rowNum].commit)
	{

	*/
	for (int i = 0; i < n; i++)
	{
		int colNum = arr[i].colNum;

		if (table[rowNum].colPresent[colNum] == 0)
		{
			struct colNode* temp = (struct colNode*)malloc(sizeof(struct colNode));
			temp->colNum = colNum;
			temp->version = 1;
			temp->val = arr[i].val;
			temp->right = NULL;
			temp->bottom = NULL;


			struct colNode* ptr = table[rowNum].right, *t = NULL;
			while (ptr != NULL && ptr->right != NULL)
			{

				if (ptr->colNum > arr[i].colNum)
					break;
				else
				{
					t = ptr;
					ptr = ptr->right;
				}
			}
			if (ptr == NULL)
			{
				ptr = temp;
				table[rowNum].right = ptr;
			}
			else if (ptr->right == NULL)
			{
				ptr->right = temp;
			}
			else
			{
				temp->right = ptr;
				t->right = temp;
			}
			table[rowNum].colPresent[colNum] = 1;

		}
		else
		{
			//creating a new node;
			struct colNode* temp = (struct colNode*)malloc(sizeof(struct colNode));
			temp->colNum = colNum;
			//temp->version = version;
			temp->val = arr[i].val;
			temp->right = NULL;
			temp->bottom = NULL;


			struct colNode* ptr = table[rowNum].right, *t = NULL;

			while (ptr)
			{
				if (ptr->colNum == arr[i].colNum)
					break;
				else
				{
					t = ptr;
					ptr = ptr->right;
				}
			}

			temp->version = ptr->version + 1;
			temp->right = ptr->right;
			if (t)
				t->right = temp;
			else
			{
				table[rowNum].right = temp;
				//continue;
			}
			temp->bottom = ptr;
			ptr->right = NULL;

		}


	}

	table[rowNum].commit += 1;
	/*}
	else
	{
	printf("version mismatch");
	}*/

}



int getCellVersion(struct row* table, int rowNum, int colNum)
{
	struct colNode* start = table[rowNum].right;

	while (start)
	{
		if (start->colNum == colNum)
			break;
		start = start->right;
	}

	return start ? start->version : 0;
}

int getCurrentForRow(struct row* table, int rowNum)
{
	int curr = table[rowNum].curr;
	table[rowNum].curr++;

	return curr;
}

//returns a specified row data
char** get(struct row* table, int maxNoOfCols, int rowNum)
{

	

	struct colNode* start = table[rowNum].right;

	char** res = (char**)calloc(sizeof(char*), maxNoOfCols);

	while (start)
	{

		int colNum = start->colNum;
		res[colNum] = (char*)malloc(sizeof(char) * 100);
		strcpy(res[colNum], start->val);

		start = start->right;
	}


	
	return res;
}


//returns the row values for all rows betwen start and end
char*** scan(struct row* table, int maxNoOfCols, int startRow, int endRow)
{
	if (startRow < 0 || endRow < 0 || startRow > endRow)
		return NULL;

	char*** result = (char***)malloc(sizeof(char**)*(endRow - startRow + 1));

	for (int rowNum = startRow; rowNum <= endRow; rowNum++)
	{

		struct colNode* start = table[rowNum].right;

		char** res = (char**)calloc(sizeof(char*), maxNoOfCols);


		while (start)
		{

			int colNum = start->colNum;
			res[colNum] = (char*)malloc(sizeof(char) * 100);
			strcpy(res[colNum], start->val);

			start = start->right;
		}

		result[rowNum - startRow] = res;
	}

	return result;

}

//deletes a specific row
void deleteRow(struct row* table, int maxNoOfCols, int rowNum)
{

	struct colNode* temp = table[rowNum].right, *temp2 = NULL;

	table[rowNum].commit = 0;
	for (int i = 0; i < maxNoOfCols; i++)
		table[rowNum].colPresent[i] = 0;
	table[rowNum].right = NULL;
	table[rowNum].curr = 0;

	while (temp)
	{
		temp2 = temp->right;

		while (temp)
		{
			struct colNode* d = temp->bottom;
			free(temp);
			temp = d;
		}

		temp = temp2;
	}

}


//this function initialises the row array
struct row* createTable(int colSize)
{
	struct row* table;

	int maxNoOfCols = 3;
	maxNoOfCols = colSize;
	table = (struct row*)malloc(sizeof(struct row) * 50);

	for (int i = 0; i < 50; i++)
	{
		table[i].colPresent = (int*)calloc(sizeof(int), colSize);
		table[i].commit = 0;
		table[i].right = NULL;
	}

	return table;
}


void increaseTableSize(struct row* table, int maxNoOfCols)
{

	struct row* newTable = (struct row*)malloc(sizeof(struct row) * 50 * 2);


	for (int i = 0; i < 50; i++)
		newTable[i] = table[i];

	maxNoOfCols *= 2;
	table = newTable;
}




void test()
{
	struct row* table = createTable(3);

	int maxNoOfCols = 3;
	struct colNode* arr = (struct colNode*)malloc(sizeof(struct colNode) * 3);
	arr[0].colNum = 0;
	arr[0].val = "1";

	arr[1].colNum = 1;
	arr[1].val = "2";

	arr[2].colNum = 2;
	arr[2].val = "3";

	put(table, 0, arr, 3, 0);

	arr[0].colNum = 0;
	arr[0].val = "3";

	put(table, 1, arr, 1, 0);

	//deleteRow(table,maxNoOfCols,1);
	char*** val = scan(table, maxNoOfCols, 0, 1);

	for (int i = 0; i <= 1; i++)
	{
		printf("row %d: ", i);
		for (int j = 0; j < maxNoOfCols; j++)
			printf("%s ", val[i][j]);

		printf("\n");
	}

}