#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <string.h>


/* Developed by Nicholas Gallo
	Class: Operating Systems
        Language: C
        Purpose: A multi-threaded memory-mapped implementation of Merge Sort
        What does it sort: a node datatype called record that contains key and data values
	Algorithm developed: 	            {mmap file to typedef struct record ->
				             split mmap-file into merge blocks and pass to separate threads containing the merge driver function, each containing parameters for two distinct blocks to be merged ->
					    {run 0 = sort phase -> sort both blocks on separate threads created in merge driver function, join threads, and merge both blocks using the merge function ->
					     run 1 ... run n = merge phases -> merge blocks passed to merge driver function in the merge function} ->
					     join merge driver threads after each run -> 
					     update blocks with new data from each run ->
					     update amount of records in each block ->
					     loop for as many runs as it takes to merge all of the sorted blocks}
	Flow:
		 _______							  _____________
		|block 1|      / -> Sort block 1 \ 				 |block 1 and 2|
		|block 2|  ->  \ -> Sort block 2 /  ->  merge blocks 1 and 2  -> |	       |
		|_______|				      			 |             |
		|block 3|      / -> Sort block 3 \        			 |block 3 and 4|
		|block 4|  ->  \ -> Sort block 4 /  ->  merge blocks 3 and 4  -> |             |
		|_______|							 |_____________|
	
	
	All code from here on is commented
	##################################			
*/


//defines object length, as well as keysize of object and datasize of object

#define objln 64
#define keysize 8
#define datasize 56

//defines the structure for each node within the mapped file

typedef struct Record {
    char key[keysize];
    char data[datasize];
} record;

//defines parameters for merging threads

typedef struct M {
    int block1;
    int block2;
	int subitems;
	int x;
} mergeparam;

//defines parameters for run 0, the sort phase

typedef struct S {
	int blockindex;
	int itm;
} sortparam;

//defines the functions: Merge Driver Function, Sorting thread, Merging thread, Comparison function, and Graph function

void *Merge(void *param);
void *SortFunc(void *param);
int compareFunc(const void *a, const void *b);
void mergeFunc(mergeparam *params);
//defines the pointer arrays for original memory mapped objects and temporary sorting array

record *records;
record *temprecord;

//global initialization of number of threads, as well as global file details

int Threadnum;
struct stat sb;

/* Note on my structs and global variables:
	
	struct record defines each object within the file, in this case containing a key and a data value
	struct mergeparam defines a pair of blocks to be either be separately sorted and/or merged, as well as which run the algorithm is on and items in each block
	struct sortparam defines a singular block to be sorted using the qsort algorithm

	The key value is 8 bits and the data within is object is 56 bits, with each object having a length of 64 bits
	
	The arrays containing object pointers are defined globally so that all threads can use them without needing to be passed them
	
	File size and thread number are also useful values to be defined globally
*/

int main(int argc, char ** argv) {

    //initialize filedecriptor and stat structure
    int i;
    int filedescrip;

    //number of arguments in command line: program, thread number, and file name
    if(argc != 3) {
        printf("Wrong amount of arguments.\n");
        exit(EXIT_FAILURE);
    }

    Threadnum = atoi(argv[1]);
    filedescrip = open(argv[2], O_RDWR);

    if(filedescrip < 0) {
        printf("File open failed.\n");
        exit(EXIT_FAILURE);
    }
    if(fstat(filedescrip, &sb) < 0) {
        printf("fstat error.\n");
        exit(EXIT_FAILURE);
    }

    //file is mapped to pointer array of object records
    records = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, filedescrip, 0);

    if(records == MAP_FAILED) {
        printf("mmap failed.\n");
        exit(EXIT_FAILURE);
    }

    //temp array is identical in size and type to original
    temprecord = (record*)malloc(sb.st_size);

    //number of items in file and subitems for each thread to be sorted

    int items = sb.st_size/objln;
	int subitm = items/Threadnum;
    printf("Items: %d\nThreads: %d\n", items, Threadnum);
	
	//intialize the value for the amount of stages that the algorithms runs through 
    int runs = 0;

	//initialize thread number to be used, which is halved after each run in the loop is completed until it reaches 0
    int thread;
	
	if(Threadnum == 1) {
		thread = Threadnum;
	}
	else if(Threadnum % 2 == 0) {
		thread = Threadnum/2;
	}
	else {
		printf("Threads should be powers of 2 (eg. 2, 4, 8, ...).\n");
		exit(EXIT_FAILURE);
	}

	//begin time at start of algorithm after file is mapped
    clock_t begin = clock();
	
	//begin driver loop in main
    do {

		//dynamic arrays for thread parameters and threads, updating after each run is complete
		printf("stage %d\n", runs);	
	
        mergeparam mergeblocks[thread];
		pthread_t TID[thread];
        for(i = 0; i < thread; i++) {

            //integer values assigned to the first and second block in each pair to be merged
            int first = i * 2;
            int second = first + 1;

			//parameters assigned to be passed to the Merge Driver Function, which operates on pairs of blocks
            mergeblocks[i].block1 = first * subitm;
            mergeblocks[i].block2 = second * subitm;
			mergeblocks[i].subitems = subitm;
			mergeblocks[i].x = runs;

            //create thread and pass the parameters to the Merge Driver Function
            if(pthread_create(&TID[i], NULL, Merge, &mergeblocks[i])) {
                printf("error in creating merge threads.\n");
                exit(EXIT_FAILURE);
            }
        }
        for(i = 0; i < thread; i++) {
            if(pthread_join(TID[i], NULL)) {
                printf("error in joining merge threads.\n");
                exit(EXIT_FAILURE);
            }
        }
     		
		//after the run is complete, the tempory array is copied back to the original array, subitem size is updated, and thread value is halved as the program amount of blocks is halved
        thread = thread/2;
		subitm = subitm * 2;
		runs++;
		memcpy(records, temprecord, sb.st_size);
    } while(thread != 0);
	
	//end timer at end of algorithm
	clock_t end = clock();

    //unmap and free memory, close file
    munmap(records, sb.st_size);
    free(temprecord);
    close(filedescrip);

    //display algorithm runtime
    printf("\nTotal time: %lu nanoseconds\n", end - begin);
	
	//exit program
    return 0;

	/* Algorithm for Main Function (Pseudo Code):

		Input: arguments passed from the command line, which are:
									 1.) Program name
									 2.) Number of threads specified by the user, for purposes of testing
									 3.) File name for objects to be sorted
		       formatted as: <./Program> <number of threads> <name of file>
		Function(Input):
			handle I/O and arguments from command line
			memory-map the file specified to an array of pointers containing objects
			find file size, amount of items in the array, and amount of blocks to be merged
			increment until objects are sorted
				initialize parameters for blocks
				pass blocks to Merge Driver, which sorts and merges the objects
				write sorted blocks back into memory after run completes
			free memory
		Exit Program	
	*/ 
}
void *SortFunc(void *param) {
    sortparam *par = (sortparam*)param;
        
	//start index in block of original array that needs to be sorted
    int start = par -> blockindex;
        
	//utilize the C qsort function, passing in the original array, amount of items, object length, as well as string comparison function
    clock_t QSortstart = clock();
	qsort(&records[start], par -> itm, objln, compareFunc);
	clock_t QSortend = clock();
	
	printf("Total time in Sorting thread: %lu nanoseconds\n", QSortend - QSortstart);
		
    pthread_exit(0);

	
	/* Algorithm for Sorting Function (Pseudo Code)

		Input: a single block to be sorted
		Function(Input):
			implement the pivot based qsort algorithm, part of the C library, on each unique block to be sorted
		Exit thread, join adjacent threads, and return to Merge Driver
	*/
}

//Note: const void *a and *b are void until a type is declared, so comparison functions for qsort are modular and can be modified depending on the type of data being passed to it
int compareFunc(const void *a, const void *b) {
    
    //cast key values to character pointer arrays from the record values passed to the comparison function
    char *aa = ((record*)a) -> key;
    char *bb = ((record*)b) -> key;
	
    //use strcmp function, returns 1 or -1
    return strcmp(aa, bb);


	/* Note on strcmp function:
		
		Compares two character arrays.
		Values returned:
				1	signifies that string 2 is less that string 1
				0	signifies that both strings are equivalent
			       -1	signifies that string 1 is less than string 2
	*/
}

void *Merge(void *param) {
	mergeparam *params = (mergeparam*)param;

	//checks which phase the sort is currently in
    int run = params -> x;
	int i;
	
	clock_t MergeDstart = clock();
	
	//run 0 is the sort phase
	if(run == 0) {

		//dynamic assignment of arrays for sort threads and parameters
        pthread_t ThreadID[2];
        sortparam spar[2];
        for(i = 0; i < 2; i++) {

			//initialize parameters for block being sorted
            if(i == 0) {
				spar[i].blockindex = params -> block1;
			}
			else {
				spar[i].blockindex = params -> block2;
			}
			spar[i].itm = params -> subitems; 

			//create two threads within the merge block, one for block 1 and the other for block 2

			if(pthread_create(&ThreadID[i], NULL, SortFunc, &spar[i])) {
                        	printf("error in creating sort thread.\n");
                        	exit(EXIT_FAILURE);
               		}
        }
        for(i = 0; i < 2; i++) {
			
            if(pthread_join(ThreadID[i], NULL)) {
                printf("error in joining sort thread.\n");
                exit(EXIT_FAILURE);
            }
        }
		
		//call a merge on the sorted blocks within the merge block
		mergeFunc(params);		
    }

	//for run 1 ... run n
    else {
		mergeFunc(params);
	}

	clock_t MergeDend = clock();
	
	printf("Total time in Merge Driver: %lu nanoseconds\n", MergeDend -  MergeDstart);
	
	pthread_exit(0);


	/* Algorithm for Merge Driver (Pseudo Code):
	
		Intput: parameters that contain two distinct blocks to be merged.
		Function(Input):
			run 0: sort phase, then merge sorted blocks.
			run 1 ... run n: merge blocks until file is completely sorted
		Exit thread, join adjacent threads in Main Function, and prepare for next run
	*/
}

void mergeFunc(mergeparam *params) {       
    //initializes the start indexes for both blocks being merged and the subitems within each block
    int first = params -> block1;
    int second = params -> block2;
    int subitem = params -> subitems;

    //index to start within original array when comparing, needs to be start index of first block
    int i = params -> block1;
    
    //loop while value of first is less that start index of block 2 and second is less than end of block 2
	clock_t MergeFstart = clock();
    while(first < params -> block2 && second < params -> block2 + subitem) {
        
        //compare the key values at each index within each separate block
        if(compareFunc((void*)&records[first], (void*)&records[second]) > 0) {
            temprecord[i] = records[second];
            second++;
            i++;
        }
        else {
            temprecord[i] = records[first];
            first++;
            i++;
        }
    }

    //next two while loops deal with left overs on each side
    while(first < params -> block2) {
        temprecord[i] = records[first];
        first++;
        i++;
    }
    while(second < params -> block2 + subitem) {
        temprecord[i] = records[second];
        second++;
        i++;
    }

	clock_t MergeFend = clock();

	printf("Total time in Merge Function loop: %lu nanoseconds\n", MergeFend - MergeFstart);

	
	/* Algorithm for Merge Function (Pseudo Code):

		Input:  parameters that contain the two distinct sorted blocks to be merged
		Function(Input):
			increment along each separate block 
				compare key values contained on the i index on each block
					depending on which index contains the lower key value, update the temporary array starting incrementally the starting index of the first block
			increment along both blocks separately while they still contain values not in the temporary array
				update temporary array with these values
		Return to Merge Driver 	
	*/
}
