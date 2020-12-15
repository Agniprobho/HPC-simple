#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <string.h>
#include <sys/time.h>
#include <stdint.h>
#include "sys/types.h"
#include "sys/sysinfo.h"

#define maxwords 50000
#define maxlines 1000000

double myclock();
int my_PE_num, nThread, nNodes, nCores;
int nwords, nlines;
double tstart, ttotal;

char word[maxwords][10];
char line[maxlines][2001];

int local_count[maxwords];
int global_count[maxwords];
int local_lines[maxwords][100];
int global_lines[maxwords][100];

/******************************************************************************/
/*
*	https://hpcf.umbc.edu/general-productivity/checking-memory-usage/
* 	Look for lines in the procfile contents like: 
* 	VmRSS:         5560 kB
* 	VmSize:         5560 kB
*
* 	Grab the number between the whitespace and the "kB"
* 	If 1 is returned in the end, there was a serious problem (we could not find one of the memory usages)
*/
int get_memory_usage_kb(long* vmrss_kb, long* vmsize_kb)
{
    /* Get the the current process' status file from the proc filesystem */
    FILE* procfile = fopen("/proc/self/status", "r");

    long to_read = 8192;
    char buffer[to_read];
    int read = fread(buffer, sizeof(char), to_read, procfile);
    fclose(procfile);

    short found_vmrss = 0;
    short found_vmsize = 0;
    char* search_result;

    /* Look through proc status contents line by line */
    char delims[] = "\n";
    char* line = strtok(buffer, delims);

    while (line != NULL && (found_vmrss == 0 || found_vmsize == 0) )
    {
        search_result = strstr(line, "VmRSS:");
        if (search_result != NULL)
        {
            sscanf(line, "%*s %ld", vmrss_kb);
            found_vmrss = 1;
        }

        search_result = strstr(line, "VmSize:");
        if (search_result != NULL)
        {
            sscanf(line, "%*s %ld", vmsize_kb);
            found_vmsize = 1;
        }

        line = strtok(NULL, delims);
    }

    return (found_vmrss == 1 && found_vmsize == 1) ? 0 : 1;
}

int get_cluster_memory_usage_kb(long* vmrss_per_process, long* vmsize_per_process, int root, int np)
{
    long vmrss_kb;
    long vmsize_kb;
    int ret_code = get_memory_usage_kb(&vmrss_kb, &vmsize_kb);

    if (ret_code != 0)
    {
        printf("Could not gather memory usage!\n");
        return ret_code;
    }

    MPI_Gather(&vmrss_kb, 1, MPI_UNSIGNED_LONG, vmrss_per_process, 1, MPI_UNSIGNED_LONG, root, MPI_COMM_WORLD);
    MPI_Gather(&vmsize_kb, 1, MPI_UNSIGNED_LONG, vmsize_per_process, 1, MPI_UNSIGNED_LONG, root, MPI_COMM_WORLD);

    return 0;
}

int get_global_memory_usage_kb(long* global_vmrss, long* global_vmsize, int np)
{
    long vmrss_per_process[np];
    long vmsize_per_process[np];
    int ret_code = get_cluster_memory_usage_kb(vmrss_per_process, vmsize_per_process, 0, np);

    if (ret_code != 0)
    {
        return ret_code;
    }

    *global_vmrss = 0;
    *global_vmsize = 0;
    for (int i = 0; i < np; i++)
    {
        *global_vmrss += vmrss_per_process[i];
        *global_vmsize += vmsize_per_process[i];
    }

    return 0;
}
/*****************************************************************************/

void init_arrays()
{
   int i, j, err;
   double nchars = 0;
   FILE *fd;
   
   for( i = 0; i < maxwords; i++ ) 
   {
      global_count[i] = 0;
	  for (j=0; j<100; j++)
		  global_lines[i][j]=0;
   }
   
   // Read in the dictionary words
   fd = fopen( "/homes/dan/625/keywords.txt", "r" );	///scratch/dan/words_4-8chars_50k
   nwords = -1;
   do {
      err = fscanf( fd, "%[^\n]\n", word[++nwords] );
   } while( err != EOF && nwords < maxwords - 1 );
   fclose( fd );
   printf( "Read in %d words\n", nwords);

   // Read in the lines from the data file
   fd = fopen( "/homes/dan/625/wiki_dump.txt", "r" );
   nlines = -1;
   do {
      err = fscanf( fd, "%[^\n]\n", line[++nlines] );
      if( line[nlines] != NULL ) nchars += (double) strlen( line[nlines] );
   } while( err != EOF && nlines < maxlines - 1);
   fclose( fd );
   printf( "Read in %d lines averaging %.0lf chars/line\n", nlines, nchars / nlines);
}

void *countw(void *rank)
{
   int i, k, startPos, endPos, check;
   int myID = *((int*)rank);
   int ratio = (maxwords-1)/nThread;
   
   startPos = (myID)*ratio;
   endPos = startPos + ratio;
   
   check = (nThread) * ratio;	//to check if the end position of last thread actually goes upto the last step, due to integer round off error

   if (endPos==check && endPos != (maxwords-1))
		endPos = (maxwords-1);
	
   printf("myID = %d startPos = %d endPos = %d \n", myID, startPos, endPos); fflush(stdout);
	
   for( i = 0; i < maxwords-1; i++ ) 
   {
        local_count[i]=0;
		for (k=0; k<100; k++)
			local_lines[i][k]=0;
   }

   for( k = startPos; k < endPos; k++ ) 
   {
      for( i = 0; i < maxlines-1; i++ ) 
      {
         if( strstr( line[i], word[k] ) != NULL ) 
		 {
			 local_count[k]++;
			 if (local_count[k]<=100)
				local_lines[k][local_count[k]-1]=i+1;
		 }
      }
   }
}

void print_results()
{
	// Dump out the word counts
	FILE *fd;
	char filename[10];
	int i, j;
	ttotal = myclock() - tstart;
	
	sprintf(filename,"wiki%d%d.out",nNodes,nCores);
	fd = fopen( filename, "w" );
   	for( i = 0; i < nwords; i++ ) 
	{
      	fprintf( fd, "%d %s: %d:", i, word[i], global_count[i] );
		for (j=0; j<100; j++)
		{
			if (global_lines[i][j]!=0 && j==0)
				fprintf( fd, " %d", global_lines[i][j] );
			else if (global_lines[i][j]!=0 && j!=0)
				fprintf( fd, ",%d", global_lines[i][j] );
			else 
				break;
		}
		fprintf( fd, "\n");
   	}
   	fprintf( fd, "The run on %d cores took %lf seconds for %d words\n", nThread, ttotal, nwords);
   	fclose( fd );
}

int main(int argc, char** argv)
{
   int i, j, k, n, err;
   int rc, numprocs, my_PE_num;
   MPI_Status status;
   //processMem_t myMem;
   long global_vmrss, global_vmsize;
   
   /*word = (char **) malloc( maxwords * sizeof( char * ) );
   for( i = 0; i < maxwords; i++ ) 
   {
      word[i] = malloc( 10 );
   }

   line = (char **) malloc( maxlines * sizeof( char * ) );
   for( i = 0; i < maxlines; i++ ) 
   {
      line[i] = malloc( 2001 );
   }*/

   // Loop over the word list   
   rc = MPI_Init(&argc,&argv);
   if (rc != MPI_SUCCESS) 
   {
	printf ("Error starting MPI program. Terminating.\n");
        MPI_Abort(MPI_COMM_WORLD, rc);
   }
   MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
   MPI_Comm_rank(MPI_COMM_WORLD, &my_PE_num);
   
   nThread = numprocs;
   nNodes = atoi(argv[1]);
   nCores = atoi(argv[2]);
   
   long vmrss_per_process[nThread];
   long vmsize_per_process[nThread];
   
   if (my_PE_num==0) 
   {
	  tstart = myclock();  // Set the zero time
	  tstart = myclock();  // Start the clock
	  init_arrays();
   }

   /*for (i=0; i<maxwords-1; i++)
   	MPI_Bcast(&(word[i][0]), 10 , MPI_CHAR , 0 , MPI_COMM_WORLD);
   for (i=0; i<maxlines-1; i++)
   	MPI_Bcast(&(line[i][0]), 2001 , MPI_CHAR , 0 , MPI_COMM_WORLD);*/
   MPI_Bcast(word, (maxwords-1)*10 , MPI_CHAR , 0 , MPI_COMM_WORLD);
   MPI_Bcast(word, (maxlines-1)*2001 , MPI_CHAR , 0 , MPI_COMM_WORLD);
   countw(&my_PE_num);

   MPI_Reduce(local_count, global_count, maxwords, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
   MPI_Reduce(local_lines, global_lines, maxwords*100, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
   
   get_cluster_memory_usage_kb(vmrss_per_process, vmsize_per_process, 0, nThread);
   if (my_PE_num==0) 
   {
	  for (int k = 0; k < nThread; k++)
      {
          printf("Process %02d: VmRSS = %6ld KB, VmSize = %6ld KB\n", k, vmrss_per_process[k], vmsize_per_process[k]);
      }
   }
   get_global_memory_usage_kb(&global_vmrss, &global_vmsize, nThread);
   if (my_PE_num==0)
   {
	  //GetProcessMemory(&myMem); 
   	  print_results();
	  printf("The run on %d cores took %lf seconds for %d words\n", nThread, ttotal, nwords);
	  printf("DATA, %d, %f, %d, %d, Memory: VmRSS = %6ld KB, VmSize = %6ld KB\n", nThread, ttotal, nNodes, nCores, global_vmrss, global_vmsize);
	  //printf("DATA, %d, %f, %d, %d\n", nThread, ttotal, nNodes, nCores);
   }

   MPI_Finalize();
   return 0;
}

double myclock() 
{
   static time_t t_start = 0;  // Save and subtract off each time

   struct timespec ts;
   clock_gettime(CLOCK_REALTIME, &ts);
   if( t_start == 0 ) t_start = ts.tv_sec;

   return (double) (ts.tv_sec - t_start) + ts.tv_nsec * 1.0e-9;
}
