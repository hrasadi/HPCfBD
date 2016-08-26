/*
 * HPCfBD Benchmark - COPYRIGHT 2016 IACS
 */


#define _GNU_SOURCE

#include<stdio.h>
#include<omp.h>
#include<string.h>
#include<stdlib.h> 
#include<regex.h>
#include<ctype.h>


int main(int argc, char **argv)
{
  double end, start = omp_get_wtime();
  int i, QuesCount = 0;
  double sum=0.0, Average=0.0;
  FILE *fp, *fpconf;
  char *rawline = NULL, *fileLine = NULL;
  char removeChar[] = {'\"', '<', '>'};
  size_t len = 0;
  ssize_t read;
  char * filename;

#pragma omp parallel /*shared(sum, QuesCount, rawline)*/ reduction (+:sum) reduction(+:QuesCount)
  {
#pragma omp master 
    {  
      fpconf = fopen(argv[1], "r");
      if (fpconf == NULL)
	{
	  printf( "Can't open input file : Data File !\n");
	  exit(1);
	}
      while ((read = getline(&rawline, &len, fpconf)) != -1)
	{
	  //char *line =  (char*) malloc(strlen(rawline) * sizeof(char));
	  char *line =  (char*) malloc(len);
	  strcpy(line, rawline);
#pragma omp task shared (sum, QuesCount) firstprivate(line)
	  {
	    double AnsCount =0;
	    int  PostTypeVal;
	    char *token;
	    if((token = strstr(line,  "PostTypeId")) != NULL){
	      if(token[12] == '1' && token[13] == '"' )
		{
		  if((token = strstr(line, "AnswerCount")) != NULL){
		    token[0] = token[13];
		    i = 13;
		    while(token[i] != '"')
		      i++;
		    token[i] = '\0';
		    sscanf(token, "%lf", &AnsCount);
		    #pragma omp critical
		    {
		      QuesCount++;
		      sum = sum + AnsCount;
		    }
		  }
		}
	    }
	  }//end task
    free(line);
	}
      #pragma omp taskwait

      Average = sum/QuesCount; 
      printf("Value of sum= %lf and average= %f, omp num threads is %d  \n", sum, Average,  omp_get_num_threads());	
      fclose(fpconf);
      end = omp_get_wtime();
      printf( " Total execution time = %lf \n",end - start);
    } //end master
  }//end parallel
  exit(EXIT_SUCCESS); 
}
