/*
 * HPCfBD Benchmark - COPYRIGHT 2016 IACS
 */

#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

void readlines(MPI_File *in, const int rank, const int size, const int overlap,
               char ***lines, int *nlines) {
    MPI_Offset filesize;
    MPI_Offset localsize;
    MPI_Offset start;
    MPI_Offset end;
    char *chunk;

    /* figure out who reads what */
    
    MPI_File_get_size(*in, &filesize);
    localsize =  filesize / size;
    start = rank * localsize;
    end   = start + localsize - 1;
    /* add overlap to the end of everyone's chunk... */
    end += overlap;

    /* except the last processor, of course */
    if (rank == size-1) end = filesize;

    localsize =  end - start + 1;
    
    /* allocate memory */
    chunk = malloc( (localsize + 1)*sizeof(char));

    /* everyone reads in their part */
    MPI_File_read_at_all(*in, start, chunk, localsize, MPI_CHAR, MPI_STATUS_IGNORE);
    chunk[localsize] = '\0';

    /*
     * everyone calculate what their start and end *really* are by going 
     * from the first newline after start to the first newline after the
     * overlap region starts (eg, after end - overlap + 1)
     */

    MPI_Offset locstart=0, locend=localsize;
    if (rank != 0) {
        while(chunk[locstart] != '\n') locstart++;
        locstart++;
    }
    if (rank != size-1) {
        locend-=overlap;
        while(chunk[locend] != '\n') locend++;
    }
    localsize = locend-locstart+1;
    
    /* Now let's copy our actual data over into a new array, with no overlaps */
    char *data = (char *)malloc((localsize+1)*sizeof(char));
    memcpy(data, &(chunk[locstart]), localsize);
    data[localsize] = '\0';
    free(chunk);
    
    /* Now we'll count the number of lines */
    *nlines = 0;
    for (int i=0; i<localsize; i++)
	if (data[i] == '\n') (*nlines)++;

    /* Now the array lines will point into the data array at the start of each line */
    /* assuming nlines > 1 */
    *lines = (char **)malloc((*nlines)*sizeof(char *));
    (*lines)[0] = strtok(data,"\n");
    for (int i=1; i<(*nlines); i++)
	(*lines)[i] = strtok(NULL, "\n");

    return;
}

void processlines(char **lines, const int nlines, const int rank) {
    
    int tm = 0, tm1, h, sum = 0, l, res, quesCount = 0, ans_index;
    double ave;
    char* token;
    char* p;
    char* post_add;
    char* ans_add;
    char post[15] = "PostTypeId=\"1\"";
    char ans[14] = "AnswerCount=\"";
    int length;
    
    for (int i=0; i<nlines; i++) {
	
	token = strtok(lines[i], "\n");
	while(token != NULL){
	    length = strlen(token);
	    p = (char* )malloc((length + 1) * sizeof(char));
	    strcpy(p, token);
	    
	    post_add = strstr(p, post);
	    if(post_add != NULL){
		
		ans_add = strstr(p, ans);
		if(ans_add != NULL){
		    quesCount++;
		    ans_index = (int)(&(ans_add[0]) - &(p[0])) + 13;
		    res = 0;
		    for(l = ans_index; p[l] != '"'; l++){
			res = 10 * res + (p[l] - 48);
		    }
		    
		    sum += res;
		}
	    }
	    
	    free(p);
	    token = strtok(NULL, "\n");
	} 
    }
    
    int sum_all, ques_all;
    MPI_Reduce(&quesCount, &ques_all, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&sum, &sum_all, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    
    if(rank == 0){
	double ave = (sum_all * 1.0) / ques_all;
	printf("Sum: %d and Average: %lf\n", sum_all, ave);
    }   
}

int main(int argc, char **argv) {
    
    MPI_File in;
    int rank, size;
    int ierr;
    char name[MPI_MAX_PROCESSOR_NAME];
    int resultlen;
    FILE *fpconf;
    
    double t1, t2, t_all, t; 
    
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_processor_name( name, &resultlen );

    t1 = MPI_Wtime();
    
    ierr = MPI_File_open(MPI_COMM_WORLD, argv[1], MPI_MODE_RDONLY, MPI_INFO_NULL, &in);
    
    if (ierr) {
	fprintf(stderr, "%d, %s: Couldn't open file %s\n\n", rank, argv[1], name);
	//if (rank == 0)
	//	fprintf(stderr, "%d, %s: Couldn't open file %s\n\n", rank, argv[0], argv[1]);
        MPI_Finalize();
	exit(2);
    }
    
    const int overlap=5000;
    char **lines;
    int nlines;
    readlines(&in, rank, size, overlap, &lines, &nlines);
    
    processlines(lines, nlines, rank);

    free(lines[0]);
    free(lines);

    MPI_File_close(&in);
    t2 = MPI_Wtime();
    t_all= t2-t1;
    
    MPI_Reduce(&t_all, &t, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    
    if (rank == 0){
    	printf( "Elapsed time is %f\n", t/size ); 
    }
    
    MPI_Finalize();
    return 0;
}
