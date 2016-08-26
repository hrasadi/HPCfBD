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
    localsize = filesize/size;
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

    int locstart=0, locend=localsize;
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
    int i;
    for (i=0; i<localsize; i++)
        if (data[i] == '\n') (*nlines)++;

    /* Now the array lines will point into the data array at the start of each line */
    /* assuming nlines > 1 */
    *lines = (char **)malloc((*nlines)*sizeof(char *));
    (*lines)[0] = strtok(data,"\n");
    for (i=1; i<(*nlines); i++)
        (*lines)[i] = strtok(NULL, "\n");

    return;
}


int main(int argc, char **argv) {

    MPI_File in;
    int rank, size;
    int ierr;
    double startTime, endTime;

    MPI_Init(&argc, &argv);
    startTime = MPI_Wtime();

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    ierr = MPI_File_open(MPI_COMM_WORLD, argv[1], MPI_MODE_RDONLY, MPI_INFO_NULL, &in);
    if (ierr) {
        if (rank == 0) fprintf(stderr, "%s: Couldn't open file %s\n Error code:%d\n", argv[0], argv[1], ierr);
        MPI_Finalize();
        exit(2);
    }

    const int overlap=1000;
    char **lines;
    int nlines;
    readlines(&in, rank, size, overlap, &lines, &nlines);

//processlines(lines, nlines, rank);

    free(lines[0]);
    free(lines);

    MPI_File_close(&in);

    endTime = MPI_Wtime();

    printf("RunningTime: %f s \n", endTime - startTime);

    MPI_Finalize();
    return 0;
}
