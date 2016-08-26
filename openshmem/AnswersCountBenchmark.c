#include <stdio.h>
#include <shmem.h>
#include <stdlib.h>
#include <string.h>

/*
    First argument passed when running the command for execution
    is the number of lines in the file.
 */

int main(int argc, char* argv[]){
    
    if(argc != 2){
        printf("Number of lines in a file not passed as a second argument\n");
    }
    
    int rank, size, j, k, i = 0;

    //number of lines in the file
    int num_line = atoi(argv[1]);
    
    char* data[num_line];
    int sizes[num_line];
    int g;
    for(g = 0; g < num_line; g++){
        data[g] = NULL;
	sizes[g] = -1;
    }

    char* data1;
    int nm;
   
    shmem_init();

    char* loc_data1;
    char* loc_data;
    int* cn1 = (int *)shmalloc(sizeof(int));
    *cn1 = 0;
    rank = shmem_my_pe();
    size = shmem_n_pes();
    
    int z_pe[size-1];
    int cn_pe[size-1];

    if(rank == 0){

        FILE* fp = fopen("test.txt", "r");
        
	    if(fp == NULL){
            printf("Cannot open the file\n");
	        exit(-1);
  	    }

        size_t len = 0;
        char* line = NULL;
        ssize_t check;
        
        while(check = getline(&line, &len, fp) != -1){
        
	        sizes[i] = strlen(line);
            data[i] = line;
	        i++;
	        len = 0;
	        line = NULL;
        }
        
        long long int temp = 0;
        for(j = 0; j < i; j++){
            temp += sizes[j];
	    } 

        data1 = malloc((temp+1)*sizeof(char));
        for(j = 0; j < i; j++){
            strcat(data1, data[j]);
        }

	    nm = i / size;
	    int cn = 0;
	    for(k = 0; k < nm; k++){
            cn += sizes[k];
	    }
        
	    loc_data = malloc((cn + 1)*sizeof(char));
	    int t;
	    for(t = 0; t < cn; t++)
	        loc_data[t] = data1[t];
	    loc_data[t] = '\0';

       
	    int z = cn;
	    cn = 0;
	    for(j = 1; j < size; j++){

	        for(k = 0; k < nm; k++){
                cn += sizes[nm * j + k];
	        }
            
	    cn_pe[j-1] = cn;
	    shmem_int_put(cn1, &cn, 1, j);
	    z_pe[j-1] = z;
	    z += cn;
	    cn = 0;

	    }
    }
    
    shmem_barrier_all();
    loc_data1 = (char *)shmalloc(((*cn1)+1)*sizeof(char));
	    
	shmem_barrier_all();

	if(rank == 0){

	    int d;
	    char* temp2 = malloc(sizeof(char));
	    for(d = 1; d < size; d++){
		    temp2 = &(data1[z_pe[d-1]]);
            shmem_putmem(loc_data1, temp2, cn_pe[d-1], d);
	    }
	}
	
 
    int tm = 0, tm1, h, l, res, ans_index;
    int* sum = (int *)shmalloc(sizeof(int));
    int* quesCount = (int *)shmalloc(sizeof(int));
    *sum = 0;
    *quesCount = 0;
    double ave;
    char* token;
    char* p;
    char* post_add;
    char* ans_add;
    char post[15] = "PostTypeId=\"1\"";
    char ans[14] = "AnswerCount=\"";
    int length;

    if(rank == 0){
        token = strtok(loc_data, "\n");
    }
    else{
        token = strtok(loc_data1, "\n");
    }

    while(token != NULL){

        length = strlen(token);
	    p = (char* )malloc((length + 1) * sizeof(char));
	    strcpy(p, token);

	    post_add = strstr(p, post);
	    if(post_add != NULL){

	    ans_add = strstr(p, ans);
	    if(ans_add != NULL){
		    
            *quesCount += 1;
            ans_index = (int)(&(ans_add[0]) - &(p[0])) + 13;
	        res = 0;
		    for(l = ans_index; p[l] != '"'; l++){
                res = 10 * res + (p[l] - 48);
		    }

		*sum += res;

	    }
	}

	    free(p);
        token = strtok(NULL, "\n");
    }
    
   // printf("RANK: %d and sum = %d and quescount = %d\n", rank, *sum, *quesCount);
    shmem_barrier_all();
   
    if(rank == 0){

	int sum_all = 0;
	int ques_all = 0;
	int target;
	int b;
	for(b = 1; b < size; b++){

        shmem_int_get(&target, sum, 1, b);
	    sum_all += target;
	    shmem_int_get(&target, quesCount, 1, b);
	    ques_all += target;

        }

        sum_all += *sum;
	    ques_all += *quesCount;
    

        double ave = (sum_all * 1.0) / ques_all;
	    printf("Sum: %d and Average: %lf\n", sum_all, ave);
    }

    shmem_barrier_all();


}
