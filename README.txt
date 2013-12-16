LDAx10
======

Latent Dirichlet Allocation implemented in x10.

in the x10 directory: 
>> make test

All results in the paper were obtained from running make test.
This will run the serial, parallel, and distributed lda programs.

MAKE SURE X10_NTHREADS and X10_NPLACES are set!

Program parameters
>> ./LDA DATA_DIRECTORY SAMPLE_ITERATIONS NUMBER_OF_TOPICS TOP_N_WORDS
>> ./PLDA DATA_DIRECTORY SAMPLE_ITERATIONS NUMBER_OF_TOPICS TOP_N_WORDS NUM_THREADS SYNC_RATE 
>> ./DLDA DATA_DIRECTORY SAMPLE_ITERATIONS NUMBER_OF_TOPICS TOP_N_WORDS NUM_THREADS NUM_PLACES LOCAL_SYNC_RATE GLOBAL_SYNC_RATE

DATA_DIRECTORY - location of text files
SAMPLE_ITERATIONS - number of sampling iterations.
TOP_N_WORDS - is the number of words to show for each topic when finised sampling.
NUMBER_OF_TOPICS - how many topics we think might best represent this corpus.
NUM_THREADS - number of worker threads in each place
NUM_PLACES - number of places to use.
SYNC_RATE/LOCAL_SYNC_RATE - interval for syncing word counts between threads.
GLOBAL_SYNC_RATE - interval for syncing word counts between places.
