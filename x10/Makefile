X10C=${X10_HOME}/bin/x10c++
FLAGS=-O -VERBOSE_CHECKS

test: LDA PLDA DLDA test-ser-large test-par-large test-dist-large

LDA: LDATester.x10 SerialLDA.x10 Vocabulary.x10 Documents.x10 MathUtils.x10
	$(X10C) $(FLAGS) -o $@ $^

PLDA: PLDATester.x10 ParallelLDA.x10 LDAWorker.x10 Vocabulary.x10 Documents.x10 MathUtils.x10
	$(X10C) $(FLAGS) -o $@ $^

DLDA: DLDATester.x10 DistLDANode.x10 DLDAWorker.x10 Vocabulary.x10 Documents.x10 MathUtils.x10
	$(X10C) $(FLAGS) -o $@ $^

IOTEST: IOTester.x10 Vocabulary.x10 Documents.x10 DocumentsFrags.x10
	$(X10C) $(FLAGS) -o $@ $^

test-ser: LDA
	./LDA ../datasets/sample_data 1000 20 20

test-ser-large: LDA
	./LDA ../datasets/ap_data 1000 100 20

test-par: PLDA
	./PLDA ../datasets/sample_data 1000 20 15 2   

test-par-large: PLDA
	./PLDA ../datasets/ap_data 1000 100 20 16 10

test-dist: DLDA
	./DLDA ../datasets/sample_data 1000 20 15 2 2 2 5 

test-dist-large: DLDA
	./DLDA ../datasets/ap_data 1000 100 20 16 8 10 100

test-io: IOTEST 
	./IOTEST ../datasets/ap_data 1000 20 15 8 4  

clean:
	rm -f LDA PLDA DLDA IOTEST *.h *.out *.err *.log *~ *.cc
