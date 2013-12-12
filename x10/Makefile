X10C=${X10_HOME}/bin/x10c++
FLAGS=-O

LDA: LDATester.x10 SerialLDA.x10 Vocabulary.x10 Documents.x10 MathUtils.x10
	$(X10C) $(FLAGS) -o $@ $^

PLDA: PLDATester.x10 ParallelLDA.x10 LDAWorker.x10 Vocabulary.x10 DocumentsFrags.x10
	$(X10C) $(FLAGS) -o $@ $^

test: LDA
	./LDA ../datasets/sample_data 1000 20 15

test-par: PLDA
	./PLDA ../datasets/sample_data 1000 20 25 2   

test-par-large: PLDA
	./PLDA ../datasets/ap_data 1000 20 25 8   

clean:
	rm -f LDA PLDA DLDA *.h *.out *.err *.log *~ *.cc
