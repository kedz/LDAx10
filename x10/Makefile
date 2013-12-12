X10C=${X10_HOME}/bin/x10c++
FLAGS=-O

LDA: LDATester.x10 SerialLDA.x10 Vocabulary.x10 Documents.x10
	$(X10C) $(FLAGS) -o $@ $^

PLDA: PLDATester.x10 DocumentsFrags.x10
	$(X10C) $(FLAGS) -o $@ $^

test: LDA
	./LDA ../datasets/sample_data 1000 20

test-par: PLDA
	./PLDA ../datasets/sample_data 1000 20    


clean:
	rm -f LDA *.h *.out *.err *.log *~ *.cc
