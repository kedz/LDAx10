X10C=${X10_HOME}/bin/x10c++
FLAGS=-O

LDA: Main.x10 WordIndexMap.x10
	$(X10C) $(FLAGS) -o $@ $^

test: LDA
	./LDA

clean:
	rm -f LDA *.h *.out *.err *.log *~ *.cc
