import x10.array.Array_2;
import x10.util.ArrayList;
import x10.util.Random;
import x10.util.RailUtils;
import x10.util.Timer;

public class ParallelLDA {

    val vocab:Vocabulary;

    val ntopics:Long;
    val ntypes:Long;
    val ndocs:Long;

    val alpha:Double;
    val alphaSum:Double;
    val beta:Double;
    val betaSum:Double;
    
    val nthreads:Long;
    val workers:Rail[LDAWorker];

    var totalSampleTime:Long = 0;
    var totalResyncTime:Long = 0;    

    public def this(vocab:Vocabulary, docsFrags:ArrayList[Rail[Documents.Document]], ntopics:Long, alphaSum:Double, beta:Double, nthreads:Long) {
    
        this.ndocs = docsFrags.size();
        this.ntopics = ntopics;
        this.alpha = alphaSum / ntopics;
        this.alphaSum = alphaSum;
        this.beta = beta;
        this.ntypes = vocab.size();
        this.betaSum = this.ntypes * this.beta;
        this.vocab = vocab;
        this.nthreads = nthreads;
        
        workers = new Rail[LDAWorker](nthreads);
        for (var t:Long = 0; t < nthreads; t++)
            workers(t) = new LDAWorker(vocab, docsFrags.get(t), ntopics, alpha, beta, this.betaSum, t);

    }

    public def toString():String {
        val repStr:String = "SerialLDA:::ntopics="+ntopics+"\n"
            + "         :::alpha="+alpha+"\n"
            + "         :::alphaSum="+alphaSum+"\n"
            + "         :::beta="+beta+"\n"
            + "         :::betaSum="+betaSum+"\n"
            + "         :::ndocs="+ndocs+"\n" 
            + "         :::ntypes="+ntypes;

        return repStr;            
    }


    public def printReport() {

        Console.OUT.println(toString());
        Console.OUT.println();
        Console.OUT.println("LDAWorker sound off...\n");
        finish for (worker in workers)
            async Console.OUT.println(worker+"\n");
    }

    public def init() {

        finish for (worker in workers)
            async worker.initLocal();

        resync();

    }

    public def shareCounts() {

        for (var i:Long = 0; i < workers.size; i++) {
            for (var j:Long = i+1; j < workers.size; j++) {
                workers(i).addGlobalCounts(workers(j).getLocalTypeTopicMatrix(),
                                           workers(j).getLocalTypesPerTopicCounts());
                workers(j).addGlobalCounts(workers(i).getLocalTypeTopicMatrix(),
                                           workers(i).getLocalTypesPerTopicCounts());
                    

            }

        }
        

    }

    public def resync() {
       
        val step:Long = (ntopics / nthreads) + 1;
        finish for (var topics:Long = 0; topics < ntopics; topics += step) {
            val topicVal = topics;
            val limit = Math.min(ntopics, topicVal+step);
            async {

                for (var t:Long = topicVal; t < limit; t++) {
                    
                    var totalWordsPerTopic:Long = 0;
                    for (worker in workers)
                        totalWordsPerTopic += worker.totalTypesPerTopicLocal(t);                    
                    for (worker in workers)
                        worker.totalTypesPerTopicGlobal(t) = totalWordsPerTopic - worker.totalTypesPerTopicLocal(t);
                
                    for (var w:Long = 0; w < ntypes; w++) {
                        var typeCount:Long = 0;
                        for (worker in workers) 
                            typeCount += worker.typeTopicCountsLocal(w,t);
                        for (worker in workers)
                            worker.typeTopicCountsGlobal(w,t) = typeCount - worker.typeTopicCountsLocal(w,t);
                    }

                }
            }
        }

    }

    public def sample(niters:Long) {

        for (var i:Long = 1; i <= niters; i++) {
            if (i%100 == 0) 
                Console.OUT.print(i);
            else
                Console.OUT.print(".");
            Console.OUT.flush();

            val sampleStart:Long = Timer.milliTime();
            finish for (worker in workers) {
                async worker.oneSampleIteration(); 
            }
            totalSampleTime += Timer.milliTime() - sampleStart;
            Console.OUT.print("*");
            Console.OUT.flush();

            if (i % 2 == 0) {
                val resyncStart:Long = Timer.milliTime();
                Console.OUT.print("r");
                Console.OUT.flush();
                resync();
                Console.OUT.print("R");
                Console.OUT.flush();
                totalResyncTime += Timer.milliTime() - resyncStart;
            }
        }
        Console.OUT.println();
    }

    public def displayTopWords(topn:Long, topic:Long) {
        workers(0).displayTopWords(topn, topic);
    }

    public def getTotalSampleTime() : Long {
        return totalSampleTime;
    }

    public def getTotalResyncTime() : Long {
        return totalResyncTime;
    }

}
