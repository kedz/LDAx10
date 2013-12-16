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

    val syncRate:Long;

    public def this(vocab:Vocabulary, docsFrags:ArrayList[Rail[Documents.Document]], ntopics:Long, alphaSum:Double, beta:Double, nthreads:Long, syncRate:Long) {
   
        var ndocs:Long = 0;
        for (docs in docsFrags) ndocs += docs.size; 
        this.ndocs = ndocs;
        this.ntopics = ntopics;
        this.alpha = alphaSum / ntopics;
        this.alphaSum = alphaSum;
        this.beta = beta;
        this.ntypes = vocab.size();
        this.betaSum = this.ntypes * this.beta;
        this.vocab = vocab;
        this.nthreads = nthreads;
        this.syncRate = syncRate;

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
                    
                    
                    val typeCounts:Rail[Long] = new Rail[Long](ntypes); 
                    for (worker in workers) {
                        for (w in worker.localIndicesMap.keySet()) {
                            val lindex = worker.localIndicesMap.get(w)();    
                            typeCounts(w) += worker.typeTopicCountsLocal(lindex, t);
                            
                        }
                        
                    }
                    for (worker in workers) {
                        for (var w:Long = 0; w < ntypes; w++) {
                            worker.typeTopicCountsGlobal(w,t) = typeCounts(w); 
                            if (worker.localIndicesMap.containsKey(w)) {
                                val lindex = worker.localIndicesMap.get(w)();    
                                worker.typeTopicCountsGlobal(w,t) -= worker.typeTopicCountsLocal(lindex, t);

                            }
                        }
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

            if (i % syncRate == 0) {
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

    public def logLikelihood() : Double {
    
        var logLikelihood:Double = 0.0;
        for (worker in workers) {

            for (var d:Long = 0; d < worker.ndocs; d++) {
                for (var t:Long = 0; t < ntopics; t++) {
                    if (worker.docTopicCounts(d,t) > 0) {
                        logLikelihood += (MathUtils.logGamma(alpha + worker.docTopicCounts(d,t)))
                                            - MathUtils.logGamma(alpha);
                    }
                }
                logLikelihood -= MathUtils.logGamma(alphaSum + worker.docs(d).size);

            }

        }

        logLikelihood += ndocs * MathUtils.logGamma(alphaSum);

        var nonZeroTypeTopics:Long = 0;
        for (var w:Long = 0; w < ntypes; w++) {
            for (var t:Long = 0; t < ntopics; t++) {
                var locCount:Long = 0;
                if (workers(0).localIndicesMap.containsKey(w)){
                    val lindex = workers(0).localIndicesMap.get(w)();
                    locCount = workers(0).typeTopicCountsLocal(lindex,t);
                }
                if (locCount + workers(0).typeTopicCountsGlobal(w,t) == 0) continue;
                nonZeroTypeTopics++;
                logLikelihood += MathUtils.logGamma(beta + locCount + workers(0).typeTopicCountsGlobal(w,t));
            }
        }

        for (var t:Long = 0; t < ntopics; t++) {
            logLikelihood -= MathUtils.logGamma((beta * ntopics) + workers(0).totalTypesPerTopicLocal(t) + workers(0).totalTypesPerTopicGlobal(t));
        }

        logLikelihood += MathUtils.logGamma(beta * ntopics) - (MathUtils.logGamma(beta)* nonZeroTypeTopics);

        return logLikelihood;
    }



}
