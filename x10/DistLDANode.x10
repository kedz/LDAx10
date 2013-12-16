import x10.array.Array_2;
import x10.util.ArrayList;
import x10.util.Random;
import x10.util.RailUtils;
import x10.util.Timer;
import x10.util.concurrent.Monitor;
import x10.util.concurrent.AtomicBoolean;
import x10.util.HashSet;
import x10.util.HashMap;

public class DistLDANode {

    val vocab:Vocabulary;

    var nodes:PlaceLocalHandle[DistLDANode];
    public val ntopics:Long;
    public val ntypes:Long;
    public val ndocs:Long;

    public val alpha:Double;
    public val alphaSum:Double;
    public val beta:Double;
    public val betaSum:Double;
    
    val nthreads:Long;
    public val workers:Rail[DLDAWorker];
    public val typeTopicWorldCounts:Array_2[Long];
    public val typeTopicWorldTotals:Rail[Long]; 

    public var typeTopicLocalCountsOld:Array_2[Long];
    public var typeTopicLocalTotalsOld:Rail[Long]; 
    
    public var typeTopicLocalCountsDelta:Array_2[Long];
    public var typeTopicLocalTotalsDelta:Rail[Long]; 

    public val visited:Rail[Boolean];    
    public val gSyncLock:Monitor = new Monitor();
    public val busy:AtomicBoolean = new AtomicBoolean(false); 

    val world:PlaceGroup.SimplePlaceGroup;
    public var exchanges:Rail[Long];

    var totalSampleTime:Long = 0;
    var totalResyncTime:Long = 0;    
    var totalTransmitTime:Long = 0;    

    public var done:Boolean = false;
    var talkingToWorld:Boolean = false;
    var accepting:Boolean = false;
    
    public val nodeIndicesMap:HashMap[Long,Long];

    public def this(world:PlaceGroup.SimplePlaceGroup,
                    vocab:Vocabulary, 
                    docsFrags:ArrayList[Rail[Documents.Document]],
                    ntopics:Long,
                    alphaSum:Double,
                    beta:Double,
                    nthreads:Long) {
    
            
        this.world = world;
        var ndocs:Long = 0;
        for (doc in docsFrags) ndocs += doc.size;
        this.ndocs = ndocs;
        this.ntopics = ntopics;
        this.alpha = alphaSum / ntopics;
        this.alphaSum = alphaSum;
        this.beta = beta;
        this.ntypes = vocab.size();
        this.betaSum = this.ntypes * this.beta;
        this.vocab = vocab;
        
        val localIndices = new HashMap[Long,Long](); 
        var cntr:Long = 0;
        for (docs in docsFrags)
            for (doc in docs)
                for (w in doc.words)
                    if (!localIndices.containsKey(w))
                        localIndices.put(w, cntr++);

        this.nodeIndicesMap = localIndices;
        

        this.nthreads = nthreads;
        this.typeTopicLocalCountsOld = new Array_2[Long](localIndices.size(), ntopics);
        this.typeTopicLocalTotalsOld = new Rail[Long](ntopics);
        this.typeTopicLocalCountsDelta = new Array_2[Long](localIndices.size(), ntopics);
        this.typeTopicLocalTotalsDelta = new Rail[Long](ntopics);

        this.typeTopicWorldCounts = new Array_2[Long](ntypes, ntopics); 
        this.typeTopicWorldTotals = new Rail[Long](ntopics);

        workers = new Rail[DLDAWorker](nthreads);
        for (var t:Long = 0; t < nthreads; t++)
            workers(t) = new DLDAWorker(vocab, docsFrags.get(t), ntopics, alpha, beta, this.betaSum, t, typeTopicWorldCounts, typeTopicWorldTotals, nodeIndicesMap);
        
        visited = new Rail[Boolean](world.numPlaces(), (i:Long) => true);
        exchanges = new Rail[Long](world.numPlaces());
    }

    public def setNodes(nodes:PlaceLocalHandle[DistLDANode]) {
        this.nodes = nodes;
    }

    public def toString():String {
        val repStr:String = "DistLDANode:::ntopics="+ntopics+"\n"
            + "           :::alpha="+alpha+"\n"
            + "           :::alphaSum="+alphaSum+"\n"
            + "           :::beta="+beta+"\n"
            + "           :::betaSum="+betaSum+"\n"
            + "           :::ndocs="+ndocs+"\n" 
            + "           :::ntypes="+ntypes;

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
                    //typeTopicLocalTotalsDelta(t) = totalWordsPerTopic;// - typeTopicLocalTotalsOld(t);                                   
                    val typeCounts:Rail[Long] = new Rail[Long](nodeIndicesMap.size()); 
                    for (worker in workers) {
                        for (w in worker.localIndicesMap.keySet()) {
                            val lindex = worker.localIndicesMap.get(w)();    
                            val nindex = nodeIndicesMap.get(w)();
                            typeCounts(nindex) += worker.typeTopicCountsLocal(lindex, t);
                        }
                    }
                    for (worker in workers) {
                        for (w in nodeIndicesMap.keySet()) {
                            val nindex = nodeIndicesMap.get(w)();
                            worker.typeTopicCountsGlobal(nindex,t) = typeCounts(nindex); 
                            if (worker.localIndicesMap.containsKey(w)) {
                                val lindex = worker.localIndicesMap.get(w)();    
                                worker.typeTopicCountsGlobal(nindex,t) -= worker.typeTopicCountsLocal(lindex, t);
                            }
                        }
                    }
                        
                       //typeTopicLocalCountsDelta(w,t) = typeCount;// - typeTopicLocalCountsOld(w,t);                                   

                }
            }
        }

    }



    public def sampleOneIteration() {

        val sampleStart:Long = Timer.milliTime();
        finish for (worker in workers) {
            async worker.oneSampleIteration(); 
        }
        totalSampleTime += Timer.milliTime() - sampleStart;

    }


    public def sample(niters:Long, localSyncRate:Long, globalSyncRate:Long) {
        
        // done flag is set to true when we have finished all of our work.
        done = false;
        
        // If we are busy, someone is writing to our global counts matrix -- wait for them to finish.
        while (!busy.compareAndSet(false,true)) {}

        for (var i:Long = 1; i <= niters; i++) {
            if (i%100 == 0) {
                Console.OUT.print(i+"{"+here+"} ");
                Console.OUT.flush();
            }

            val sampleStart:Long = Timer.milliTime();
            
            /** Sampling - must lock down all matrices **/
            finish for (worker in workers) {
                async worker.oneSampleIteration(); 
            }
            totalSampleTime += Timer.milliTime() - sampleStart;

            if (i % localSyncRate == 0) {
                val resyncStart:Long = Timer.milliTime();
                resync();
                totalResyncTime += Timer.milliTime() - resyncStart;
            }

            if (i % globalSyncRate == 0) {
                //Console.OUT.println(here+" Looking to resync..."); 
                //gSyncLock.unlock();
                val startTransmit = Timer.milliTime();
                var reset:Boolean = true;
                for (var p:Long = 0; p < world.numPlaces(); p++) 
                    if (!visited(p)) reset = false;
                if (reset) {
                    //Console.OUT.println("Reseting");
                    for (var p:Long = 0; p < world.numPlaces(); p++) {
                        if (p != here.id)
                            visited(p) = false;
                       
                    }
                    //Console.OUT.println(here+" : "+visited);
                    calculateNewCounts();    
                }
                //Console.OUT.println(here+" : Looking to share counts.");
                busy.set(false);
                shareCounts();
                totalTransmitTime += Timer.milliTime() - startTransmit;
                //Console.OUT.println(here+" Finished transmitting - looking to get lock.");
                //while (!gSyncLock.tryLock()) {}
                //Console.OUT.println(here+" Finished transmitting - got lock.");
                
            }
        }
        busy.set(false);
        done = true;
        Console.OUT.println();
    }

    private def calculateNewCounts() {
        val step:Long = (ntopics / nthreads) + 1;
        finish for (var topics:Long = 0; topics < ntopics; topics += step) {
            val topicVal = topics;
            val limit = Math.min(ntopics, topicVal+step);
            async {

                for (var t:Long = topicVal; t < limit; t++) {
                    
                    var totalWordsPerTopic:Long = 0;
                    for (worker in workers)
                        totalWordsPerTopic += worker.totalTypesPerTopicLocal(t);                    
                    typeTopicLocalTotalsOld(t) = typeTopicLocalTotalsDelta(t);
                    typeTopicLocalTotalsDelta(t) = totalWordsPerTopic;// - typeTopicLocalTotalsOld(t);                                   

                    for (w in nodeIndicesMap.keySet()) {
                        val nindex = nodeIndicesMap.get(w)();
                        var typeCount:Long = 0;
                        for (worker in workers) { 
                            if (worker.localIndicesMap.containsKey(w)) {
                                val lindex = worker.localIndicesMap.get(w)();
                                typeCount += worker.typeTopicCountsLocal(lindex,t);
                                
                            }
                        }
                        typeTopicLocalCountsOld(nindex,t) = typeTopicLocalCountsDelta(nindex,t);
                        typeTopicLocalCountsDelta(nindex,t) = typeCount;// - typeTopicLocalCountsOld(w,t);                                   
                    }

                }
            }
        }

    }

    public def shareCounts() {
        
        val nodesPlh = nodes;
        //accepting = true;
        
        var pIndex:Long = -1;
        
        // Need to reset visited
        // Need to safe guard lock;
        // need to implement lock
        // only update counts after visiting all places
        
        while (pIndex < 0) {
        
            for (var i:Long = 0; i < world.numPlaces(); i++) {
                if (!visited(i)) {
                    val hasLock:GlobalRef[Cell[Boolean]] = new GlobalRef[Cell[Boolean]](new Cell[Boolean](false));
                    //Console.OUT.println(here+ " : about to visit "+world(i));
                    at (world(i)) {
                        //if (nodes().accepting) {
                            //Console.OUT.println(nodes().gSyncLock.getHoldCount());
                            //val success = nodesPlh().gSyncLock.tryLock();
                            val success = nodesPlh().busy.compareAndSet(false,true);
                            //val success = true;
                            //Console.OUT.println(here+": "+success);
                            at (hasLock.home) {
                                hasLock()() = success;
                            }
                        //}
                    }
                    if (hasLock.getLocalOrCopy()())
                        pIndex = i; 
                }
                
            }
            /*
            if (pIndex == -1) {
                for (var i:Long = 0; i < world.numPlaces(); i++) visited(i) = false;
            }
            */
        }
        visited(pIndex) = true;
        exchanges(pIndex)++;

        val step:Long = (ntopics / nthreads)+1;
        //Console.OUT.println(here+" : Exchanging with "+world(pIndex));
        finish for (var topicStart:Long = 0; topicStart < ntopics; topicStart+=step) {
            //Console.OUT.println(here + " : TOPIC "+topic+" to "+world(pIndex));
            
            val topicStartVal = topicStart;
            val limit = Math.min(ntopics, topicStartVal+step);
            async {
                for (var topic:Long = topicStartVal; topic < limit; topic++) {
                    val t = topic;


                    val newC = typeTopicLocalTotalsDelta(t);
                    val oldC = typeTopicLocalTotalsOld(t);
                    val c = newC - oldC;
                        
                    if (c != 0) {
                        //Console.OUT.println(here + " :NONZERO TOPIC "+t+" to "+world(pIndex));
                        at (world(pIndex)) {
                            //val tHere = t;
                            //val cHere = c;
                            //val totals = nodesPlh().typeTopicWorldTotals;
                            //Console.OUT.println(here + " :NONZERO TOPIC "+t+" to transmitted.");
                            //Console.OUT.println(c);
                            //totals(tHere) += cHere;
                            nodesPlh().typeTopicWorldTotals(t) += c;
                        }
                    }

                    //Console.OUT.println(here + " transmitting topic "+t+" word updates...");
                    for (w in nodeIndicesMap.keySet()) {
                        val nindices = nodeIndicesMap.get(w)();
                        val word = w;
                        val newWC = typeTopicLocalCountsDelta(nindices,t);
                        val oldWC = typeTopicLocalCountsOld(nindices,t);
                        val wc = newWC - oldWC;
                        if (wc != 0) {                    
                            at (world(pIndex)) {
                                
                                nodesPlh().typeTopicWorldCounts(word,t) += wc;
                            }
                        }
                    }
                }
            }
        }

        at (world(pIndex)) {
            nodesPlh().busy.set(false);
            //nodesPlh().gSyncLock.unlock();
        }
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

    public def getTotalTransmitTime() : Long {
        return totalTransmitTime;
    }
    
}
