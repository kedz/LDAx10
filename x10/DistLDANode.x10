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
    
    // PLH for communicating to nodes at other places
    var nodes:PlaceLocalHandle[DistLDANode];
    
    // number of topics
    public val ntopics:Long;

    //number of word types
    public val ntypes:Long;
    // number of docs in this node
    public val ndocs:Long;

    // smoothing parameters
    public val alpha:Double;
    public val alphaSum:Double;
    public val beta:Double;
    public val betaSum:Double;
    
    //number of threads for this node to use
    val nthreads:Long;
    // Each worker runs on its own thread
    public val workers:Rail[DLDAWorker];

    // typeTopicCount matrix for the rest of the world(other node's counts)
    // This is initially a 0 matrix, but will get added to when another node
    // sends us its counts
    public val typeTopicWorldCounts:Array_2[Long];
   
    // total words assigned to each topic for the world(other node's counts)
    // We can calculate this from typeTopicWorldCounts but it is faster to precompute and store.
    public val typeTopicWorldTotals:Rail[Long]; 


    // When sharing counts with another node, if we have already shared counts with this node,
    // we must subtract the last counts we gave before adding new ones. These old counts are stored here.
    public var typeTopicLocalCountsOld:Array_2[Long];
    public var typeTopicLocalTotalsOld:Rail[Long]; 
   
    // The counts are the latest node state that we are sharing with another node. 
    public var typeTopicLocalCountsDelta:Array_2[Long];
    public var typeTopicLocalTotalsDelta:Rail[Long]; 

    // Track which places we have visited
    public val visited:Rail[Boolean];    
    // Boolean flag for locking this node when sampling/sharing counts
    public val busy:AtomicBoolean = new AtomicBoolean(false); 
    
    // known places
    val world:PlaceGroup.SimplePlaceGroup;

    //Counts for the number of times we have shared to each node
    public var exchanges:Rail[Long];

    var totalSampleTime:Long = 0;
    var totalResyncTime:Long = 0;    
    var totalTransmitTime:Long = 0;    

    // true when all sampling finished
    public var done:Boolean = false;
    
    // not all of the words in the vocabulary will exist in this node
    // our count matrices can be smaller than the whole matrix -- this is
    // a mapy from a global vocab index to the node local vocab index.
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

    // Sync counts between threads
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
            
            /** Sampling - **/
            finish for (worker in workers) {
                async worker.oneSampleIteration(); 
            }
            totalSampleTime += Timer.milliTime() - sampleStart;

            // local syncing amongst threads
            if (i % localSyncRate == 0) {
                val resyncStart:Long = Timer.milliTime();
                resync();
                totalResyncTime += Timer.milliTime() - resyncStart;
            }

            // global syncing amongst places
            if (i % globalSyncRate == 0) {
                
                val startTransmit = Timer.milliTime();
                var reset:Boolean = true;
                for (var p:Long = 0; p < world.numPlaces(); p++) 
                    if (!visited(p)) reset = false;
                if (reset) {
                    for (var p:Long = 0; p < world.numPlaces(); p++) {
                        if (p != here.id)
                            visited(p) = false;
                       
                    }
                    calculateNewCounts();    
                }
                busy.set(false);
                shareCounts();
                totalTransmitTime += Timer.milliTime() - startTransmit;
                
            }
        }
        busy.set(false);
        done = true;
        Console.OUT.println();
    }

    // calculate the counts to send to the world
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
        
        var pIndex:Long = -1;
        
        // find someone to share with. 
        while (pIndex < 0) {
        
            for (var i:Long = 0; i < world.numPlaces(); i++) {
                if (!visited(i)) {
                    val hasLock:GlobalRef[Cell[Boolean]] = new GlobalRef[Cell[Boolean]](new Cell[Boolean](false));
                    at (world(i)) {
                        val success = nodesPlh().busy.compareAndSet(false,true);
                        at (hasLock.home) {
                            hasLock()() = success;
                        }
                    }
                    if (hasLock.getLocalOrCopy()())
                        pIndex = i; 
                }
                
            }
        }
        visited(pIndex) = true;
        exchanges(pIndex)++;

        val step:Long = (ntopics / nthreads)+1;
        finish for (var topicStart:Long = 0; topicStart < ntopics; topicStart+=step) {
            
            val topicStartVal = topicStart;
            val limit = Math.min(ntopics, topicStartVal+step);
            async {
                for (var topic:Long = topicStartVal; topic < limit; topic++) {
                    val t = topic;


                    val newC = typeTopicLocalTotalsDelta(t);
                    val oldC = typeTopicLocalTotalsOld(t);
                    val c = newC - oldC;
                        
                    if (c != 0) {
                        at (world(pIndex)) {
                            nodesPlh().typeTopicWorldTotals(t) += c;
                        }
                    }

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
