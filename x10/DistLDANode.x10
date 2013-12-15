import x10.array.Array_2;
import x10.util.ArrayList;
import x10.util.Random;
import x10.util.RailUtils;
import x10.util.Timer;

public class DistLDANode {

    val vocab:Vocabulary;

    public val ntopics:Long;
    public val ntypes:Long;
    public val ndocs:Long;

    public val alpha:Double;
    public val alphaSum:Double;
    public val beta:Double;
    public val betaSum:Double;
    
    val nthreads:Long;
    public val workers:Rail[LDAWorker];
    public val typeTopicWorldCounts:Array_2[Long];
    public val typeTopicWorldTotals:Rail[Long]; 

    public var typeTopicLocalCountsOld:Array_2[Long];
    public var typeTopicLocalTotalsOld:Rail[Long]; 
    
    public var typeTopicLocalCountsDelta:Array_2[Long];
    public var typeTopicLocalTotalsDelta:Rail[Long]; 

    

    val world:PlaceGroup.SimplePlaceGroup;


    var totalSampleTime:Long = 0;
    var totalResyncTime:Long = 0;    

    var done:Boolean = false;
    var talkingToWorld:Boolean = false;

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

        this.nthreads = nthreads;
        this.typeTopicLocalCountsOld = new Array_2[Long](ntypes, ntopics);
        this.typeTopicLocalTotalsOld = new Rail[Long](ntopics);
        this.typeTopicLocalCountsDelta = new Array_2[Long](ntypes, ntopics);
        this.typeTopicLocalTotalsDelta = new Rail[Long](ntopics);


        this.typeTopicWorldCounts = new Array_2[Long](ntypes, ntopics); 
        this.typeTopicWorldTotals = new Rail[Long](ntopics);

        workers = new Rail[LDAWorker](nthreads);
        for (var t:Long = 0; t < nthreads; t++)
            workers(t) = new LDAWorker(vocab, docsFrags.get(t), ntopics, alpha, beta, this.betaSum, t, typeTopicWorldCounts, typeTopicWorldTotals);
        
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
                    typeTopicLocalTotalsDelta(t) = totalWordsPerTopic;// - typeTopicLocalTotalsOld(t);                                   
                    //typeTopicLocalTotalsOld(t) = totalWordsPerTopic;                                   
                    /*
                    if (transmit && deltaTot != 0 ) {
                        val tVal = t;
                        Console.OUT.println("Topic: "+tVal);
                        
                        for (p in world) {
                            if (p.id != here.id) {
                                at (p) {
                                    typeTopicWorldTotalsPlh()(tVal) += deltaTot;
                                }
                            }
                        }

                    }
                    */
                    for (var w:Long = 0; w < ntypes; w++) {
                        var typeCount:Long = 0;
                        for (worker in workers) 
                            typeCount += worker.typeTopicCountsLocal(w,t);
                        for (worker in workers)
                            worker.typeTopicCountsGlobal(w,t) = typeCount - worker.typeTopicCountsLocal(w,t);
                        
                        typeTopicLocalCountsDelta(w,t) = typeCount;// - typeTopicLocalCountsOld(w,t);                                   
                        //typeTopicLocalCountsOld(w,t) = typeCount;   


                        /*
                        val delta = typeCount - typeTopicLocalCountsOld(w,t);                                   
                        typeTopicLocalCountsOld(w,t) = typeCount;   
                        if (transmit && delta != 0 ) {
                            val wVal = w;
                            val tVal = t;
                            for (p in world) {
                                if (p != here) {
                                    at (p) {
                                        typeTopicWorldCountsPlh()(wVal,tVal) += delta;
                                    }
                                }
                            }

                        }
                        */                                
                    }

                }
            }
        }

        /*
        if (!talkingToWorld) {
            talkingToWorld = true;
*/
/*
        Console.OUT.println("Communicating from "+here);
        finish for (p in world) {
            if (p != here) {
                async { 
                    //at (p) {
                        for (var t:Long = 0; t < ntopics; t++) {
                            val tVal = t;
                            if (typeTopicLocalTotalsDelta(tVal) != 0) {
                                val c = typeTopicLocalTotalsDelta(tVal);
                                at (p) typeTopicWorldTotalsPlh()(tVal) += c;    
                            }

                            for (var w:Long = 0; w < ntypes; w++) {
                                val wVal = w;
                                if (typeTopicLocalCountsDelta(wVal,tVal) != 0) {
                                    val c = typeTopicLocalCountsDelta(wVal,tVal);
                                    at (p) typeTopicWorldCountsPlh()(wVal,tVal) += c;
                                
                                }
                            }

                        }
                        
                    //}
                }
            }
        
          //  talkingToWorld = false;
        }
  */      
       // }
    }


    public def sampleOneIteration() {

        val sampleStart:Long = Timer.milliTime();
        finish for (worker in workers) {
            async worker.oneSampleIteration(); 
        }
        totalSampleTime += Timer.milliTime() - sampleStart;

    }


    public def sample(niters:Long) {

        done = false;

        for (var i:Long = 1; i <= niters; i++) {
            if (i%100 == 0) 
                Console.OUT.print(i+"{"+here+"} ");
            //else
            //    Console.OUT.print(".");
            Console.OUT.flush();

            val sampleStart:Long = Timer.milliTime();
            finish for (worker in workers) {
                async worker.oneSampleIteration(); 
            }
            totalSampleTime += Timer.milliTime() - sampleStart;
            Console.OUT.print("*");
            Console.OUT.flush();

            if (i % 2 == 0) {
                //val transmit = (i % 10 == 0) ? true : false;
                val resyncStart:Long = Timer.milliTime();
                Console.OUT.print("r");
                Console.OUT.flush();
                resync();
                Console.OUT.print("R");
                Console.OUT.flush();
                totalResyncTime += Timer.milliTime() - resyncStart;
            }
        }
        done = true;
        Console.OUT.println();
    }

    public def shareCounts(p:Place, nodePlh:PlaceLocalHandle[DistLDANode]) {
        for (var topic:Long = 0; topic < ntopics; topic++) {
            val t = topic;
            for (var word:Long = 0; word < ntypes; word++) {
                val w = word;
                if (typeTopicLocalCountsDelta(w,t) != 0) {
                    val c = typeTopicLocalCountsDelta(w,t);
                    at (p) {
                        nodePlh().typeTopicWorldCounts(w,t) += c;
                    }
                }
            }

            if (typeTopicLocalTotalsDelta(t) != 0) {
                val c = typeTopicLocalTotalsDelta(t);
                at (p) {
                    nodePlh().typeTopicWorldTotals(t) += c;
                }
            }
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

}
