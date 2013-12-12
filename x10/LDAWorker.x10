import x10.array.Array_2;
import x10.util.Random;
import x10.util.RailUtils;
import x10.util.Timer;

public class ParallelLDA {

    val docs:Rail[DocumentsFrags.Document];
    val vocab:Vocabulary;

    val ntopics:Long;
    val ntypes:Long;
    val ndocs:Long;

    val alpha:Double;
    val alphaSum:Double;
    val beta:Double;
    val betaSum:Double;

    val rand = new Random(Timer.milliTime());

    val docTopicCounts:Array_2[Long];
    
    val typeTopicCountsLocal:Array_2[Long];
    val typeTopicCountsGlobal:Array_2[Long];

    val totalTypesPerTopicLocal:Rail[Long];
    val totalTypesPerTopicGlobal:Rail[Long];
    
    var topicWeights:Rail[Double];

    val id:Long;

    public def this(vocab:Vocabulary, docs:Rail[DocumentsFrags.Document], ntopics:Long, alpha:Double, beta:Double, betaSum:Double, id:Long) {
        this.id = id;
        this.ndocs = docs.size;
        this.ntopics = ntopics;
        this.alpha = alpha;
        this.beta = beta;
        this.betaSum = betaSum;
        this.ntypes = vocab.size();
        this.docs = docs;
        this.vocab = vocab;
        
        this.docTopicCounts = new Array_2[Long](ndocs, ntopics);
        this.typeTopicCountsLocal = new Array_2[Long](ntypes, ntopics);
        this.typeTopicCountsGlobal = new Array_2[Long](ntypes, ntopics);
        this.totalTypesPerTopicLocal = new Rail[Long](ntopics);
        this.totalTypesPerTopicGlobal = new Rail[Long](ntopics);
        this.topicWeights = new Rail[Double](ntopics, (t:Long) => 0.0); 

    }

    public def toString():String {
        val repStr:String = "LDAWorker:::thread="+id"\n"
            + "         :::location="+here"\n"
            + "         :::ntopics="+ntopics+"\n"
            + "         :::alpha="+alpha+"\n"
            + "         :::beta="+beta+"\n"
            + "         :::betaSum="+betaSum+"\n"
            + "         :::ndocs="+ndocs+"\n" 
            + "         :::ntypes="+ntypes;

        return repStr;            
    }

    public def initLocal() {
        
        for (var d:Long = 0; d < docs.size; d++) {
            var doc:DocumentsFrags.Document = docs(d);
            for (var w:Long = 0; w < doc.size; w++) {
                val wIndex:Long = doc.words(w);
                val t:Long = rand.nextLong(ntopics);    
                typeTopicCountsLocal(wIndex,t)++;
                docTopicCounts(d,t)++;
                totalTypesPerTopicLocal(t)++;
                doc.topics(w) = t;
            }

        }

    }


    public def resetGlobalCounts() {
        for (var t:Long = 0; t < ntopics; t++) {
            for (var i:Long = 0; i < ntypes; i++) {
                typeTopicCountsGlobal(i,t) = 0;

            }
            totalTypesPerTopicGlobal(t) = 0;
        }
    }

    public def addGlobalCounts(oTypeTopicMatrix:Array_2[Long], oTypesPerTopicCounts:Rail[Long]) {
        
        for (var t:Long = 0; t < ntopics; t++) {
            for (var i:Long = 0; i < ntypes; i++) {
                typeTopicCountsGlobal(i,t) += oTypeTopicMatrix(i,t);

            }
            totalTypesPerTopicGlobal(t) += oTypesPerTopicCounts(t);
        }
    }
    

    public def getLocalTypeTopicMatrix() : Array_2[Long] {
        return typeTopicCountsLocal;
    }

    public def getLocalTypesPerTopicCounts() : Rail[Long] {
        return totalTypesPerTopicLocal;
    }

    /*
    public def sample(niters:Long) {

        init();
                
        Console.OUT.println("Sampling for "+niters+" iterations.");
        for (var i:Long = 0; i < niters; i++) {
            if (i%100 == 0) 
                Console.OUT.print(i);
            else
                Console.OUT.print(".");

            for (var d:Long = 0; d < ndocs; d++) {
                sampleTopicsForDoc(d); 
            }
        }
        Console.OUT.println();

    }

    private def sampleTopicsForDoc(d:Long) {
        for (var w:Long = 0; w < docs(d).size; w++) {
           
            
            val wIndex = docs(d).words(w);
            val oldTopic = docs(d).topics(w);
            
            // Subtract counts 
            docTopicCounts(d,oldTopic)--;
            typeTopicCounts(wIndex, oldTopic)--;
            totalTypesPerTopic(oldTopic)--;
            
            var sum:Double = 0.0;
        
            for (var t:Long = 0; t < ntopics; t++) {
                val weight:Double = makeTopicWeight(d, wIndex, t);
                topicWeights(t) = weight;
                sum += weight;
            }

                
            var sample:Double = rand.nextDouble() * sum;
            
            var newTopic:Long = -1;
            while (sample > 0.0 && newTopic < ntopics) {
                newTopic++;
                sample -= topicWeights(newTopic);
            }
           
            if (newTopic >= ntopics || newTopic < 0)
                Console.OUT.println("BAD TOPIC " + newTopic+"  "+docs(d).size +"  "+sum);
            docTopicCounts(d,newTopic)++;
            typeTopicCounts(wIndex, newTopic)++;
            totalTypesPerTopic(newTopic)++;
            docs(d).topics(w) = newTopic;
        }

    }

    private def makeTopicWeight(d:Long, wIndex:Long, t:Long) : Double {
        return (alpha + docTopicCounts(d,t)) * ((beta + typeTopicCounts(wIndex,t)) / (betaSum + totalTypesPerTopic(t)));
    }


    public def displayTopN(topn:Long, topic:Long) {

        val topWords:Rail[Long] = new Rail[Long](topn);
        val topCounts:Rail[Long] = new Rail[Long](topn);

        val wordCounts:Rail[Long] = new Rail[Long](ntypes, (w:Long) => typeTopicCounts(w, topic));
        for (var i:Long = 0; i < ntypes; i++) {

            var j:Long = topn-1;
            while (j >= 0) {
                if (wordCounts(i) > topCounts(j)) {
                    if (j+1 < topn) {
                       topCounts(j+1) = topCounts(j);
                       topWords(j+1) = topWords(j);
                                        
                    }
                   topCounts(j) = wordCounts(i);
                   topWords(j) = i;


                } else {
                    //if (j+1 < topn) {
                    //    topCounts(j+1) = wordCounts(i);
                   //     topWords(j+1) = i;
                    //}
        
                    break;
                }
                
                j--;
            }
      
        }

        for (var i:Long = 0; i < topn; i++) {
            Console.OUT.print(vocab.getWord(topWords(i))+" ");
            //Console.OUT.println(vocab.getWord(i)+": "+wordCounts(i));
        }
        Console.OUT.println();

    }

    public def logLikelihood() : Double {
        return 42.0;
    }
    */
}