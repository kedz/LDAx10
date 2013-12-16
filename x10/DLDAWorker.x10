import x10.array.Array_2;
import x10.util.Random;
import x10.util.RailUtils;
import x10.util.Timer;
import x10.util.HashMap;

public class DLDAWorker {

    public val docs:Rail[Documents.Document];
    val vocab:Vocabulary;

    val ntopics:Long;
    val ntypes:Long;
    public val ndocs:Long;

    val alpha:Double;
    val beta:Double;
    val betaSum:Double;

    val rand = new Random(Timer.milliTime());

    public val docTopicCounts:Array_2[Long];
    
    public val typeTopicCountsLocal:Array_2[Long];
    public val typeTopicCountsGlobal:Array_2[Long];

    public val totalTypesPerTopicLocal:Rail[Long];
    public val totalTypesPerTopicGlobal:Rail[Long];
    
    var topicWeights:Rail[Double];

    val id:Long;
    public var typeTopicWorldCounts:Array_2[Long] = null;
    public var typeTopicWorldTotals:Rail[Long] = null;
    var useWorld:Boolean = false;

    public val localIndicesMap:HashMap[Long,Long];
    public val nodeIndicesMap:HashMap[Long,Long];
    
    public def this(vocab:Vocabulary,
                    docs:Rail[Documents.Document],
                    ntopics:Long,
                    alpha:Double,
                    beta:Double,
                    betaSum:Double,
                    id:Long,
                    typeTopicWorldCounts:Array_2[Long],
                    typeTopicWorldTotals:Rail[Long],
                    nodeIndicesMap:HashMap[Long,Long]) {

        this.id = id;
        this.ndocs = docs.size;
        this.ntopics = ntopics;
        this.alpha = alpha;
        this.beta = beta;
        this.betaSum = betaSum;
        this.ntypes = vocab.size();
        this.docs = docs;
        this.vocab = vocab;
        
        val localIndices = new HashMap[Long,Long](); 
        var cntr:Long = 0;
        for (doc in docs)
            for (w in doc.words)
                if (!localIndices.containsKey(w))
                    localIndices.put(w, cntr++);

        this.localIndicesMap = localIndices;
        this.nodeIndicesMap = nodeIndicesMap;        

        this.docTopicCounts = new Array_2[Long](ndocs, ntopics);
        this.typeTopicCountsLocal = new Array_2[Long](localIndices.size(), ntopics);
        this.typeTopicCountsGlobal = new Array_2[Long](nodeIndicesMap.size(), ntopics);
        this.totalTypesPerTopicLocal = new Rail[Long](ntopics);
        this.totalTypesPerTopicGlobal = new Rail[Long](ntopics);
        this.topicWeights = new Rail[Double](ntopics, (t:Long) => 0.0); 
        this.typeTopicWorldCounts = typeTopicWorldCounts;
        this.typeTopicWorldTotals = typeTopicWorldTotals;
        if (this.typeTopicWorldCounts != null && this.typeTopicWorldTotals != null) {
            useWorld = true;
        }

    }

    public def toString():String {
        val repStr:String = "LDAWorker:::thread="+id+"\n"
            + "         :::location="+here+"\n"
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
            var doc:Documents.Document = docs(d);
            for (var w:Long = 0; w < doc.size; w++) {
                val wIndex:Long = doc.words(w);
                val t:Long = rand.nextLong(ntopics);    
                typeTopicCountsLocal(localIndicesMap.get(wIndex)(),t)++;
                docTopicCounts(d,t)++;
                totalTypesPerTopicLocal(t)++;
                doc.topics(w) = t;
            }

        }

    }


    public def resetGlobalCounts() {


        for (var t:Long = 0; t < ntopics; t++) {
            for (w in nodeIndicesMap.keySet()) {
                val nindex = nodeIndicesMap.get(w)();
                typeTopicCountsGlobal(nindex,t) = 0;

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

    public def oneSampleIteration() {

        for (var d:Long = 0; d < ndocs; d++) {
            sampleTopicsForDoc(d); 
        }

    }

    private def sampleTopicsForDoc(d:Long) {
        for (var w:Long = 0; w < docs(d).size; w++) {
           
             
            val wIndex = docs(d).words(w);
            val nIndex = nodeIndicesMap.get(wIndex)();
            val oldTopic = docs(d).topics(w);
            val localIndex = localIndicesMap.get(wIndex)();
            // Subtract counts 
            docTopicCounts(d,oldTopic)--;
            typeTopicCountsLocal(localIndex, oldTopic)--;
            totalTypesPerTopicLocal(oldTopic)--;
            
            var sum:Double = 0.0;
        
            for (var t:Long = 0; t < ntopics; t++) {
                val weight:Double = makeTopicWeight(d, wIndex, localIndex, nIndex, t);
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
            typeTopicCountsLocal(localIndex, newTopic)++;
            totalTypesPerTopicLocal(newTopic)++;
            docs(d).topics(w) = newTopic;
        }

    }

    private def makeTopicWeight(d:Long, wIndex:Long, localIndex:Long, nodeIndex:Long, t:Long) : Double {
        
        if (!useWorld) {
            return (alpha + docTopicCounts(d,t)) 
                    * ((beta + typeTopicCountsLocal(localIndex,t) + typeTopicCountsGlobal(nodeIndex,t)) 
                        / (betaSum + totalTypesPerTopicLocal(t) + totalTypesPerTopicGlobal(t)));
        } else {
            return (alpha + docTopicCounts(d,t)) 
                    * ((beta + typeTopicCountsLocal(localIndex,t) + typeTopicCountsGlobal(nodeIndex,t) + typeTopicWorldCounts(wIndex,t)) 
                        / (betaSum + totalTypesPerTopicLocal(t) + totalTypesPerTopicGlobal(t) + typeTopicWorldTotals(t)));
        }
    }

    public def displayTopWords(topn:Long, topic:Long) {

        val topWords:Rail[Long] = new Rail[Long](topn);
        val topCounts:Rail[Long] = new Rail[Long](topn);

        
        var wordCounts:Rail[Long] = null;
        
        wordCounts = new Rail[Long](ntypes, (w:Long) => typeTopicWorldCounts(w, topic));
        //} else {
        //    wordCounts = new Rail[Long](ntypes, (w:Long) =>  typeTopicCountsGlobal(w, topic));
        //}
        for (w in nodeIndicesMap.keySet()) {
            val nIndex = nodeIndicesMap.get(w)();
            wordCounts(w) += typeTopicCountsGlobal(nIndex, topic);
        }
       
        for (doc in docs)
            for (var w:Long = 0; w < doc.size; w++) {
                if (doc.topics(w) == topic)
                    wordCounts(doc.words(w))++;
            }

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

        Console.OUT.print("( "+topic+" )  "); 
        for (var i:Long = 0; i < topn; i++) {
            Console.OUT.print(" "+vocab.getWord(topWords(i)));
        }
        Console.OUT.println();

    }

}
