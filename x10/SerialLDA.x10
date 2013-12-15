import x10.array.Array_2;
import x10.util.Random;
import x10.util.RailUtils;
import x10.util.Timer;

public class SerialLDA {

    val docs:Rail[Documents.Document];
    val vocab:Vocabulary;

    val ntopics:Long;
    val ntypes:Long;
    val ndocs:Long;

    val alpha:Double;
    val alphaSum:Double;
    val beta:Double;
    val betaSum:Double;

    
    val rand = new Random(Timer.milliTime());

    val typeTopicCounts:Array_2[Long];
    val docTopicCounts:Array_2[Long];
    val totalTypesPerTopic:Rail[Long];
    
    var topicWeights:Rail[Double];

    public def this(vocab:Vocabulary, docs:Rail[Documents.Document], ntopics:Long, alphaSum:Double, beta:Double) {
        this.ndocs = docs.size;
        this.ntopics = ntopics;
        this.alpha = alphaSum / ntopics;
        this.alphaSum = alphaSum;
        this.beta = beta;
        this.ntypes = vocab.size();
        this.betaSum = this.ntypes * this.beta;
        this.docs = docs;
        this.vocab = vocab;
        this.typeTopicCounts = new Array_2[Long](ntypes, ntopics);
        this.docTopicCounts = new Array_2[Long](ndocs, ntopics);
        this.totalTypesPerTopic = new Rail[Long](ntopics);
        this.topicWeights = new Rail[Double](ntopics, (t:Long) => 0.0); 

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


    public def sample(niters:Long) {

                
        Console.OUT.println("Sampling for "+niters+" iterations.");
        for (var i:Long = 1; i <= niters; i++) {
            if (i%100 == 0) 
                Console.OUT.print(i);
            else
                Console.OUT.print(".");

            for (var d:Long = 0; d < ndocs; d++) {
                sampleTopicsForDoc(d); 
            }
        }
        Console.OUT.println("\n");

    }

    public def init() {
        
        
        for (var d:Long = 0; d < docs.size; d++) {
            var doc:Documents.Document = docs(d);
            for (var w:Long = 0; w < doc.size; w++) {
                val wIndex:Long = doc.words(w);
                val t:Long = rand.nextLong(ntopics);    
                typeTopicCounts(wIndex,t)++;
                docTopicCounts(d,t)++;
                totalTypesPerTopic(t)++;
                doc.topics(w) = t;
            }

        }


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


    public def displayTopWords(topn:Long, topic:Long) {

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

        Console.OUT.print("( "+topic+" ) ");
        for (var i:Long = 0; i < topn; i++) {
            Console.OUT.print(" "+vocab.getWord(topWords(i)));
        }
        Console.OUT.println();

    }

    public def logLikelihood() : Double {
    
        var logLikelihood:Double = 0.0;
        for (var d:Long = 0; d < ndocs; d++) {
            for (var t:Long = 0; t < ntopics; t++) {
                if (docTopicCounts(d,t) > 0) {
                    logLikelihood += (MathUtils.logGamma(alpha + docTopicCounts(d,t)))
                                        - MathUtils.logGamma(alpha);
                }
            }

            logLikelihood -= MathUtils.logGamma(alphaSum + docs(d).size);

        }

        logLikelihood += ndocs * MathUtils.logGamma(alphaSum);

        var nonZeroTypeTopics:Long = 0;
        for (var w:Long = 0; w < ntypes; w++) {
            for (var t:Long = 0; t < ntopics; t++) {
                if (typeTopicCounts(w,t) == 0) continue;
                nonZeroTypeTopics++;
                logLikelihood += MathUtils.logGamma(beta + typeTopicCounts(w,t));
            }
        }

        for (var t:Long = 0; t < ntopics; t++) {
            logLikelihood -= MathUtils.logGamma((beta * ntopics) + totalTypesPerTopic(t));
        }

        logLikelihood += MathUtils.logGamma(beta * ntopics) - (MathUtils.logGamma(beta)* nonZeroTypeTopics);

        return logLikelihood;
    }

}
