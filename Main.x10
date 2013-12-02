import x10.util.Timer;
import x10.io.File;
import x10.util.ArrayList;
import x10.lang.Exception;
import x10.io.FileNotFoundException;
import x10.array.Array;
import x10.array.Array_2;
import x10.util.HashSet;
import x10.util.HashMap;
import x10.util.Random;

public class Main {

    static struct Doc {
        val docWordIndices: Rail[Long];
        val docWordCounts: Rail[Long];
        val wordTopicAssignments: Array_2[Long];
        val size:Long;

        def this(docWordIndices: Rail[Long], docWordCounts: Rail[Long], ntopics:Long) {
            this.docWordIndices = docWordIndices;
            this.docWordCounts = docWordCounts;
            this.size = docWordCounts.size;
            this.wordTopicAssignments = new Array_2[Long](size,ntopics);
        }
    }


    static val rand:Random = new Random();

    public static def initMatrices(docTopicMatrix:Array_2[Long], wordTopicMatrix:Array_2[Long], docs:Rail[Doc], totTopCnts:Rail[Long], ntopics:Long) {
        for (var d:Long = 0; d < docs.size; d++) {
            
            val doc:Doc = docs(d);
             
            for (var w:Long = 0; w < doc.size; w++) {
                val wIndex:Long = doc.docWordIndices(w);
                for (var c:Long = 0; c < doc.docWordCounts(w); c++) {
                    val topicAssign:Long  = rand.nextLong(ntopics);     
                    docTopicMatrix(d,topicAssign)++;
                    wordTopicMatrix(wIndex, topicAssign)++;
                    doc.wordTopicAssignments(w, topicAssign)++;
                    totTopCnts(topicAssign)++;
                }
                
            }
        }
        

    }

    public static def main(args:Rail[String]) {
        var dataDir:File;
        dataDir  = new File("sample_data"); 
        val wordIndexMap:WordIndexMap = new WordIndexMap(dataDir);

        //printDocs(docs, wordIndexMap);

        val ntopics:Long = 20;
        val docs = makeDocuments(dataDir, wordIndexMap, ntopics);
        val ndocs:Long = docs.size;
        val alpha:Double = 50.0/ntopics;
        val beta:Double = 0.01;
        val burnin:Long = 200;
        val iters:Long = 1000;
        val nwords:Long = wordIndexMap.vocabSize();

        Console.OUT.println("Running LDA\n===========\n");
        Console.OUT.println("Num Docs: "+ndocs);
        Console.OUT.println("Num Topics: "+ntopics);
        Console.OUT.println("Num Alpha: "+alpha);
        Console.OUT.println("Num Beta: "+beta);
        Console.OUT.println("Vocab size: "+ nwords);
        Console.OUT.println("Sampling Iterations: "+iters);
        Console.OUT.println("Burn-in: "+burnin);
        
        val docTopicMatrix:Array_2[Long] = new Array_2[Long](ndocs, ntopics, (i:Long, j:Long) => 0);
        
        val wordTopicMatrix:Array_2[Long] = new Array_2[Long](nwords, ntopics, (i:Long, j:Long) => 0);
        val totalTopicCnts:Rail[Long] = new Rail[Long](ntopics, (i:Long) =>0);
        initMatrices(docTopicMatrix, wordTopicMatrix, docs, totalTopicCnts, ntopics);

        val finalWordSamples:Array_2[Long] = new Array_2[Long](nwords, ntopics);


        for (var i:Long = 1; i <= iters; i++) {
            Console.OUT.println("Iteration "+i);
                
            for (var d:Long = 0; d < docs.size; d++) {
                val dnum = d;
                val doc:Doc = docs(d);
                val topicDist:Rail[Double] = new Rail[Double](ntopics, (i:Long) => (docTopicMatrix(dnum,i)+alpha) / (doc.size-1+ (ntopics*alpha ) ));
             
                for (var w:Long = 0; w < doc.size; w++) {
                    val cnt:Long = doc.docWordCounts(w);
                    val wIndex:Long = doc.docWordIndices(w);
                    val wordDist:Rail[Double] = new Rail[Double](ntopics, (i:Long) => (wordTopicMatrix(wIndex,i)+beta) / (totalTopicCnts(i)-1+ (nwords*beta)));
                    
                    
                    val newTopicAssignments:Rail[Long] = new Rail[Long](ntopics, (i:Long)=>0);
                    
                    //Console.OUT.println("Document "+d+" : Word "+w+" : "+wordIndexMap.getWord(wIndex)+" : count "+cnt);
                    var idx:Long = 0;
                    for (var t:Long = 0; t < ntopics; t++) {
                        val oldTopic:Long = t;
                        val tcnt:Long = doc.wordTopicAssignments(w,t);
                        for (var c:Long = 0; c < tcnt; c++) {
                            //Remove the current assignment
                            topicDist(oldTopic) -= 1.0 / (doc.size-1+ (ntopics*alpha ) );
                            wordDist(oldTopic) -= 1.0 / (totalTopicCnts(oldTopic)-1+ (nwords*beta));
                            
                                
                            val posteriorDist:Rail[Double] = new Rail[Double](ntopics, (i:Long) => topicDist(i) * wordDist(i));
                            var norm:Double = 0.0;
                            for (val p in posteriorDist)
                                norm += p;
                            for (var p:Long = 0; p < ntopics; p++)
                                posteriorDist(p) = posteriorDist(p) / norm;
                             
                            val newTopic:Long = sampleMulti(posteriorDist);
                            newTopicAssignments(newTopic)++;
                            
                            docTopicMatrix(d,oldTopic)--;
                            docTopicMatrix(d,newTopic)++;
                            
                            wordTopicMatrix(wIndex, oldTopic)--;    
                            wordTopicMatrix(wIndex, newTopic)++;    
                            
                            topicDist(newTopic) -= 1.0 / (doc.size-1+ (ntopics*alpha ) );
                            wordDist(newTopic) += 1.0 / (totalTopicCnts(newTopic)-1+ (nwords*beta));
                            
                            totalTopicCnts(oldTopic)--;
                            totalTopicCnts(newTopic)++;
                            
                            if (i > burnin) {
                                finalWordSamples(wIndex,newTopic)++;

                            }

                            //oldTopicAssignments(idx) = t;
                            idx++;

                        }
                        //Console.OUT.print("Tpc "+t+"- "+ tcnt+ " | ");  
        
                    }

                    for (var t:Long = 0; t < ntopics; t++) {
                        doc.wordTopicAssignments(w,t) = newTopicAssignments(t);
                    }
                    //Console.OUT.println();
                

                }
            }
        
        }
    

        Console.OUT.println("Computing word distributions...");
        val wordDists:Array_2[Double] = new Array_2[Double](nwords, ntopics);
        for (var t:Long = 0; t < ntopics; t++) {
            var norm:Double = nwords*beta;
            
            for (var wIndex:Long = 0; wIndex < nwords; wIndex++) {
                norm += finalWordSamples(wIndex, t);
            }
            
            for (var wIndex:Long = 0; wIndex < nwords; wIndex++) {
                wordDists(wIndex, t) = (finalWordSamples(wIndex,t)+beta) / norm;

            }

        }


        val top10Words:Array_2[String] = new Array_2[String](ntopics,10);
        val top10Probs:Array_2[Double] = new Array_2[Double](ntopics,10);
        
        val len:Long = 10;

        for (var wIndex:Long = 0; wIndex < nwords; wIndex++) {
            
            var maxTopic:Long = -1;
            val wIndexVal = wIndex;
            var counts:Rail[Long] = new Rail[Long](ntopics, (i:Long) => wordTopicMatrix(wIndexVal,i));
            
            for (var t:Long = 0; t < ntopics; t++) {
                var i:Long = len;
                while (i >0 && top10Probs(t, i-1) < wordDists(wIndex,t)) {
                    if (i < len -1) {
                        top10Probs(t, i) = top10Probs(t, i-1);
                        top10Words(t, i) = top10Words(t, i-1);

                    }

                    i--;
                }
                if (i < len) {
                    top10Probs(t, i) = wordDists(wIndex,t);
                    top10Words(t, i) = wordIndexMap.getWord(wIndex);
                }
            }
            
               
        }
        for (var t:Long = 0; t < ntopics; t++) {
            Console.OUT.print("Topic\t"+t+"\t");
            for (var w:Long = 0; w < len; w++) {
                Console.OUT.print(top10Words(t,w)+" ");
            }
            Console.OUT.println();
        }
    
    }

    public static def sampleMulti(dist:Rail[Double]) : Long {
        var sum:Double = 0.0;
        val r:Double = rand.nextDouble();

        for (var t:Long = 0; t < dist.size; t++) {
            sum += dist(t);
            if (r <= sum)
                return t;
            
        }

        //Should never get here
        return -1;
    }

    public static def printDocs(docs:Rail[Doc], wordIndexMap:WordIndexMap) {
        
        for (var i:Long = 0; i < docs.size; i++) { 
            Console.OUT.println("Document "+i);
            val doc = docs(i);
            val nwords = doc.docWordIndices.size;
            for (var w:Long = 0; w < nwords; w++) {
                var wIndex:Long = doc.docWordIndices(w);
                var word:String = wordIndexMap.getWord(wIndex);
                var wCount:Long = doc.docWordCounts(w);    
                Console.OUT.print(word+":"+wCount+" ");
            }
            Console.OUT.println("\n============================================\n");
        }
    }

    public static def makeDocuments(dataDir:File, wordIndexMap:WordIndexMap, ntopics:Long) : Rail[Doc] {
        val ndocs:Long = dataDir.list().size;
        val docVocabCounts:Rail[Long] = new Rail[Long](ndocs, (i:Long)=> 0);
        val tempDocs:Rail[HashMap[Long,Long]] = new Rail[HashMap[Long,Long]](dataDir.list().size, (i:Long)=>new HashMap[Long,Long]());
        var dIndex:Long = 0;
        for (val fname:String in dataDir.list()) {
            
            val f:File = new File(fname);
            val reader = f.openRead();
            
            for (val line:String in reader.lines()) {
                for (val W:String in line.split(" ")) {
                    val w:String = W.toLowerCase();
                    val wIndex = wordIndexMap.getIndex(w);  
                    incCountMap(tempDocs(dIndex), wIndex);
                }
            }
            reader.close();
            docVocabCounts(dIndex) = tempDocs(dIndex).size();
            dIndex++;
        }

        val docs: Rail[Doc] = new Rail[Doc](ndocs);
        
        dIndex = 0;
        for (val tempDoc in tempDocs) {
            val docWordIndices: Rail[Long] = new Rail[Long](docVocabCounts(dIndex), (i:long) => 0);       
            val docWordCounts: Rail[Long] = new Rail[Long](docVocabCounts(dIndex), (i:long) => 0);       
            
            var localIndex:Long = 0;
            for (wIndex in tempDoc.keySet()) {
                docWordIndices(localIndex) = wIndex;
                docWordCounts(localIndex) = tempDoc.get(wIndex).value;
                localIndex++;
            }                
            docs(dIndex) = new Doc(docWordIndices, docWordCounts, ntopics);

            dIndex++;
        }        

        return docs;

    }



    public static def incCountMap(countMap:HashMap[Long,Long], wIndex:Long) {
        if (countMap.containsKey(wIndex)) {
            var cntPlusPlus:Long = countMap.get(wIndex).value + 1;
            countMap.put(wIndex, cntPlusPlus);
        } else {
            countMap.put(wIndex, 1);
        }
    }

}
