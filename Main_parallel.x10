import x10.util.Timer;
import x10.io.File;
import x10.util.ArrayList;
import x10.lang.Exception;
import x10.io.FileNotFoundException;
import x10.array.Array;
import x10.util.HashSet;
import x10.util.HashMap;
import x10.util.concurrent.AtomicLong;
import x10.lang.System;

public class Main {

    static struct Doc {
        val docWordIndices: Rail[Long];
        val docWordCounts: Rail[Long];

        def this(docWordIndices: Rail[Long], docWordCounts: Rail[Long]) {
            this.docWordIndices = docWordIndices;
            this.docWordCounts = docWordCounts;
        }
    }

    public static def main(args:Rail[String]) {
		val start = Timer.milliTime();
		
        var dataDir:File;
        dataDir  = new File("large_data_set"); 
		
        val wordIndexMap:WordIndexMap = new WordIndexMap(dataDir);
        val docs = makeDocuments(dataDir, wordIndexMap);
        printDocs(docs, wordIndexMap);

        val ntopics:Long = 20;
		
		val end = Timer.milliTime();
		Console.OUT.println("Time taken = "+ (end-start));

        

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

    public static def makeDocuments(dataDir:File, wordIndexMap:WordIndexMap) : Rail[Doc] {
	
		Console.OUT.println("Starting to make documents "+dataDir.getName());
		Console.OUT.println(dataDir.list());
		for (val fname:String in dataDir.lines()) {
			Console.OUT.println("Hello");
        }
        val ndocs:Long = dataDir.list().size;
		Console.OUT.println("nDocs: =" + ndocs);
        val docVocabCounts:Rail[Long] = new Rail[Long](ndocs, (i:Long)=> 0);
		
        val tempDocs:Rail[HashMap[Long,Long]] = new Rail[HashMap[Long,Long]](dataDir.list().size, (i:Long)=>new HashMap[Long,Long]());
        var dIndex:Long = 0;
		val idx = new AtomicLong(0);
		val numThreads = new AtomicLong(0);
		
		Console.OUT.println("Starting loop");
        finish for (val fname:String in dataDir.list()) {
            val f:File = new File(fname);
			val j = idx.get();
			Console.OUT.println("Document name "+fname);
			
			async{
				numThreads.getAndIncrement();
				Console.OUT.println("Size "+tempDocs.size+" Index " + j);
				for (val line:String in f.lines()) {
					for (val W:String in line.split(" ")) {
						val w:String = W.toLowerCase();
						val wIndex = wordIndexMap.getIndex(w); 
						incCountMap( tempDocs( j ), wIndex);
					}
				}
				Console.OUT.println("Size "+tempDocs( j ).size());
				docVocabCounts(j) = tempDocs( j ).size();
				numThreads.getAndDecrement();
			}
			idx.getAndIncrement();
			while( numThreads.get() > 2*Runtime.NTHREADS){
				//Wait;
			}
        }

        val docs: Rail[Doc] = new Rail[Doc](ndocs);
        
        dIndex = 0;
		numThreads.set(0);
		
        finish for (val tempDoc in tempDocs) {
		
			val threadDocIdx = dIndex;
		
			async{
				numThreads.getAndIncrement();
				val docWordIndices: Rail[Long] = new Rail[Long](docVocabCounts(threadDocIdx), (i:long) => 0);       
				val docWordCounts: Rail[Long] = new Rail[Long](docVocabCounts(threadDocIdx), (i:long) => 0);       
				
				var localIndex:Long = 0;
				for (wIndex in tempDoc.keySet()) {
					docWordIndices(localIndex) = wIndex;
					docWordCounts(localIndex) = tempDoc.get(wIndex).value;
					localIndex++;
				}                
				docs(threadDocIdx) = new Doc(docWordIndices, docWordCounts);
				numThreads.getAndDecrement();
			}
			while( numThreads.get() > 2*Runtime.NTHREADS){
				//Wait;
			}
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
