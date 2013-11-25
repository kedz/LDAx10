import x10.util.Timer;
import x10.io.File;
import x10.util.ArrayList;
import x10.lang.Exception;
import x10.io.FileNotFoundException;
import x10.array.Array;
import x10.util.HashSet;
import x10.util.HashMap;

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
        var dataDir:File;
        dataDir  = new File("sample_data"); 
        val wordIndexMap:WordIndexMap = new WordIndexMap(dataDir);
        val docs = makeDocuments(dataDir, wordIndexMap);

        printDocs(docs, wordIndexMap);

        val ntopics:Long = 20;

        

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
        val ndocs:Long = dataDir.list().size;
        val docVocabCounts:Rail[Long] = new Rail[Long](ndocs, (i:Long)=> 0);
        val tempDocs:Rail[HashMap[Long,Long]] = new Rail[HashMap[Long,Long]](dataDir.list().size, (i:Long)=>new HashMap[Long,Long]());
        var dIndex:Long = 0;
        for (val fname:String in dataDir.list()) {
            val f:File = new File(fname);
            for (val line:String in f.lines()) {
                for (val W:String in line.split(" ")) {
                    val w:String = W.toLowerCase();
                    val wIndex = wordIndexMap.getIndex(w);  
                    incCountMap(tempDocs(dIndex), wIndex);
                }
            }
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
            docs(dIndex) = new Doc(docWordIndices, docWordCounts);

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
