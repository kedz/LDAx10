import x10.io.File;
import x10.util.Stack;
import x10.util.HashSet;
import x10.util.HashMap;

public class Vocabulary {
    
    private var wordIndexMap:HashMap[String, Long] = null;
    private var indexWordMap:HashMap[Long, String] = null;

    public def this(dataDir: File) {

        val vocab:HashSet[String] = new HashSet[String]();
        
        for (val fname:String in dataDir.list()) {

            var f:File = new File(fname);
            val reader = f.openRead();
            
            for (val line in reader.lines()) {
                for (w in line.split(" ")) {
                    val word:String = w.trim().toLowerCase();
                    if (!word.equals(""))
                        vocab.add(word);
                }
            }
            reader.close();
            f = null;
        }

        indexWordMap = new HashMap[Long,String]();
        wordIndexMap = new HashMap[String,Long]();
        var index:Long = 0;
        for (val w in vocab) {
            wordIndexMap.put(w, index);
            indexWordMap.put(index, w);
            index++;
        }

    }

    public def this(vocab:HashSet[String]) {
        
        this.indexWordMap = new HashMap[Long,String]();
        this.wordIndexMap = new HashMap[String,Long]();
        var index:Long = 0;
        for (val w in vocab) {
            wordIndexMap.put(w, index);
            indexWordMap.put(index, w);
            index++;
        }
        
    }

    public static def buildVocabParallel(fileList:Rail[String], nthreads:Long) : Vocabulary {
        
        val vocabs:Rail[HashSet[String]] = new Rail[HashSet[String]](nthreads-1, (i:Long) => new HashSet[String]());
        val finished:Rail[Boolean] = new Rail[Boolean](nthreads-1, (i:Long) => false);
        val fstacks:Rail[Stack[File]] = new Rail[Stack[File]](nthreads-1, (i:Long) => new Stack[File]());
        
        var cntr:Long = 0;
        for (fname in fileList) {
            fstacks(cntr % (nthreads-1)).push(new File(fname));
            cntr++;
        }
            
            
        for (var thread:Long = 0; thread < nthreads-1; thread++) {
            val t = thread;
            async {    
                while (fstacks(t).size() > 0) {
                    var file:File = fstacks(t).pop();    
                    val reader = file.openRead();
            
                    for (val line in reader.lines()) {
                        for (w in line.split(" ")) {
                            val word:String = w.trim().toLowerCase();
                            if (!word.equals(""))
                                vocabs(t).add(word);
                        }
                    }

                    reader.close();

                }

                finished(t) = true;
            }
        }

        var done:Boolean = false;
        while (!done) {
            done = true;
            for (flag in finished)
                if (flag == false) done = false;
        }
                
        val allVocab:HashSet[String] = new HashSet[String]();
        for (vocab in vocabs) {
            for (word in vocab)
                allVocab.add(word);
        }

        return new Vocabulary(allVocab);
                
    }





    public def getIndex(val w:String) : Long {
        return wordIndexMap.getOrElse(w, -1);
    }

    public def getWord(val i:Long) : String {
        return indexWordMap.getOrElse(i, "");
    }

    public def size(): Long {
        return indexWordMap.size();
    }

}
