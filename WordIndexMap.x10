import x10.io.File;
import x10.util.HashSet;
import x10.util.HashMap;

public class WordIndexMap {

    
    private var wordIndexMap:HashMap[String, Long] = null;
    private var indexWordMap:HashMap[Long, String] = null;

    public def this(dataDir: File) {

        val vocab:HashSet[String] = new HashSet[String]();

        for (val fname:String in dataDir.list()) {
            val f:File = new File(fname);
            for (val line in f.lines()) {
                for (w in line.split(" ")) {
                    vocab.add(w.toLowerCase());
                }
            }
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

    public def getIndex(val w:String) : Long {
        return wordIndexMap.getOrElse(w, -1);
    }

    public def getWord(val i:Long) : String {
        return indexWordMap.getOrElse(i, "");
    }
}
