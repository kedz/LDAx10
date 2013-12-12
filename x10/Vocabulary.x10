import x10.io.File;
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
