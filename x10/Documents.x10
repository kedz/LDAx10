import x10.util.Stack;
import x10.io.File;

public class Documents {

    public static struct Document {

        val words:Rail[Long];
        val topics:Rail[Long];
        val size:Long;

        public def this(words:Rail[Long], topics:Rail[Long]) {
            this.words = words;
            this.topics = topics;
            this.size = topics.size;
        }

    }

    val docs:Rail[Document];
    val size:Long;

    public def this(vocab:Vocabulary, dataDir:File) {

        val docStack:Stack[Document] = new Stack[Document]();
        val wordStack:Stack[Long] = new Stack[Long]();

        for (fname:String in dataDir.list()) { 
            var f:File = new File(fname);
            val reader = f.openRead();

            for (val line in reader.lines()) {
                for (w in line.split(" ")) {
                    val wStr = w.trim().toLowerCase();
                    if (!wStr.equals(""))
                        wordStack.push(vocab.getIndex(wStr));
                }
            }
            reader.close();
            f = null;

            val words:Rail[Long] = new Rail[Long](wordStack.size(), (i:Long) => wordStack.pop());
            val topics:Rail[Long] = new Rail[Long](words.size);
            docStack.push(new Document(words, topics));

        }

        this.docs = new Rail[Document](docStack.size(), (i:Long) => docStack.pop());
        this.size = docs.size;
       
    }


}
