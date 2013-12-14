import x10.util.ArrayList;
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

    public static def buildDocumentFragments(vocab:Vocabulary, fileList:Rail[String], nthreads:Long) : ArrayList[Rail[Document]] {

        val fstacks:Rail[Stack[File]] = new Rail[Stack[File]](nthreads, (i:Long) => new Stack[File]());
        val documentFragments:ArrayList[Rail[Document]] = new ArrayList[Rail[Document]](nthreads);
        for (var i:Long = 0; i < nthreads; i++)
            documentFragments.add(null);

        var cntr:Long = 0;
        for (fname in fileList) {
            fstacks(cntr % (nthreads)).push(new File(fname));
            cntr++;
        }
            
            
        finish for (var thread:Long = 0; thread < nthreads; thread++) {
            val t = thread;
            async {    
                val docStack:Stack[Document] = new Stack[Document]();
                val wordStack:Stack[Long] = new Stack[Long]();
                while (fstacks(t).size() > 0) {
                    var file:File = fstacks(t).pop();    
                    val reader = file.openRead();
            
                    for (val line in reader.lines()) {
                        for (w in line.split(" ")) {
                            val word:String = w.trim().toLowerCase();
                            if (!word.equals(""))
                                wordStack.add(vocab.getIndex(word));
                        }
                    }

                    reader.close();
                    val words:Rail[Long] = new Rail[Long](wordStack.size(), (i:Long) => wordStack.pop());
                    val topics:Rail[Long] = new Rail[Long](words.size);
                    docStack.push(new Document(words, topics));
                }


                val docs:Rail[Document] = new Rail[Document](docStack.size(), (i:Long) => docStack.pop());
                documentFragments.set(docs, t);
            
            }
        }

        return documentFragments;
 

    }

}
