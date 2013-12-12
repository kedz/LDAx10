import x10.util.Stack;
import x10.util.ArrayList;
import x10.io.File;

public class DocumentsFrags {


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

    val docFrags:ArrayList[Rail[Document]];


    public def this(vocab:Vocabulary, dataDir:File, nthreads:long ) {

        val dirSize:long = dataDir.list().size;
        val dirList = dataDir.list();
        val chunkSize = dirSize/nthreads;
		var rem:long = dirSize%nthreads;
		docFrags = new ArrayList[Rail[Document]](nthreads);
        Console.OUT.println("Chunksize: "+chunkSize);
        	
        finish for (var i:long = 0; i < dirSize ; i+= chunkSize , rem--) {
            
            val offset = i;
            val threadIndex = i / chunkSize;
            var limit:long = (i+chunkSize) > dirSize ? dirSize : i+chunkSize;
			if(rem > 0){
				limit++;
				i++;
			}
			val lim=limit;
            Console.OUT.println("Limit: "+lim);
            val docStack:Stack[Document] = new Stack[Document]();
			
            async{
				Console.OUT.println("Chunk " + (lim-offset) + " Offset " + offset + " lim " + lim);
                for (var j:long = offset ; j < lim ; j++ ) {
                    val f:File = new File(dirList(j));
                    val wordStack:Stack[Long] = new Stack[Long]();
                    val reader = f.openRead();
                
                    for (val line in reader.lines()) {
                        for (w in line.split(" ")) {
                            val wStr = w.trim().toLowerCase();
                            if (!wStr.equals(""))
                                wordStack.push(vocab.getIndex(wStr));
                        }
                    }
                    reader.close();

                    val words:Rail[Long] = new Rail[Long](wordStack.size(), (i:Long) => wordStack.pop());
                    val topics:Rail[Long] = new Rail[Long](words.size);
                    docStack.push(new Document(words, topics));
                }
				docFrags.add(new Rail[Document](docStack.size(), (x:Long) => docStack.pop()));
				
            }
       
        }

    }

    public def size() : Long {
        var total:Long = 0;
        for (frag in docFrags) {
            Console.OUT.println("In this frag there are "+frag.size+" files.");
            total += frag.size;
        }
        return total;
    }

}
