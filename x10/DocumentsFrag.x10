import x10.util.Stack;
import x10.io.File;

public class DocumentsFrags {

	public static struct DocFrags{
	
		val DocFrags:ArrayList[Rail[Document]];
		val nthreads:long;
		
		public def this( nthreads:long){
			this.DocFrags = new ArrayList[ Rail[Document] ](nthreads);
			this.nthreads = nthreads;
		}
		
	}

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

    public def this(vocab:Vocabulary, dataDir:File, nthreads:long ) {

		val numThreads = new AtomicLong(0);
		val dirSize:long = dataDir.list().size();
		val dirList:long = dataDir.list()
		val chunkSize = (dirSize-1)/nthreads+1;
			
		finish for ( var i:long = 0; i < dirSize ; i+= chunkSize ) {
		
			val lim = (i+chunkSize) > dirSize ? DirSize : i+chunkSize;
			val docStack:Stack[Document] = new Stack[Document]();
			
			for( var j:long = i ; j < lim ; j++ ) async{
				var f:File = new File(dirList(j));
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
			
				DocFrags.set( new Rail[Document](docStack.size(), (i:Long) => docStack.pop()) ,i);
		
			}
		}
       
    }


}
