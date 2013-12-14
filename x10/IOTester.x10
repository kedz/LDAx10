import x10.io.File;
import x10.util.Timer;
import x10.util.ArrayList;

public class IOTester {

    public static def main(args:Rail[String]) {

        /* Timing info*/
        var vocabTimeSer:Long;
        var vocabTimePar:Long;
        var vocabTimeDist:Long;
        var loadDocsSer:Long;
        var loadDocsPar:Long;
        var loadDocsFrag:Long;
        var loadDocsDist:Long;
        

        val dataDir:File = (args.size > 0) ? new File(args(0)) : new File("./data");
        var niters:Long = (args.size > 1) ? Long.parseLong(args(1)) : 1000;
        var ntopics:Long = (args.size > 2) ? Long.parseLong(args(2)) : 20;
        var topn:Long = (args.size > 3) ? Long.parseLong(args(3)) : 10;
        val nthreads:Long = (args.size > 4) ? Long.parseLong(args(4)) : 1;
        var nplaces:Long = (args.size > 5) ? Long.parseLong(args(5)) : 1;

        /*
        if (args.size < 1) {
            Console.OUT.println("LDATester [DOC-DIR [ITERATIONS [NTOPICS [TOPN [NTHREADS [NPLACES]]]]]]");
            System.killHere();
        } else {
            dataDir = new File(args(0));
            

        }
        */
        if (!dataDir.isDirectory()) {
            Console.OUT.println("Data directory: "+dataDir+ " does not exist.");
            System.killHere();
        }

        val places = PlaceGroup.make(nplaces);
        Console.OUT.println("Running in "+places.numPlaces()+" places.");

        var start:Long = 0;

         /** FILE IO **/


        Console.OUT.println("Creating document vocabulary...");
        
        start = Timer.milliTime();
        val vocabSer:Vocabulary = new Vocabulary(dataDir);
        vocabTimeSer = Timer.milliTime() - start;

        start = Timer.milliTime();
        val vocabPar:Vocabulary = Vocabulary.buildVocabParallel(dataDir.list(), nthreads);
        vocabTimePar = Timer.milliTime() - start;
       
        start = Timer.milliTime();
        val vocabPlh = PlaceLocalHandle.make[Vocabulary](places, () => Vocabulary.buildVocabParallel(dataDir.list(), nthreads));
        vocabTimeDist = Timer.milliTime() - start;

        Console.OUT.println("Reading documents...");

        start = Timer.milliTime();
        val docsSer:Documents = new Documents(vocabSer, dataDir);
        loadDocsSer = Timer.milliTime() - start;        
        
        start = Timer.milliTime();
        val docsPar:ArrayList[Rail[Documents.Document]] = Documents.buildDocumentFragments(vocabSer, dataDir.list(), nthreads);
        loadDocsPar = Timer.milliTime() - start;        

        start = Timer.milliTime();
        val docsFrag:DocumentsFrags = new DocumentsFrags(vocabSer, dataDir, nthreads);
        loadDocsFrag = Timer.milliTime() - start;        
 
        start = Timer.milliTime();
        val ddocs:DistDocuments = new DistDocuments(vocabPlh, dataDir.list(), places, nthreads);
        loadDocsDist = Timer.milliTime() - start;        
 

        //val fileList = dataDir.list();
        

        //var documentFragments:ArrayList[Rail[Documents.Document]] = Documents.buildDocumentFragments(vocab, fileList, nthreads);


        //val ddocs:DistDocuments = new DistDocuments(vocabPlh, fileList, places, nthreads);

        //Console.OUT.println("There are "+dataDir.list().size+" files in the directory");
        //Console.OUT.println("There are "+docs.size()+" in the doc frag.");

        //ioTime = Timer.milliTime() - ioStart;

        Console.OUT.println("Time breakdown\n==============\n");
        Console.OUT.println("Vocab Serial       :   "+vocabTimeSer);
        Console.OUT.println("Vocab Parallel     :   "+vocabTimePar);
        Console.OUT.println("Vocab Distributed  :   "+vocabTimeDist);
        Console.OUT.println("Docs Serial        :   "+loadDocsSer);
        Console.OUT.println("Docs Parallel      :   "+loadDocsPar);
        Console.OUT.println("Docs Frags         :   "+loadDocsFrag);
        Console.OUT.println("Docs Distributed   :   "+loadDocsDist);

    }
}

