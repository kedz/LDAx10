import x10.io.File;
import x10.util.Timer;

public class PLDATester {


    public static def main(args:Rail[String]) {

        /* Timing info*/
        var ioTime:Long;
        var initTime:Long;
        var sampleTime:Long;
        var syncTime:Long;
        

        var dataDir:File = null;
        var niters:Long = 1000;
        var ntopics:Long = 20;
        var topn:Long = 10;
        var nthreads:Long = (args.size > 4) ? Long.parseLong(args(4)) : 1;

        if (args.size < 1) {
            Console.OUT.println("LDATester [DOC-DIR [ITERATIONS [NTOPICS [TOPN [NTHREADS]]]]]");
            System.killHere();
        } else {
            dataDir = new File(args(0));
            if (!dataDir.isDirectory()) {
                Console.OUT.println("Data directory: "+dataDir+ " does not exist.");
                System.killHere();
            }

        }

        if (args.size > 1) {
            niters = Long.parseLong(args(1));
        }

        if (args.size > 2) {
            ntopics = Long.parseLong(args(2));
        }

        if (args.size > 3) 
            topn = Long.parseLong(args(3));


        /** FILE IO **/

        var ioStart:Long = Timer.milliTime();
        
        Console.OUT.println("Creating document vocabulary...");
        val vocab:Vocabulary = new Vocabulary(dataDir);
        
        Console.OUT.println("Reading documents...");
        val docs:DocumentsFrags = new DocumentsFrags(vocab, dataDir, nthreads);

        //Console.OUT.println("There are "+dataDir.list().size+" files in the directory");
        //Console.OUT.println("There are "+docs.size()+" in the doc frag.");

        ioTime = Timer.milliTime() - ioStart;

        /** MATRIX INIT **/

        val initStart:Long = Timer.milliTime();
        
        var plda:ParallelLDA = new ParallelLDA(vocab, docs.docFrags, ntopics, 50.0, 0.01, nthreads);
        plda.printReport();  
        plda.init();
        
        initTime = Timer.milliTime() - initStart;
        
        /** SAMPLE **/

        plda.sample(niters);

        Console.OUT.println("Sampling complete!");
        Console.OUT.println();

        sampleTime = plda.getTotalSampleTime();
        syncTime = plda.getTotalResyncTime();
        
        /** DISPLAY **/
        for (var t:Long = 0; t < ntopics; t++)
            plda.displayTopWords(topn, t);
        
        Console.OUT.println("Time breakdown\n==============\n\n");
        Console.OUT.println("File IO Time       :   "+ioTime);
        Console.OUT.println("Matrix Init Time   :   "+initTime);
        Console.OUT.println("Sample Time        :   "+sampleTime);
        Console.OUT.println("Sync Time          :   "+syncTime);
        


    }



}
