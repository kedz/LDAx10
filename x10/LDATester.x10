import x10.io.File;
import x10.util.Timer;

public class LDATester {


    public static def main(args:Rail[String]) {
        
        var ioTime:Long = 0;
        var initTime:Long = 0;
        var sampleTime:Long = 0;
        var runTime:Long = 0;
        
        var dataDir:File = null;
        var niters:Long = (args.size > 1) ? Long.parseLong(args(1)) : 1000;
        var ntopics:Long = (args.size > 2) ? Long.parseLong(args(2)) : 20;
        var topn:Long = (args.size > 3) ? Long.parseLong(args(3)) : 10;
        
        if (args.size < 1) {
            Console.OUT.println("LDATester [DOC-DIR [ITERATIONS [NTOPICS [TOPN]]]]");
            System.killHere();
        } else {
            dataDir = new File(args(0));
            if (!dataDir.isDirectory()) {
                Console.OUT.println("Data directory: "+dataDir+ " does not exist.");
                System.killHere();
            }

        }

        val runtimeStart = Timer.milliTime();

        /** FILE IO **/

        var ioStart:Long = Timer.milliTime();

        Console.OUT.println("Creating document vocabulary...");
        val vocab:Vocabulary = new Vocabulary(dataDir); 
        Console.OUT.println("Creating documents...");
        val docs:Documents = new Documents(vocab, dataDir);
        
        ioTime = Timer.milliTime() - ioStart;

        
        /** INIT MATRICES **/

        var initStart:Long = Timer.milliTime();
        
        Console.OUT.println("Initializing count matricies...");
        var slda:SerialLDA = new SerialLDA(vocab, docs.docs, ntopics, 50.0, 0.01);
        Console.OUT.println(slda+"\n"); 
        slda.init();
        
        initTime = Timer.milliTime() - initStart;
          
        /** SAMPLING **/
          
        var sampleStart:Long = Timer.milliTime();       
        
        slda.sample(niters);
        
        sampleTime = Timer.milliTime() - sampleStart;
        
        runTime = Timer.milliTime() - runtimeStart; 

        /** DISPLAY **/

        Console.OUT.println("TOP "+topn+" WORDS BY TOPIC\n=======================================\n");
        for (var t:Long = 0; t < ntopics; t++)
            slda.displayTopWords(topn, t);
        Console.OUT.println();
       
        Console.OUT.println("Model Log Likelihood: "+slda.logLikelihood()+"\n"); 
       
        Console.OUT.println("Time breakdown\n==============\n");
        Console.OUT.println("File IO Time       :   "+ioTime);
        Console.OUT.println("Matrix Init Time   :   "+initTime);
        Console.OUT.println("Sample Time        :   "+sampleTime);
        Console.OUT.println("Runtime            :   "+runTime);

    }



}
