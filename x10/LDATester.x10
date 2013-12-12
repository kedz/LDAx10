import x10.io.File;
import x10.util.Timer;

public class LDATester {


    public static def main(args:Rail[String]) {
        var dataDir:File = null;
        var niters:Long = 1000;
        var ntopics:Long = 20;
        var topn:Long = 10;
        if (args.size < 1) {
            Console.OUT.println("LDATester [DOC-DIR] [ITERATIONS]");
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

        Console.OUT.println("Creating document vocabulary...");
        var ioStart:Long = Timer.milliTime();
        val vocab:Vocabulary = new Vocabulary(dataDir); 
        val docs:Documents = new Documents(vocab, dataDir);
        val ioTime:Long = Timer.milliTime() - ioStart;
        Console.OUT.println(ioTime);


        var sampleStart:Long = Timer.milliTime();
        var slda:SerialLDA = new SerialLDA(vocab, docs.docs, ntopics, 50.0, 0.01);
        
        Console.OUT.println(slda); 
        slda.sample(niters);
        val sampleTime:Long = Timer.milliTime() - sampleStart;
        for (var t:Long = 0; t < ntopics; t++)
            slda.displayTopN(topn, t);
        Console.OUT.println(sampleTime);

        //Console.OUT.println(vocab.wordIndexMap);
       

    }



}
