import x10.io.File;
import x10.util.Timer;
import x10.util.ArrayList;

public class DLDATester {


    public static def main(args:Rail[String]) {

        /* Timing info*/
        var ioTime:Long;
        var initTime:Long;
        var sampleTime:Long;
        var syncTime:Long;
        

        var dataDir:File = null;
        val niters:Long = (args.size > 1) ? Long.parseLong(args(1)) : 1000;
        val ntopics:Long = (args.size > 2) ? Long.parseLong(args(2)) : 20;
        var topn:Long = (args.size > 3) ? Long.parseLong(args(3)) : 10;
        val nthreads:Long = (args.size > 4) ? Long.parseLong(args(4)) : 1;
        var nplaces:Long = (args.size > 5) ? Long.parseLong(args(5)) : 1;

        if (args.size < 1) {
            Console.OUT.println("LDATester [DOC-DIR [ITERATIONS [NTOPICS [TOPN [NTHREADS [NPLACES]]]]]]");
            System.killHere();
        } else {
            dataDir = new File(args(0));
            if (!dataDir.isDirectory()) {
                Console.OUT.println("Data directory: "+dataDir+ " does not exist.");
                System.killHere();
            }

        }

        val places = PlaceGroup.make(nplaces);
        Console.OUT.println("Running in "+places.numPlaces()+" places.");


         /** FILE IO **/

        var ioStart:Long = Timer.milliTime();
        
        Console.OUT.println("Creating document vocabulary...");
        val fileList = dataDir.list();
        val vocabPlh = PlaceLocalHandle.make[Vocabulary](places, () => Vocabulary.buildVocabParallel(fileList, nthreads));

        Console.OUT.println("Reading documents...");
        val ddocs:DistDocuments = new DistDocuments(vocabPlh, fileList, places, nthreads);

        ioTime = Timer.milliTime() - ioStart;


        /** INIT MATRICES **/

        val dlda:PlaceLocalHandle[DistLDANode] = 
            PlaceLocalHandle.make[DistLDANode](places, () => new DistLDANode(places,
                                                                             vocabPlh(), 
                                                                             ddocs.plh()(),
                                                                             ntopics,
                                                                             50.0,
                                                                             0.01,
                                                                             nthreads));
        
        
        finish for (p in places) {
            at (p) {
                async {
                    dlda().init();
                    dlda().sample(niters);
                }
            }
        }

        /*
        for (p in places) {
            Console.OUT.println("AT PLACE "+p);
            at (p) {
                         
                for (var t:Long = 0; t < ddocs.plh()().size(); t++) {
                    Console.OUT.println("\t Thread "+t);    
                    for (doc in ddocs.plh()().get(t)) {
                        for (w in doc.words) {
                            Console.OUT.print(vocabPlh().getWord(w)+" ");    
                        }
                        Console.OUT.println("\n");
                    }
                    Console.OUT.println("\nttttttttttttttttttt\n\n");
                }   
                Console.OUT.println("\nppppppppppppppppppppppp\n\n"); 
            }
        }
        */

        /** DISPLAY **/
        
        //for (var t:Long = 0; t < ntopics; t++)
        //    plda.displayTopWords(topn, t);
        
        Console.OUT.println("Time breakdown\n==============\n\n");
        Console.OUT.println("File IO Time       :   "+ioTime);
        Console.OUT.println("Matrix Init Time   :   "+"?");//initTime);
        Console.OUT.println("Sample Time        :   "+"?");//sampleTime);
        Console.OUT.println("Sync Time          :   "+"?");//syncTime);
 

   }
}
