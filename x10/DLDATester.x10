import x10.io.File;
import x10.util.Timer;
import x10.util.ArrayList;
import x10.array.Array_2;

public class DLDATester {


    public static def main(args:Rail[String]) {

        /* Timing info*/
        var ioTime:Long;
        var initTime:Long;
        val sampleTime:GlobalRef[Cell[Long]] = new GlobalRef[Cell[Long]](new Cell[Long](0));
        val syncTime:GlobalRef[Cell[Long]] = new GlobalRef[Cell[Long]](new Cell[Long](0));
        val transmitTime:GlobalRef[Cell[Long]] = new GlobalRef[Cell[Long]](new Cell[Long](0));
        
        var dataDir:File = null;
        val niters:Long = (args.size > 1) ? Long.parseLong(args(1)) : 1000;
        val ntopics:Long = (args.size > 2) ? Long.parseLong(args(2)) : 20;
        var topn:Long = (args.size > 3) ? Long.parseLong(args(3)) : 10;
        val nthreads:Long = (args.size > 4) ? Long.parseLong(args(4)) : 1;
        var nplaces:Long = (args.size > 5) ? Long.parseLong(args(5)) : 1;
        val localSyncRate = (args.size > 6) ? Long.parseLong(args(6)) : 2;
        val globalSyncRate = (args.size > 6) ? Long.parseLong(args(7)) : 10;

        if (args.size < 1) {
            Console.OUT.println("LDATester [DOC-DIR [ITERATIONS [NTOPICS [TOPN [NTHREADS [NPLACES [LOCALSYNCRATE [GLOBALSYNCRATE]]]]]]]]");
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
        val vocab = Vocabulary.buildVocabParallel(fileList, nthreads);
        val vocabPlh = PlaceLocalHandle.make[Vocabulary](places, () => vocab);

        Console.OUT.println("Reading documents...");
        val ddocs:DistDocuments = new DistDocuments(vocabPlh, fileList, places, nthreads);

        ioTime = Timer.milliTime() - ioStart;

        val ntypes = vocab.size();
        //val typeTopicWorldCounts = PlaceLocalHandle.make[Array_2[Long]](places, () => new Array_2[Long](ntypes, ntopics));
        //val typeTopicWorldTotals = PlaceLocalHandle.make[Rail[Long]](places, () => new Rail[Long](ntopics));


        /** INIT MATRICES **/

        val initStart = Timer.milliTime();
        
        val dlda:PlaceLocalHandle[DistLDANode] = 
            PlaceLocalHandle.make[DistLDANode](places, () => new DistLDANode(places,                                                                             
                                                                             vocabPlh(), 
                                                                             ddocs.plh()(),
                                                                             ntopics,
                                                                             50.0,
                                                                             0.01,
                                                                             nthreads));
        
        for (p in places) {
            at (p) {
                dlda().setNodes(dlda);
            }
        }

        finish for (p in places) {
            at (p) {
                async {
                    dlda().init();
                }
            }
        }

        initTime = Timer.milliTime() - initStart;
        Console.OUT.println("INIT COMPLETE");
        //var done:GlobalRef[Cell[Boolean]] = new GlobalRef[Cell[Boolean]](new Cell[Boolean](false));
        for (p in places) {
            at (p) {
                Console.OUT.println(p);
                Console.OUT.println(dlda().done);
                Console.OUT.println(dlda().visited);
                Console.OUT.println("\n");
            }
        }

        
        initTime = Timer.milliTime() - initStart;
        for (p in places) {
            async at (p) {
                dlda().sample(niters, localSyncRate, globalSyncRate);
                //dlda().sample(niters, localSyncRate, globalSyncRate);
            }
        }

        val done:GlobalRef[Cell[Boolean]] = new GlobalRef[Cell[Boolean]](new Cell[Boolean](false));
        while (!done.getLocalOrCopy()()) {
            done.getLocalOrCopy()() = true;
            for (p in places) {
                at (p) {
                    val workerDone = dlda().done; 
                    if (!workerDone) {
                        at (done.home) {
                            done()() = false; 
                        }
                    }

                }
            }
        }


        for (p in places) {

            at (p) {
                val samTime = dlda().getTotalSampleTime();
                val synTime = dlda().getTotalResyncTime();
                val transTime = dlda().getTotalTransmitTime();

                at (sampleTime.home) {
                    sampleTime()() += samTime;
                }

                at (syncTime.home) {
                    syncTime()() += synTime;
                }

                at (transmitTime.home) {
                    transmitTime()() += transTime;
                }
            }
        }

        /** DISPLAY **/
        
        Console.OUT.println("TOP "+topn+" WORDS BY TOPIC\n=======================================\n");
        for (var t:Long = 0; t < ntopics; t++)
            dlda().displayTopWords(topn, t);
        Console.OUT.println();
      
        Console.OUT.println("Model Log Likelihood: "+logLikelihood(dlda, places));     
        
        Console.OUT.println();
        Console.OUT.println("Time breakdown\n==============\n\n");
        Console.OUT.println("File IO Time       :   "+ioTime);
        Console.OUT.println("Matrix Init Time   :   "+initTime);
        Console.OUT.println("Sample Time        :   "+sampleTime.getLocalOrCopy()());
        Console.OUT.println("Sync Time          :   "+syncTime.getLocalOrCopy()());
        Console.OUT.println("Transmit Time      :   "+transmitTime.getLocalOrCopy()());

        for (p in places) {
            at (p) {   
                Console.OUT.println(p+" "+dlda().exchanges);
            }
        }

        

   }

   public static def logLikelihood(nodes:PlaceLocalHandle[DistLDANode], world:PlaceGroup.SimplePlaceGroup) : Double {


        val ntopics = nodes().ntopics; 
        val alphaSum = nodes().alphaSum;
        val alpha = nodes().alpha;
        val beta = nodes().beta;
        val betaSum = nodes().betaSum;
        val ntypes = nodes().ntypes;
        var logLikelihood:Double = 0.0;
        
        val placeLLRef:GlobalRef[Cell[Double]] = new GlobalRef[Cell[Double]](new Cell[Double](0.0));

        val ndocs:GlobalRef[Cell[Long]] = new GlobalRef[Cell[Long]](new Cell[Long](0)); 
        for (p in world) {
            at (p) {
                val node = nodes(); 
                val ndocsVal = node.ndocs;
                at (ndocs.home) {
                    ndocs()() += ndocsVal;
                }
                for (worker in node.workers) {

                    for (var d:Long = 0; d < worker.ndocs; d++) {
                        for (var t:Long = 0; t < ntopics; t++) {
                            if (worker.docTopicCounts(d,t) > 0) {
                                val c = worker.docTopicCounts(d,t);
                                at (placeLLRef.home) {
                                    placeLLRef()() += (MathUtils.logGamma(alpha + c))
                                                        - MathUtils.logGamma(alpha);
                                }
                            }
                        }
                        val doctokens = worker.docs(d).size;
                        at (placeLLRef.home) {
                            placeLLRef()() -= MathUtils.logGamma(alphaSum + doctokens);
                        }
                    }
                }
            }
        }
       
        
        logLikelihood += placeLLRef.getLocalOrCopy().value;
        logLikelihood += ndocs.getLocalOrCopy().value * MathUtils.logGamma(alphaSum);
        //Console.OUT.println("NDOCS : "+ndocs()());
        var nonZeroTypeTopics:Long = 0;
        for (var w:Long = 0; w < ntypes; w++) {
            for (var t:Long = 0; t < ntopics; t++) {
                if (nodes().workers(0).typeTopicCountsLocal(w,t) 
                    + nodes().workers(0).typeTopicCountsGlobal(w,t)
                    + nodes().workers(0).typeTopicWorldCounts(w,t)  == 0) continue;
                nonZeroTypeTopics++;
                logLikelihood += MathUtils.logGamma(beta + nodes().workers(0).typeTopicCountsLocal(w,t) 
                                                         + nodes().workers(0).typeTopicCountsGlobal(w,t)
                                                         + nodes().workers(0).typeTopicWorldCounts(w,t));
            }
        }

        for (var t:Long = 0; t < ntopics; t++) {
            logLikelihood -= MathUtils.logGamma((beta * ntopics) + nodes().workers(0).totalTypesPerTopicLocal(t) 
                                                                 + nodes().workers(0).totalTypesPerTopicGlobal(t)
                                                                 + nodes().workers(0).typeTopicWorldTotals(t));
        }

        logLikelihood += MathUtils.logGamma(beta * ntopics) - (MathUtils.logGamma(beta)* nonZeroTypeTopics);

        return logLikelihood;
    }




}
