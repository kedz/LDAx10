import x10.util.ArrayList;
import x10.util.Stack;

public class DistDocuments {

    val vocabPlh:PlaceLocalHandle[Vocabulary];
    val documentsPlh:PlaceLocalHandle[ArrayList[Rail[Documents.Document]]];

    public def this(vocabPlh:PlaceLocalHandle[Vocabulary], fileList:Rail[String], places:PlaceGroup.SimplePlaceGroup, nthreads:Long) {

        this.vocabPlh = vocabPlh;
        //vocabPlh = PlaceLocalHandle.make[Vocabulary](places, () => vocab);
        
        val fstackPlh = PlaceLocalHandle.make[Stack[String]](places, () => new Stack[String]());
        //val fstacks:Rail[Stack[File]] = new Rail[Stack[File]](places.numPlaces(), (i:Long) => new Stack[File]());
        var cntr:Long = 0;
        for (fname in fileList) {
            val fnameVal:String = fname;
            at (places(cntr % places.numPlaces())) {
                fstackPlh().push(fnameVal);
            }
            cntr++;
        }
        
        documentsPlh = PlaceLocalHandle.make[ArrayList[Rail[Documents.Document]]](places, () => Documents.buildDocumentFragments(vocabPlh(), fstackPlh().pop(fstackPlh().size()), nthreads));   

    }

    public def plh() : PlaceLocalHandle[ArrayList[Rail[Documents.Document]]] {
        return documentsPlh;
    }

}
