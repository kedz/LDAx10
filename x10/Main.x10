public class Main {

    public static def main(args:Rail[String]) {
        
        



        val alphaSum:Double = 50.0;
        val beta:Double = 0.01;
        val ntopics:Long = 20;

        val topicModel:SerialLDA = new SerialLDA(ntopics, alphaSum, beta);

        Console.OUT.println(topicModel);


    }

}
