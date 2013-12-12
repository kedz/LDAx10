public class MathUtils {

    public static val HALF_LOG_TWO_PI:Double = Math.log(2 * Math.PI) / 2.0;

    // Stirling's Approximation
    public static def logGamma(var z:Double) : Double {
        var shift:Long = 0;
        while (z < 2) {
            z++;
            shift++;
        }

        var result:Double = HALF_LOG_TWO_PI + (z - 0.5) * Math.log(z) - z +
            1/(12 * z) - 1 / (360 * z * z * z) + 1 / (1260 * z * z * z * z * z);

        while (shift > 0) {
            shift--;
            z--;
            result -= Math.log(z);
        }

        return result;

    }
    
}
