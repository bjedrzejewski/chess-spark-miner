import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class ChessSparkMiner {
    public static void main(String[] args) {
        String pngFile = "/Users/bartoszjedrzejewski/github/chesssparkminer/lichess_db_standard_rated_2013-01.pgn"; // Should be some file on your system
        SparkSession spark = SparkSession.builder()
                .appName("Chess Spark Miner")
                .config(new SparkConf().setMaster("local[2]"))
                .getOrCreate();
        Dataset<String> logData = spark.read().textFile(pngFile).cache();

        long whiteWin = logData.filter(s -> s.contains("1-0")).count();
        long blackWin = logData.filter(s -> s.contains("0-1")).count();
        long draw = logData.filter(s -> s.contains("1/2-1/2")).count();

        System.out.println("White wins: " + whiteWin);
        System.out.println("Black wins: " + blackWin);
        System.out.println("Draw: " + draw);

        spark.stop();
    }

}
