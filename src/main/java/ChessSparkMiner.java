import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ChessSparkMiner {
    public static void main(String[] args) {
        String pgnFile = "/Users/bartoszjedrzejewski/github/chesssparkminer/lichess_db_standard_rated_2013-01.pgn"; // Should be some file on your system
        SparkConf conf = new SparkConf()
                .setAppName("Chess Spark Miner")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("textinputformat.record.delimiter", "[Event");
        JavaRDD<String> pgnData = sc.textFile(pgnFile);

        pgnData = pgnData.filter(line -> line.length() > 1);

        long records = pgnData.count();
        long whiteWin = pgnData.filter(s -> s.contains("1-0")).count();
        long blackWin = pgnData.filter(s -> s.contains("0-1")).count();
        long draw = pgnData.filter(s -> s.contains("1/2-1/2")).count();

        System.out.println("Processed games: " + records);
        System.out.println("White wins: " + whiteWin);
        System.out.println("Black wins: " + blackWin);
        System.out.println("Draw: " + draw);

        sc.stop();
    }

}
