import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ChessSparkMiner {
    public static void main(String[] args) {
        String pgnFile = "/Users/bartoszjedrzejewski/github/chesssparkminer/lichess_db_standard_rated_2013-01.pgn"; // Should be some file on your system
        SparkConf conf = new SparkConf()
                .setAppName("Chess Spark Miner")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        sc.hadoopConfiguration().set("textinputformat.record.delimiter", "[Event");
        JavaRDD<String> pgnData = sc.textFile(pgnFile);

        pgnData = pgnData.filter(line -> line.length() > 1);


        JavaRDD<String> ultraBullet = pgnData.filter(a -> getSpeedMode(a).equals("UltraBullet"));
        JavaRDD<String> bullet = pgnData.filter(a -> getSpeedMode(a).equals("Bullet"));
        JavaRDD<String> blitz = pgnData.filter(a -> getSpeedMode(a).equals("Blitz"));
        JavaRDD<String> classical = pgnData.filter(a -> getSpeedMode(a).equals("Classical"));

        System.out.println("ultraBullet performance");
        printOpeningsPerformance(ultraBullet);

        System.out.println("bullet performance");
        printOpeningsPerformance(bullet);

        System.out.println("blitz performance");
        printOpeningsPerformance(blitz);

        System.out.println("classical performance");
        printOpeningsPerformance(classical);

        sc.stop();
    }

    private static void printWinDrawLose(JavaRDD<String> pgnData) {
        long records = pgnData.count();
        long whiteWin = pgnData.filter(s -> s.contains("1-0")).count();
        long blackWin = pgnData.filter(s -> s.contains("0-1")).count();
        long draw = pgnData.filter(s -> s.contains("1/2-1/2")).count();

        System.out.println("Processed games: " + records);
        System.out.println("White wins: " + whiteWin);
        System.out.println("Black wins: " + blackWin);
        System.out.println("Draw: " + draw);
    }

    private static void printOpeningsPerformance(JavaRDD<String> pgnData) {
        JavaPairRDD<String, ScoreCount> openingToGameScore = pgnData
                .mapToPair(game -> new Tuple2<>(getOpening(game), new ScoreCount(getScore(game), 1)))
                .reduceByKey((score1, score2) -> score1.add(score2));

        openingToGameScore = openingToGameScore.filter(a -> a._2.count > 100);

        List<Tuple2<String, ScoreCount>> gamesToScore = openingToGameScore.collect();
        gamesToScore = new ArrayList<>(gamesToScore);
        gamesToScore.sort((a, b) -> a._2.getAverageScore() > b._2.getAverageScore() ? 1 : -1);
        for(Tuple2<String, ScoreCount> gameToScore : gamesToScore){
            System.out.printf("%s : %.3f from %.0f games%n", gameToScore._1 ,gameToScore._2.getAverageScore(), gameToScore._2.count);
        }
    }

    public static String getSpeedMode(String pgn){
        pgn = pgn.substring(0, pgn.indexOf("]")); //to avoid words appearing in player names
        if(pgn.contains("UltraBullet"))
            return "UltraBullet";
        else if(pgn.contains("Bullet"))
            return "Bullet";
        else if(pgn.contains("Blitz"))
            return "Blitz";
        else if(pgn.contains("Classical"))
            return "Classical";
        else
            return "?";
    }


    public static String getOpening(String pgn){
        int i = pgn.indexOf("[Opening \"");
        pgn = pgn.substring(i+10);
        int e = pgn.indexOf("\"");
        return pgn.substring(0, e);
    }

    public static double getScore(String pgn){
        if(pgn.contains("1-0"))
            return 1;
        else if (pgn.contains("1/2-1/2"))
            return 0.5;
        else
            return 0;
    }

    public static class ScoreCount implements java.io.Serializable{

        private final double score;
        private final double count;

        public ScoreCount(double score, double count) {
            this.score = score;
            this.count = count;
        }

        public double getAverageScore() {
            return score/count;
        }

        public double getScore() {
            return score;
        }

        public double getCount() {
            return count;
        }

        public ScoreCount add(ScoreCount a){
            return new ScoreCount(a.score + score, count + a.count);
        }
    }
}
