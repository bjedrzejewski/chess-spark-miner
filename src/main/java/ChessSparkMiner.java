import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChessSparkMiner {
    public static void main(String[] args) throws IOException {
        consolidateOpeningFiles("/Users/bartoszjedrzejewski/github/chesssparkminer/eloOpeningFiles/*OpeningsFile");
        //processPGNFile("/Users/bartoszjedrzejewski/github/chesssparkminer/lichess_db_standard_rated_2017-07.pgn");
    }

    private static void consolidateOpeningFiles(String dirPath) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("Chess Spark Miner")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("INFO");
        JavaRDD<String> pgnData = sc.textFile(dirPath);
        pgnData = pgnData.filter(line -> !line.contains("opening|eco|tempo")); //remove the header lines
        pgnData = pgnData.filter(line -> line.length() > 10);

        JavaPairRDD<GameKey, ScoreCount> openingToGameScore = pgnData
                .mapToPair(line -> createTupleFromLine(line))
                .reduceByKey((score1, score2) -> score1.add(score2));

        saveAnalyzedOpeningsToFile(openingToGameScore.collect(), "consolidatedOpeningFile");

    }

    private static Tuple2<GameKey, ScoreCount> createTupleFromLine(String line){
        String[] parts = line.split("\\|");
        GameKey gameKey = new GameKey(parts[0], parts[1], parts[2], parts[3], parts[4]);
        ScoreCount scoreCount = new ScoreCount(Double.parseDouble(parts[6]),
                Double.parseDouble(parts[7]),
                Double.parseDouble(parts[8]),
                Double.parseDouble(parts[9]),
                Double.parseDouble(parts[10]),
                Double.parseDouble(parts[13]),
                Double.parseDouble(parts[14]));
        return new Tuple2<>(gameKey, scoreCount);
    }

    private static void processPGNFile(String filePath) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("Chess Spark Miner")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("INFO");
        sc.hadoopConfiguration().set("textinputformat.record.delimiter", "[Event");
        JavaRDD<String> pgnData = sc.textFile(filePath);

        pgnData = pgnData.filter(line -> line.length() > 1);

        computeGameStats(pgnData);

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

    private static void computeGameStats(JavaRDD<String> pgnData) throws IOException {
        JavaPairRDD<GameKey, ScoreCount> openingToGameScore = pgnData
                .mapToPair(game -> new Tuple2<>(createGameKey(game), new ScoreCount(getScore(game), 1, getWhiteElo(game), getBlackElo(game))))
                .reduceByKey((score1, score2) -> score1.add(score2));

        //openingToGameScore = openingToGameScore.filter(a -> a._2.count > 100);

        List<Tuple2<GameKey, ScoreCount>> analyzedOpenings = openingToGameScore.collect();
        analyzedOpenings = new ArrayList<>(analyzedOpenings);
        analyzedOpenings.sort((a, b) -> Double.compare(a._2.getCount(),b._2.getCount()));

        //write out the analyzed openings
        saveAnalyzedOpeningsToFile(analyzedOpenings, "openingsFile");
    }

    private static void saveAnalyzedOpeningsToFile(List<Tuple2<GameKey, ScoreCount>> analyzedOpenings, String fileName) throws IOException {
        FileWriter writer = new FileWriter(fileName);
        writer.write(GameKey.getFileHeader()+"|"+ScoreCount.getFileHeader()+"\n");
        for(Tuple2<GameKey, ScoreCount> tuple : analyzedOpenings){
            writer.write(tuple._1.toString()+"|"+tuple._2.toString()+"\n");
        }
        writer.close();
    }

    private static GameKey createGameKey(String game) {
        return new GameKey(getOpening(game),
                getPgnField(game, "ECO"),
                getSpeedMode(game),
                getAvgEloClass(game),
                getRatingDiffClass(game)
                );
    }

    public static double getWhiteElo(String game){
        String whiteEloS = getPgnField(game, "WhiteElo");
        if(whiteEloS.contains("?")){
            return 0;
        }
        return Double.parseDouble(whiteEloS);
    }

    public static double getBlackElo(String game){
        String blackEloS = getPgnField(game, "BlackElo");
        if(blackEloS.contains("?")){
            return 0;
        }
        return Double.parseDouble(blackEloS);
    }

    private static String getRatingDiffClass(String game) {
        String whiteEloS = getPgnField(game, "WhiteElo");
        String blackEloS = getPgnField(game, "BlackElo");
        if(whiteEloS.contains("?") || blackEloS.contains("?")){
            return "?";
        }
        double whiteElo = Double.parseDouble(whiteEloS);
        double blackElo = Double.parseDouble(blackEloS);
        double diff = Math.abs(whiteElo - blackElo);
        String stronger = whiteElo > blackElo ? "White" : "Black";
        if(diff < 100){
            return "White=Black";
        } else if(diff < 300){
            return stronger+"+200";
        } else if(diff < 500){
            return stronger+"+400";
        } else {
            return stronger+"+500+";
        }
    }

    private static String getAvgEloClass(String game) {
        String whiteEloS = getPgnField(game, "WhiteElo");
        String blackEloS = getPgnField(game, "BlackElo");
        if(whiteEloS.contains("?") || blackEloS.contains("?")){
            return "?";
        }
        double whiteElo = Double.parseDouble(whiteEloS);
        double blackElo = Double.parseDouble(blackEloS);
        double average = (whiteElo+blackElo)/2;
        if(average < 1200){
            return "0-1199";
        } else if(average < 1400){
            return "1200-1399";
        } else if(average < 1600){
            return "1400-1599";
        } else if(average < 1800){
            return "1600-1799";
        } else if(average < 2000){
            return "1800-1999";
        } else if(average < 2200){
            return "2000-2199";
        } else if(average < 2400){
            return "2200-2399";
        } else
            return "2400+";
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

    public static String getPgnField(String pgn, String field){
        pgn = pgn.substring(pgn.indexOf(field));
        pgn = pgn.substring(pgn.indexOf("\"")+1);
        pgn = pgn.substring(0, pgn.indexOf("\""));
        if(pgn.contains("\n"))
            return "?";
        return pgn;
    }


    public static String getOpening(String pgn){
        return getPgnField(pgn, "Opening");
    }

    public static double getScore(String pgn){
        if(pgn.contains("1-0"))
            return 1;
        else if (pgn.contains("1/2-1/2"))
            return 0.5;
        else
            return 0;
    }
}
