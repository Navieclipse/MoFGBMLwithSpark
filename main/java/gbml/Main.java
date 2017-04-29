package gbml;

import java.util.Date;

import javax.management.JMException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import methods.CommandLineFunc;
import methods.MersenneTwisterFast;
import methods.OsSpecified;
import methods.Output;
import methods.ResultMaster;
import methods.StaticFuzzyFunc;
import methods.StaticGeneralFunc;
import moead.Moead;
import nsga2.Nsga2;
import time.TimeWatch;

public class Main {

	public static void main(String[] args) throws JMException {

		System.out.print("ver." + 15.0);

		int osType = 0;
		if(OsSpecified.isLinux()==true || OsSpecified.isMac()==true){
			osType = Consts.UNIX;		//linux mac
			System.out.println(" OS: Linux or Mac");
		}else{
			osType = Consts.WINDOWS;		//win
			System.out.println(" OS: Windows");
		}
		/******************************************************************************/
		//コマンドライン引数が足りてるかどうか
		CommandLineFunc.lessArgs(args, 13);

	    String dataName = args[0];
	    int generationNum = Integer.parseInt(args[1]);
	    int objectives = Integer.parseInt(args[2]);
	    int divideNum = Integer.parseInt(args[3]);
	    int emoType = Integer.parseInt(args[4]);

	    int populationSize = Integer.parseInt(args[5]);

	    int crossValidationNum = Integer.parseInt(args[6]);
	    int repeatTimes = Integer.parseInt(args[7]);

	    int Seed = Integer.parseInt(args[8]);

	    //このmasterはネームノードのアドレス名が入る
	    String Master = args[9];

	    //Sparkの実行をWebから見るときの名前の指定
	    String AppName = args[10];

	    int PartitionSize = Integer.parseInt(args[11]);

	    //分散環境かどうか
	    boolean isDistributed = Boolean.parseBoolean(args[12]);
	    if (isDistributed) osType = Consts.HDFS;

	    /******************************************************************************/
	    //HDFSにおけるフォルダ
	    String hdfs = "";
	    if(osType==Consts.HDFS) hdfs = args[13];

	    //number of executor
	    int executors = 0;
	    if(osType==Consts.HDFS) executors = Integer.parseInt(args[14]);

	    //number of executor per cores
	    int exeCores = 0;
	    if(osType==Consts.HDFS) exeCores = Integer.parseInt(args[15]);

	    //1回ずつ分ける（メモリのうまい使い方が不明）
	    boolean isOne = true;
	    if(osType==Consts.HDFS) isOne = Boolean.parseBoolean(args[16]);

		/******************************************************************************/
	    //ファジィ分割の生成
	    StaticFuzzyFunc kk = new StaticFuzzyFunc();
	    kk.KKkk(Consts.MAX_FUZZY_DIVIDE_NUM);
	    /******************************************************************************/
	    //基本データ出力と実行（一回かまとめてか）

	    Date date = new Date();
		System.out.print("START: ");
		System.out.println(date);
		System.out.println("Processors:" + Runtime.getRuntime().availableProcessors()+ " ");

		SparkSession spark = SparkSession.builder().master(Master).appName(AppName).getOrCreate();
		System.out.println( "Spark version: " + spark.version() );

		for(int i=0; i<args.length; i++){
			System.out.print(args[i] + " ");
		}
		System.out.println();

		if(isOne){
			One(Seed, executors, exeCores, PartitionSize, dataName, hdfs, objectives, generationNum, divideNum, spark, emoType, populationSize, crossValidationNum, repeatTimes, osType, args);
		}else{
			CC(Seed, executors, exeCores, PartitionSize, dataName, hdfs, objectives, generationNum, divideNum, spark, emoType, populationSize, crossValidationNum, repeatTimes, osType, args);
		}
		/******************************************************************************/
	}

	static public void One(int Seed, int executors, int exeCores, int PartitionSize, String dataName, String hdfs,
			  int objectives, int gen,int dpop, SparkSession spark, int func, int Npop, int CV, int Rep, int os, String[] args){

		/************************************************************/
		//読み込みファイル名とディレクトリ名
		String traFile = Output.makeFileNameOne(dataName, hdfs, CV, Rep, true);
		String tstFile = Output.makeFileNameOne(dataName, hdfs, CV, Rep, false);
		String resultDir = Output.makeDirName(dataName, hdfs, executors, exeCores, Seed, os);

		//実験パラメータ出力 + ディレクトリ作成
		if(CV == 0 && Rep == 0){
			String st = StaticGeneralFunc.getExperimentSettings(args);
			resultDir = Output.makeDir(dataName, hdfs, executors, exeCores, Seed, os);
			Output.makeDirRule(resultDir, os);
			Output.writeExp(dataName, resultDir, st, os);
	    }

		//出力専用クラス
		ResultMaster res = new ResultMaster(resultDir, os);
		/************************************************************/
		//繰り返しなし
		int j = Rep;
		int i = CV;

		MersenneTwisterFast rand = new MersenneTwisterFast(Seed);

		System.out.print(j + " " + i);
		pall(traFile, tstFile, spark, PartitionSize, rand, objectives, gen, func, Npop, res, i, j, os);
		System.out.println();

		/************************************************************/
		//出力
		res.writeAveTime();
		res.writeBestAve();
		Date end = new Date();
		System.out.println("END: " + end);
		/************************************************************/

	}

	static public void CC(int Seed, int executors, int exeCores, int PartitionSize, String dataName, String hdfs,
						  int objectives, int gen,int dpop, SparkSession spark, int func, int Npop,
						  int CV, int Rep, int os, String[] args){

		/************************************************************/
		//読み込みファイル名
		String traFiles[][] = new String[Rep][CV];
	    String tstFiles[][] = new String[Rep][CV];
	    Output.makeFileName(dataName, traFiles, tstFiles, hdfs);

	    //データディレクトリ作成
	    String resultDir;
	    resultDir = Output.makeDir(dataName, hdfs, executors, exeCores, Seed, os);
	    Output.makeDirRule(resultDir, os);

	    //実験パラメータ出力
		String st = StaticGeneralFunc.getExperimentSettings(args);
	    Output.writeExp(dataName, resultDir, st, os);

	    //出力専用クラス
	    ResultMaster res = new ResultMaster(resultDir, os);
	    /************************************************************/
	    //繰り返し
		MersenneTwisterFast rand = new MersenneTwisterFast(Seed);
		//繰り返し回数
		for(int j=0;j<Rep; j++){
			//ＣＶ
			for(int i=0;i<CV; i++){
				System.out.print(j + " " + i);
				pall(traFiles[j][i], tstFiles[j][i], spark, PartitionSize, rand, objectives, gen, func, Npop, res, i, j, os);
				System.out.println();
			}
		}
		/************************************************************/
		//出力
		res.writeAveTime();
		res.writeBestAve();
		Date end = new Date();
		System.out.println("END: " + end);
		/************************************************************/
	}

	static public void pall(String traFile, String testFile, SparkSession spark, int PartitionSize, MersenneTwisterFast rnd,
			int objectives, int gen, int func, int Npop, ResultMaster res ,int CV, int Rep, int os){

		/************************************************************/
		//時間計測開始
		TimeWatch time = new TimeWatch();
		time.start();

		/************************************************************/
		//データを読み込む
		SQLContext sqlc = new SQLContext(spark);

		Dataset<Row> df = sqlc.read()
				.format("com.databricks.spark.csv")
				.option("inferSchema", "true")
				.load(traFile);

		//データを論理的に分割して永続化（高速化のため）
		df.repartition(PartitionSize);
		df.persist( StorageLevel.MEMORY_ONLY() );

		//データの属性数を把握
		int Ndim = df.first().length() - 1;
		int DataSize = (int) df.count();
		String rowName = "_c" + Ndim;
		int Cnum= (int) df.dropDuplicates(rowName).count();

		DataSetInfo traData = new DataSetInfo(DataSize, Ndim, Cnum);

		/************************************************************/
		//ルール初期化
		Classifier ruleset = new Classifier(rnd, objectives);
		ruleset.initialPal(traData, df, Npop);

		//EMOアルゴリズム初期化
		Moead moe = new Moead(Npop, Consts.VECTOR_DIVIDE_NUM, Consts.MOEAD_ALPHA, func, objectives, Consts.SELECTION_NEIGHBOR_NUM, Consts.UPDATE_NEIGHBOR_NUM, rnd);
		Nsga2 nsg = new Nsga2(objectives, rnd);

		//GA操作
		GaManager GA = new GaManager(Npop, ruleset, nsg, moe, rnd, objectives, gen, df, func, res);
		GA.GAFrame(traData, Rep, CV);

		//時間計測終了
		time.end();
		res.setTime(time.getSec());
		res.writeTime(time.getSec(), time.getNano(), CV, Rep);

		//永続化終了（メモリにはまだ残っているのでOutOfMemoryする）
		df.unpersist();

		/***********************これ以降出力操作************************/
		//評価用DataFrame作成
		Dataset<Row> dftst = sqlc.read()
				.format("com.databricks.spark.csv")
				.option("inferSchema", "true")
				.load(testFile);
		int DataSizeTst = (int) dftst.count();
		//テストデータ情報集約
		DataSetInfo tstData = new DataSetInfo(DataSizeTst, Ndim, Cnum);

		RuleSet best = GA.GetBestRuleSet(objectives, ruleset, res, tstData, dftst, true);

		res.setBest(best);
		res.writeAllbest(best, CV, Rep);
		res.outputRules(ruleset, CV, Rep);
		res.outputVec(ruleset, CV, Rep);
		if(objectives != 1){
			res.outSolution(CV, Rep);
			res.resetSolution();
		}
		/************************************************************/
	}


}

