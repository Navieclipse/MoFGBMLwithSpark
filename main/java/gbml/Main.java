package gbml;

import java.util.Date;
import java.util.concurrent.ForkJoinPool;

import javax.management.JMException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import methods.CommandLineFunc;
import methods.DataLoader;
import methods.MersenneTwisterFast;
import methods.OsSpecified;
import methods.Output;
import methods.ResultMaster;
import methods.StaticGeneralFunc;
import moead.Moead;
import nsga2.Nsga2;
import time.TimeWatcher;

public class Main {

	public static void main(String[] args) throws JMException {

		System.out.print("ver." + 15.0);

		int osType = 0;
		if(OsSpecified.isLinux()==true || OsSpecified.isMac()==true){
			osType = Consts.UNIX;	//linux mac
			System.out.println(" OS: Linux or Mac");
		}else{
			osType = Consts.WINDOWS;	//win
			System.out.println(" OS: Windows");
		}
		/******************************************************************************/
		//コマンドライン引数が足りてるかどうか
		CommandLineFunc.lessArgs(args, 14);

	    String dataName = args[0];
	    int generationNum = Integer.parseInt(args[1]);
	    int objectiveNum = Integer.parseInt(args[2]);
	    int divideNum = Integer.parseInt(args[3]);	//Spark実行時は使われない．
	    int emoType = Integer.parseInt(args[4]);

	    int populationSize = Integer.parseInt(args[5]);

	    int crossValidationNum = Integer.parseInt(args[6]);
	    int repeatTimes = Integer.parseInt(args[7]);

	    int seed = Integer.parseInt(args[8]);

	    //このmasterはネームノードのアドレス名が入る
	    String masterNodeName = args[9];

	    //Sparkの実行をWebから見るときの名前の指定
	    String appName = args[10];

	    //データの分割数
	    int partitionNum = Integer.parseInt(args[11]);

	    /******************************************************************************/
	    //分散環境かどうか
	    boolean isDistributed = Boolean.parseBoolean(args[12]);
	    boolean isSpark = true;
	    if (isDistributed){
	    	osType = Consts.HDFS;
	    }
	    else {
	    	isSpark = Boolean.parseBoolean(args[13]);
	    }

	    /******************************************************************************/
	    //HDFSにおけるフォルダ
	    String dirNameInHdfs = "";
	    if(osType==Consts.HDFS) dirNameInHdfs = args[13];

	    //number of executor
	    int executorNum = 0;
	    if(osType==Consts.HDFS) executorNum = Integer.parseInt(args[14]);

	    //number of executor per cores
	    int executorCoreNum = 0;
	    if(osType==Consts.HDFS) executorCoreNum = Integer.parseInt(args[15]);

	    //1回ずつ分ける（メモリのうまい使い方が不明）
	    boolean isOnceExe = false;
	    if(osType==Consts.HDFS) isOnceExe = Boolean.parseBoolean(args[16]);

		/******************************************************************************/
	    //ファジィ分割の生成
	    //StaticFuzzyFunc kk = new StaticFuzzyFunc();
	    //kk.KKkk(Consts.MAX_FUZZY_DIVIDE_NUM);
	    /******************************************************************************/
	    //基本データ出力と実行
	    Date date = new Date();
		System.out.print("START: ");
		System.out.println(date);
		System.out.println("Processors:" + Runtime.getRuntime().availableProcessors() + " ");

	    //Sparkを使う場合
		SparkSession sparkSession = null;
		ForkJoinPool forkJoinPool = null;
		if (isSpark){
			sparkSession = SparkSession.builder().master(masterNodeName).appName(appName).getOrCreate();
			System.out.println( "Spark version: " + sparkSession.version() );
		}else{
			forkJoinPool = new ForkJoinPool(divideNum);
		}

		for(int i=0; i<args.length; i++){
			System.out.print(args[i] + " ");
		}
		System.out.println();

		if(isOnceExe){
			onceExection(seed, executorNum, executorCoreNum, partitionNum, dataName, dirNameInHdfs, objectiveNum, generationNum,
					divideNum, sparkSession, forkJoinPool, emoType, populationSize, crossValidationNum, repeatTimes, osType, args);
		}else{
			repeatExection(seed, executorNum, executorCoreNum, partitionNum, dataName, dirNameInHdfs, objectiveNum, generationNum,
					divideNum, sparkSession, forkJoinPool, emoType, populationSize, crossValidationNum, repeatTimes, osType, args);
		}

		/******************************************************************************/
	}

	static public void onceExection(int seed, int executorNum, int executorCoreNum, int partitionNum, String dataName,
			String dirNameInHdfs, int objectiveNum, int generationNum, int divideNum, SparkSession sparkSession,
			ForkJoinPool forkJoinPool,
			int emoType, int populationSize, int crossValidationNum, int repeatTimes, int osType, String[] args){

		/************************************************************/
		//読み込みファイル名とディレクトリ名
		String traFile = Output.makeFileNameOne(dataName, dirNameInHdfs, crossValidationNum, repeatTimes, true, sparkSession);
		String tstFile = Output.makeFileNameOne(dataName, dirNameInHdfs, crossValidationNum, repeatTimes, false, sparkSession);
		String resultDir = Output.makeDirName(dataName, dirNameInHdfs, executorNum, executorCoreNum, seed, osType);

		//実験パラメータ出力 + ディレクトリ作成
		if(crossValidationNum == 0 && repeatTimes == 0){
			String settings = StaticGeneralFunc.getExperimentSettings(args);
			resultDir = Output.makeDir(dataName, dirNameInHdfs, executorNum, executorCoreNum, seed, osType);
			Output.makeDirRule(resultDir, osType);
			Output.writeSetting(dataName, resultDir, settings, osType);
	    }

		//出力専用クラス
		ResultMaster resultMaster = new ResultMaster(resultDir, osType);
		/************************************************************/
		//繰り返しなし
		int repeat_i = repeatTimes;
		int cv_i = crossValidationNum;

		MersenneTwisterFast rnd = new MersenneTwisterFast(seed);

		System.out.print(repeat_i + " " + cv_i);
		startExeperiment(traFile, tstFile, sparkSession, forkJoinPool, partitionNum, rnd, objectiveNum, generationNum,
				emoType, populationSize, resultMaster, cv_i, repeat_i, osType, traFile, tstFile);
		System.out.println();

		/************************************************************/
		//出力
		resultMaster.writeAveTime();
		resultMaster.writeBestAve();
		Date end = new Date();
		System.out.println("END: " + end);
		/************************************************************/

	}

	static public void repeatExection(int seed, int executorNum, int executorCoreNum, int partitionNum, String dataName,
			String dirNameInHdfs, int objectiveNum, int generationNum, int divideNum, SparkSession sparkSession,
			ForkJoinPool forkJoinPool, int emoType, int populationSize, int crossValidationNum, int repeatTimes, int osType, String[] args){

		/************************************************************/
		//読み込みファイル名
		String traFiles[][] = new String[repeatTimes][crossValidationNum];
	    String tstFiles[][] = new String[repeatTimes][crossValidationNum];
	    Output.makeFileName(dataName, traFiles, tstFiles, dirNameInHdfs, sparkSession);

	    //データディレクトリ作成
	    String resultDir = Output.makeDir(dataName, dirNameInHdfs, executorNum, executorCoreNum, seed, osType);
	    Output.makeDirRule(resultDir, osType);

	    //実験パラメータ出力
		String settings = StaticGeneralFunc.getExperimentSettings(args);
	    Output.writeSetting(dataName, resultDir, settings, osType);

	    //出力専用クラス
	    ResultMaster resultMaster = new ResultMaster(resultDir, osType);

	    /************************************************************/
		MersenneTwisterFast rnd = new MersenneTwisterFast(seed);

		for(int repeat_i=0; repeat_i<repeatTimes; repeat_i++){
			for(int cv_i=0; cv_i<crossValidationNum; cv_i++){
				System.out.print(repeat_i + " " + cv_i);
				startExeperiment(traFiles[repeat_i][cv_i], tstFiles[repeat_i][cv_i], sparkSession, forkJoinPool,
						partitionNum, rnd, objectiveNum, generationNum, emoType, populationSize, resultMaster,
						cv_i, repeat_i, osType, traFiles[repeat_i][cv_i], tstFiles[repeat_i][cv_i]);
				System.out.println();
			}
		}

		/************************************************************/
		//出力
		resultMaster.writeAveTime();
		resultMaster.writeBestAve();
		Date end = new Date();
		System.out.println("END: " + end);
		/************************************************************/

	}

	static public void startExeperiment(String traFile, String testFile, SparkSession sparkSession, ForkJoinPool forkJoinPool,
			int partitionSize, MersenneTwisterFast rnd, int objectiveNum, int generationNum, int emoType, int populationSize,
			ResultMaster resultMaster, int crossValidationNum, int repeatNum, int osType, String nowTrainFile, String nowTestFile){

		/************************************************************/
		//時間計測開始
		TimeWatcher timeWatcher = new TimeWatcher();
		timeWatcher.start();

		/************************************************************/
		//データを読み込む
		Dataset<Row> trainData = null;
		DataSetInfo trainDataInfo = null;
		SQLContext sqlc = null;
		int attributeNum = 0;
		int trainDataSize = 0;
		int classNum = 0;

		//Sparkを使わない場合（HDFSも使わない）
		if(sparkSession == null){
			trainDataInfo = new DataSetInfo();
			DataLoader.inputFile(trainDataInfo, nowTrainFile);
		}
		else{ //Sparkを使う場合（HDFSも使う）
			sqlc = new SQLContext(sparkSession);
			trainData = sqlc.read()
					.format("com.databricks.spark.csv")
					.option("inferSchema", "true")
					.load(traFile);

			//データを論理的に分割して永続化（高速化のため）
			trainData.repartition(partitionSize);
			trainData.persist( StorageLevel.MEMORY_ONLY() );

			//データの属性数を把握
			attributeNum = trainData.first().length() - 1;
			trainDataSize = (int) trainData.count();
			String rowName = "_c" + attributeNum;
			classNum= (int) trainData.dropDuplicates(rowName).count();
			trainDataInfo = new DataSetInfo(trainDataSize, attributeNum, classNum);
		}

		/************************************************************/
		//初期個体群の生成
		PopulationManager populationManager = new PopulationManager(rnd, objectiveNum);
		populationManager.generateInitialPopulation(trainDataInfo, trainData, populationSize, forkJoinPool);

		//EMOアルゴリズム初期化
		Moead moead = new Moead(populationSize, Consts.VECTOR_DIVIDE_NUM, Consts.MOEAD_ALPHA, emoType, objectiveNum,
								Consts.SELECTION_NEIGHBOR_NUM, Consts.UPDATE_NEIGHBOR_NUM, rnd);
		Nsga2 nsga2 = new Nsga2(objectiveNum, rnd);

		//GA操作
		GaManager gaManager = new GaManager(populationSize, populationManager, nsga2, moead, rnd, forkJoinPool,
											objectiveNum, generationNum, trainData, emoType, resultMaster);
		gaManager.gaFrame(trainDataInfo, repeatNum, crossValidationNum);

		//時間計測終了
		timeWatcher.end();
		resultMaster.setTime( timeWatcher.getSec() );
		resultMaster.writeTime(timeWatcher.getSec(), timeWatcher.getNano(), crossValidationNum, repeatNum);

		//永続化終了（メモリにはまだ残っているのでOutOfMemoryする）
		if(trainData != null){
			trainData.unpersist();
		}

		/***********************これ以降出力操作************************/
		//評価用DataFrame作成
		Dataset<Row> testData = null;
		DataSetInfo testDataInfo = null;

		if(sparkSession == null){
			testDataInfo = new DataSetInfo();
			DataLoader.inputFile(testDataInfo, nowTestFile);
		}else{
			testData = sqlc.read()
					.format("com.databricks.spark.csv")
					.option("inferSchema", "true")
					.load(testFile);

			int testDataSize = (int) testData.count();
			testDataInfo = new DataSetInfo(testDataSize, attributeNum, classNum);
		}

		RuleSet bestRuleSet = gaManager.calcBestRuleSet(objectiveNum, populationManager,
														resultMaster, testDataInfo, testData, true);

		resultMaster.setBest(bestRuleSet);
		resultMaster.writeAllbest(bestRuleSet, crossValidationNum, repeatNum);
		resultMaster.outputRules(populationManager, crossValidationNum, repeatNum);
		resultMaster.outputVec(populationManager, crossValidationNum, repeatNum);
		if(objectiveNum != 1){
			resultMaster.outSolution(crossValidationNum, repeatNum);
			resultMaster.resetSolution();

		}
		/************************************************************/
	}


}

