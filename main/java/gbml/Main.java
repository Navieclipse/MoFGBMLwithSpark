package gbml;

import java.util.Date;

import javax.management.JMException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import methods.DataLoader;
import methods.MersenneTwisterFast;
import methods.Output;
import methods.ResultMaster;
import methods.StaticGeneralFunc;
import moead.Moead;
import nsga2.Nsga2;
import time.TimeWatcher;

public class Main {

	public static void main(String[] args) throws JMException {
		System.out.println("ver." + 20.0);

		/******************************************************************************/
		//基本設定
		Settings sets = new Settings(args);

		/******************************************************************************/
	    //ファジィ分割の生成
	    //StaticFuzzyFunc kk = new StaticFuzzyFunc();
	    //kk.KKkk(Consts.MAX_FUZZY_DIVIDE_NUM);
	    /******************************************************************************/
	    //基本データ出力と実行
		System.out.println("Processors:" + Runtime.getRuntime().availableProcessors() + " ");

		System.out.print("args: ");
		for(int i=0; i<args.length; i++){
			System.out.print(args[i] + " ");
		}
		System.out.println();
		System.out.println();

	    Date date = new Date();
		System.out.print("START: ");
		System.out.println(date);

		//一回ごとにjarファイルを呼び出すか否か
		if(sets.isOnceExe){
			onceExection(sets, args);
		}else{
			repeatExection(sets, args);
		}
		/******************************************************************************/
	}

	static public void onceExection(Settings sets, String[] args){

		//読み込みファイル名とディレクトリ名
		String traFile = Output.makeFileNameOne(sets.dataName, sets.dirNameInHdfs, sets.crossValidationNum, sets.repeatTimes, true, sets.sparkSession);
		String tstFile = Output.makeFileNameOne(sets.dataName, sets.dirNameInHdfs, sets.crossValidationNum, sets.repeatTimes, false, sets.sparkSession);
		String resultDir = Output.makeDirName(sets.dataName, sets.dirNameInHdfs, sets.executorNum, sets.executorCoreNum, sets.seed, sets.osType);

		//実験パラメータ出力 + ディレクトリ作成
		if(sets.crossValidationNum == 0 && sets.repeatTimes == 0){
			String settings = StaticGeneralFunc.getExperimentSettings(args);
			resultDir = Output.makeDir(sets.dataName, sets.dirNameInHdfs, sets.executorNum, sets.executorCoreNum, sets.seed, sets.osType);
			Output.makeDirRule(resultDir, sets.osType);
			Output.writeSetting(sets.dataName, resultDir, settings, sets.osType);
	    }

		//出力専用クラス
		ResultMaster resultMaster = new ResultMaster(resultDir, sets.osType);
		/************************************************************/
		//繰り返しなし
		int repeat_i = sets.repeatTimes;
		int cv_i = sets.crossValidationNum;

		MersenneTwisterFast rnd = new MersenneTwisterFast(sets.seed);

		System.out.print(repeat_i + " " + cv_i);
		startExeperiment(sets, traFile, tstFile, rnd, resultMaster, cv_i, repeat_i, traFile, tstFile);
		System.out.println();

		/************************************************************/
		//出力
		resultMaster.writeAveTime();
		resultMaster.writeBestAve();
		Date end = new Date();
		System.out.println("END: " + end);

	}

	static public void repeatExection(Settings sets, String[] args){

		//読み込みファイル名
		String traFiles[][] = new String[sets.repeatTimes][sets.crossValidationNum];
	    String tstFiles[][] = new String[sets.repeatTimes][sets.crossValidationNum];
	    Output.makeFileName(sets.dataName, traFiles, tstFiles, sets.dirNameInHdfs, sets.sparkSession);

	    //データディレクトリ作成
	    String resultDir = Output.makeDir(sets.dataName, sets.dirNameInHdfs, sets.executorNum, sets.executorCoreNum, sets.seed, sets.osType);
	    Output.makeDirRule(resultDir, sets.osType);

	    //実験パラメータ出力
		String settings = StaticGeneralFunc.getExperimentSettings(args);
	    Output.writeSetting(sets.dataName, resultDir, settings, sets.osType);

	    //出力専用クラス
	    ResultMaster resultMaster = new ResultMaster(resultDir, sets.osType);

	    /************************************************************/
	    //繰り返し
		MersenneTwisterFast rnd = new MersenneTwisterFast(sets.seed);

		for(int repeat_i=0; repeat_i<sets.repeatTimes; repeat_i++){
			for(int cv_i=0; cv_i<sets.crossValidationNum; cv_i++){
				System.out.print(repeat_i + " " + cv_i);

				startExeperiment(sets, traFiles[repeat_i][cv_i], tstFiles[repeat_i][cv_i], rnd, resultMaster,
									cv_i, repeat_i, traFiles[repeat_i][cv_i], tstFiles[repeat_i][cv_i]);

				System.out.println();
			}
		}
		/************************************************************/
		//出力
		resultMaster.writeAveTime();
		resultMaster.writeBestAve();
		Date end = new Date();
		System.out.println("END: " + end);

	}

	static public void startExeperiment(Settings sets, String traFile, String testFile, MersenneTwisterFast rnd,
			ResultMaster resultMaster, int crossValidationNum, int repeatNum, String nowTrainFile, String nowTestFile){

		/************************************************************/
		//時間計測開始
		TimeWatcher evaWatcher = new TimeWatcher();
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
		if(sets.calclationType == 0 || sets.calclationType == 2){
			trainDataInfo = new DataSetInfo();
			DataLoader.inputFile(trainDataInfo, nowTrainFile);
		}
		else if(sets.calclationType == 1){ //Sparkを使う場合（HDFSも使う）
			sqlc = new SQLContext(sets.sparkSession);
			trainData = sqlc.read()
					.format("com.databricks.spark.csv")
					.option("inferSchema", "true")
					.load(traFile);

			//データを論理的に分割して永続化（高速化のため）
			trainData.repartition(sets.partitionNum);
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
		PopulationManager populationManager = new PopulationManager(rnd, sets.objectiveNum);
		populationManager.generateInitialPopulation(trainDataInfo, trainData, sets.populationSize, sets.forkJoinPool, sets.calclationType);

		//EMOアルゴリズム初期化
		Moead moead = new Moead(sets.populationSize, Consts.VECTOR_DIVIDE_NUM, Consts.MOEAD_ALPHA, sets.emoType,
								sets.objectiveNum, Consts.SELECTION_NEIGHBOR_NUM, Consts.UPDATE_NEIGHBOR_NUM, rnd);
		Nsga2 nsga2 = new Nsga2(sets.objectiveNum, rnd);

		//GA操作
		GaManager gaManager = new GaManager(sets.populationSize, populationManager, nsga2, moead, rnd, sets.forkJoinPool, sets.serverList,
											sets.objectiveNum, sets.generationNum, trainData, sets.emoType, resultMaster, evaWatcher);
		gaManager.gaFrame(trainDataInfo, repeatNum, crossValidationNum);

		//時間計測終了
		timeWatcher.end();
		resultMaster.setTime( timeWatcher.getSec() );
		resultMaster.writeTime(timeWatcher.getSec(), timeWatcher.getNano(), crossValidationNum, repeatNum);
		resultMaster.writeTime(evaWatcher.getSec(), evaWatcher.getNano(), 114, 514);

		//永続化終了（メモリにはまだ残っているのでOutOfMemoryする）
		if(trainData != null){
			trainData.unpersist();
		}

		/***********************これ以降出力操作************************/
		//評価用DataFrame作成
		Dataset<Row> testData = null;
		DataSetInfo testDataInfo = null;

		if(sets.calclationType == 0 || sets.calclationType == 2){
			testDataInfo = new DataSetInfo();
			DataLoader.inputFile(testDataInfo, nowTestFile);
		}
		else if(sets.calclationType == 1){
			testData = sqlc.read()
					.format("com.databricks.spark.csv")
					.option("inferSchema", "true")
					.load(testFile);

			int testDataSize = (int) testData.count();
			testDataInfo = new DataSetInfo(testDataSize, attributeNum, classNum);
		}

		RuleSet bestRuleSet =
		gaManager.calcBestRuleSet(sets.objectiveNum, populationManager,	resultMaster, testDataInfo, testData, true);

		resultMaster.setBest(bestRuleSet);
		resultMaster.writeAllbest(bestRuleSet, crossValidationNum, repeatNum);
		resultMaster.outputRules(populationManager, crossValidationNum, repeatNum);
		resultMaster.outputVec(populationManager, crossValidationNum, repeatNum);
		if(sets.objectiveNum != 1){
			resultMaster.outSolution(crossValidationNum, repeatNum);
			resultMaster.resetSolution();
		}

	}

}

