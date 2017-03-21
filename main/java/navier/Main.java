package navier;

import java.util.Date;

import javax.management.JMException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import methods.CLineMethod;
import methods.Fmethod;
import methods.MersenneTwisterFast;
import methods.Osget;
import methods.Output;
import methods.Resulton;
import moead.Moead;
import nsga2.Nsga2;
import time.TimeWatch;

public class Main {

	public static void main(String[] args) throws JMException {

		System.out.print("ver." + 14.0);

		int os = 0;
		if(Osget.isLinux()==true || Osget.isMac()==true){
			os = Cons.Uni;		//linux mac
			System.out.println(" OS: Linux or Mac");
		}else{
			os = Cons.Win;		//win
			System.out.println(" OS: Windows");
		}
		/******************************************************************************/
		//コマンドライン引数が足りてるかどうか
		CLineMethod.lessArgs(args, 14);

	    String dataName = args[0];
	    int gen = Integer.parseInt(args[1]);
	    int objectives = Integer.parseInt(args[2]);
	    int dpop = Integer.parseInt(args[3]);
	    int func = Integer.parseInt(args[4]);

	    int Npop = Integer.parseInt(args[5]);

	    int CV = Integer.parseInt(args[6]);
	    int Rep = Integer.parseInt(args[7]);
	    int Pon = Integer.parseInt(args[8]);

	    int Seed = Integer.parseInt(args[9]);

	    //このmasterはネームノードのアドレス名が入る
	    String Master = args[10];

	    //Sparkの実行をWebから見るときの名前の指定
	    String AppName = args[11];

	    int PartitionSize = Integer.parseInt(args[12]);

	    //分散環境かどうか
	    boolean isDistributed = Boolean.parseBoolean(args[13]);
	    if (isDistributed) os = Cons.HDFS;

	    /******************************************************************************/
	    //HDFSにおけるフォルダ
	    String hdfs = "";
	    if(os==Cons.HDFS) hdfs = args[14];

	    //no. of executor
	    int executors = 0;
	    if(os==Cons.HDFS) executors = Integer.parseInt(args[15]);

	    //no. of executor per cores
	    int exeCores = 0;
	    if(os==Cons.HDFS) exeCores = Integer.parseInt(args[16]);

	    //1回ずつ分ける（メモリのうまい使い方が不明）
	    boolean isOne = true;
	    if(os==Cons.HDFS) isOne = Boolean.parseBoolean(args[17]);

		/******************************************************************************/
	    //ファジィ分割の生成
	    Fmethod kk = new Fmethod();
	    kk.KKkk(Cons.MaxFnum);
	    /******************************************************************************/
	    //基本データ出力と実行（一回かまとめてか）

	    Date date = new Date();
		System.out.print("START: ");
		System.out.println(date);
		System.out.println("Processors:" + Runtime.getRuntime().availableProcessors()+ " ");

		//SparkConf sparkConf = new SparkConf().setMaster(Master).setAppName(AppName);
		//JavaSparkContext sc = new JavaSparkContext(sparkConf);

		SparkSession spark = SparkSession.builder().master(Master).appName(AppName).getOrCreate();
		System.out.println( "Spark version: " + spark.version() );

		for(int i=0; i<args.length; i++){
			System.out.print(args[i] + " ");
		}
		System.out.println();

		if(isOne){
			One(Seed, executors, exeCores, PartitionSize, dataName, hdfs, objectives, gen, dpop, spark, func, Npop, CV, Rep, Pon, os);
		}else{
			CC(Seed, executors, exeCores, PartitionSize, dataName, hdfs, objectives, gen, dpop, spark, func, Npop, CV, Rep, Pon, os);
		}
		/******************************************************************************/
	}

	static public void One(int Seed, int executors, int exeCores, int PartitionSize, String dataName, String hdfs,
			  int objectives, int gen,int dpop, SparkSession spark, int func, int Npop, int CV, int Rep, int Pon ,int os){

		/************************************************************/
		//読み込みファイル名とディレクトリ名
		String traFile = Output.makeFileNameOne(dataName, hdfs, CV, Rep, true);
		String tstFile = Output.makeFileNameOne(dataName, hdfs, CV, Rep, false);
		String resultDir = Output.makeDirName(dataName, hdfs, executors, exeCores, Seed, os);

		//実験パラメータ出力 + ディレクトリ作成
		if(CV == 0 && Rep == 0 && Pon == 0){
			String st = "DataName: " + dataName + " 0: NSGAII, 1: WS, 2: TCH, 3: PBI, 4: IPBI, 5: SSF"
				+ "\n gen: " + gen + " cv: " + CV + " Rep: " + Rep + " Pon: " + Pon + " seed: " + Seed + " 2objWay: " + Cons.Way
				+ "\n Npop: " + Npop + " Nini: " + Cons.Nini + "objectives: " + objectives + " dpop: " + dpop + " func: " + func
				+ "\n Len: " + Cons.Len + " Dont: " + Cons.Dont + " dWitch: " + Cons.dWitch
				+ "\n Fnum: " + Cons.Fnum + " MaxFnum: " + Cons.MaxFnum + " Rmax: " + Cons.Rmax + " Rmin: " + Cons.Rmin
				+ "\n micope: " + Cons.Micope + " micNum: " + Cons.MicNum + " CrossM: " + Cons.CrossM + " CrossP: " + Cons.CrossP
				+ "\n Fnum: " + Cons.Fnum + " MaxFnum: " + Cons.MaxFnum + " Rmax: " + Cons.Rmax + " Rmin: " + Cons.Rmin
				+ "\n inclination: " + Cons.inclination + " isCDnormalize: " + Cons.isCDnormalize + " isParent: " + Cons.isParent
				+ "\n neiPerSwhit: " + Cons.neiPerSwit + " Neiper: " + Cons.neiPer+ " H: " + Cons.H + " alpha: " + Cons.alpha + " theta: "+ Cons.theta
				+ "\n seleN: " + Cons.seleN + " upN: " +Cons.upN  + " normalization: " +Cons.Normalization + " isBias: " +Cons.isBias
				+ "\n idealDown: " + Cons.idealDown + " isWSfromNadia: " +Cons.isWSfromNadia  + " isNewGen: " +Cons.isNewGen + " ShowRate: " +Cons.ShowRate
				;

			resultDir = Output.makeDir(dataName, hdfs, executors, exeCores, Seed, os);
			Output.makeDirRule(resultDir, os);
			Output.writeExp(dataName, resultDir, st, os);
	    }

		//出力専用クラス
		Resulton res = new Resulton(resultDir, os);
		/************************************************************/
		//繰り返しなし
		int pp = Pon;
		int j = Rep;
		int i = CV;

		MersenneTwisterFast rand = new MersenneTwisterFast( Seed * (pp+1) );

		System.out.print(pp + " " + j + " " + i);
		pall(traFile, tstFile, spark, PartitionSize, rand, objectives, gen, func, Npop, res, i, j, pp, os);
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
						  int CV, int Rep, int Pon ,int os){

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
	    String st = "DataName: " + dataName + " 0: NSGAII, 1: WS, 2: TCH, 3: PBI, 4: IPBI, 5: SSF"
	    		+ "\n gen: " + gen + " cv: " + CV + " Rep: " + Rep + " Pon: " + Pon + " seed: " + Seed + " 2objWay: " + Cons.Way
	    		+ "\n Npop: " + Npop + " Nini: " + Cons.Nini + "objectives: " + objectives + " dpop: " + dpop + " func: " + func
	    		+ "\n Len: " + Cons.Len + " Dont: " + Cons.Dont + " dWitch: " + Cons.dWitch
	    		+ "\n Fnum: " + Cons.Fnum + " MaxFnum: " + Cons.MaxFnum + " Rmax: " + Cons.Rmax + " Rmin: " + Cons.Rmin
	    		+ "\n micope: " + Cons.Micope + " micNum: " + Cons.MicNum + " CrossM: " + Cons.CrossM + " CrossP: " + Cons.CrossP
	    		+ "\n Fnum: " + Cons.Fnum + " MaxFnum: " + Cons.MaxFnum + " Rmax: " + Cons.Rmax + " Rmin: " + Cons.Rmin
	    		+ "\n inclination: " + Cons.inclination + " isCDnormalize: " + Cons.isCDnormalize + " isParent: " + Cons.isParent
	    		+ "\n neiPerSwhit: " + Cons.neiPerSwit + " Neiper: " + Cons.neiPer+ " H: " + Cons.H + " alpha: " + Cons.alpha + " theta: "+ Cons.theta
	    		+ "\n seleN: " + Cons.seleN + " upN: " +Cons.upN  + " normalization: " +Cons.Normalization + " isBias: " +Cons.isBias
	    		+ "\n idealDown: " + Cons.idealDown + " isWSfromNadia: " +Cons.isWSfromNadia  + " isNewGen: " +Cons.isNewGen + " ShowRate: " +Cons.ShowRate
	    		;

	    Output.writeExp(dataName, resultDir, st, os);

	    //出力専用クラス
	    Resulton res = new Resulton(resultDir, os);
	    /************************************************************/
	    //繰り返し
		for(int pp=0;pp<Pon;pp++){
			MersenneTwisterFast rand = new MersenneTwisterFast( Seed * (pp+1) );
			//繰り返し回数
			for(int j=0;j<Rep; j++){
				//ＣＶ
				for(int i=0;i<CV; i++){
					System.out.print(pp + " " + j + " " + i);
					pall(traFiles[j][i], tstFiles[j][i], spark, PartitionSize, rand, objectives, gen, func, Npop, res, i, j, pp, os);
					System.out.println();
				}
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
			int objectives, int gen, int func, int Npop, Resulton res ,int CV, int Rep, int Pon, int os){

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
		RuleSet ruleset = new RuleSet(rnd, objectives);
		ruleset.initialPal(traData, df, Npop);

		//EMOアルゴリズム初期化
		Moead moe = new Moead(Npop, Cons.H, Cons.alpha, func, objectives, Cons.seleN, Cons.upN, rnd);
		Nsga2 nsg = new Nsga2(objectives, rnd);

		//GA操作
		GAH GA = new GAH(Npop, ruleset, nsg, moe, rnd, objectives, gen, df, func, res);
		GA.GAFrame(traData, Pon, Rep, CV);

		//時間計測終了
		time.end();
		res.setTime(time.getSec());
		res.writeTime(time.getSec(), time.getNano(), CV, Rep, Pon);

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

		Pittsburgh best = GA.GetBestRuleSet(objectives, ruleset, res, tstData, dftst, true);

		res.setBest(best);
		res.writeAllbest(best, CV, Rep, Pon);
		res.outputRules(ruleset, CV, Rep, Pon);
		res.outputVec(ruleset, CV, Rep, Pon);
		if(objectives != 1){
			res.outSolution(CV, Rep, Pon);
			res.resetSolution();
		}
		/************************************************************/
	}


}

