package gbml;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import methods.MersenneTwisterFast;
import methods.ResultMaster;
import methods.StaticGeneralFunc;
import moead.Moead;
import nsga2.Nsga2;


public class GaManager {

	PopulationManager populationManager;

	Nsga2 nsga2;

	Moead moead;

	MersenneTwisterFast rnd;

	ForkJoinPool forkJoinPool;

	Dataset<Row> trainData;

	int secondObjType = Consts.SECOND_OBJECTIVE_TYPE;

	ResultMaster resultMaster;

	int objectiveNum;
	long generationNum;

	int emoType;
	int populationSize;

	public static final int PARENT = 1;
	public static final int OFF_SPRING = 0;


	public GaManager( int popSize, PopulationManager populationManager, Nsga2 nsga2, Moead moead, MersenneTwisterFast rnd, ForkJoinPool forkJoinPool,
				int objectiveNum, int generationNum, Dataset<Row> trainData, int emoType, ResultMaster resultMaster) {

		this.populationManager = populationManager;
		this.nsga2 = nsga2;
		this.moead = moead;

		this.forkJoinPool = forkJoinPool;

		this.trainData = trainData;

		this.resultMaster = resultMaster;

		this.objectiveNum = objectiveNum;
		this.generationNum = generationNum;
		this.emoType = emoType;
		this.populationSize = popSize;

	}

	public void gaFrame(DataSetInfo trainDataInfo, int repeat, int cv){

		//初期個体群評価
		parentEvaluation(trainDataInfo, populationManager);

		//MOEAD初期化 （２目的のみ）
		if(emoType > 0){
			moead.ini();
			moead.inidvi(populationManager.currentRuleSets);
		}
		else if(objectiveNum != 1 && emoType == 0){
			nsga2.calcRank(populationManager.currentRuleSets);
		}

		if(populationSize == 1){
			populationManager.bestOfAllGen = new RuleSet( populationManager.currentRuleSets.get(0) );
		}

		boolean isNewGen = Consts.DO_LOG_PER_LOG;

		for (int gen_i = 0; gen_i < generationNum; gen_i++) {

			if(gen_i % Consts.PER_SHOW_GENERATION_NUM == 0){
				System.out.print(".");
			}

			if(isNewGen){		//途中結果保持（テストデータは無理）
				genCheck(gen_i, repeat, cv);
			}

			//GA操作
			if(populationSize == 1){
				michiganTypeGa(trainDataInfo, gen_i);
			}
			else{
				if(emoType == 0||objectiveNum == 1){
					nsga2TypeGa(trainDataInfo, gen_i);

				}else{
					moeadTypeGa(trainDataInfo, gen_i);
				}
			}
		}
	}

	public void genCheck(int gen, int repeat, int cv){
		if( (gen+1) <=10 ||
			(gen+1) %10==0 && gen<=100||
			(gen+1) %100==0 && gen<=1000||
			(gen+1) %1000==0 && gen<=10000||
			(gen+1) %10000==0 && gen<=100000||
			(gen+1) %100000==0 && gen<=1000000||
			(gen+1) %1000000==0
		){

		RuleSet bestb;
		bestb = calcBestRuleSet();
		double trat = bestb.getMissRate();
		double tstt = bestb.getTestMissRate();
		double numr = bestb.getRuleNum();
		double lengtht = bestb.getRuleLength();
		resultMaster.writeBestLog(trat, tstt, numr, lengtht, gen+1, repeat, cv);

		}
	}

	void michiganTypeGa(DataSetInfo trainDataInfo, int gen){

		populationManager.newRuleSets.add(  new RuleSet( populationManager.currentRuleSets.get(0) )  );

		populationManager.michiganOperation(0, trainData, trainDataInfo, forkJoinPool);

		populationManager.newRuleSets.get(0).evaluationRule(trainDataInfo, trainData, objectiveNum, secondObjType, forkJoinPool);

		poplationUpdateOfMichigan();

	}

	void poplationUpdateOfMichigan(){

		boolean isES = Consts.IS_ES_UPDATE;
		if(isES){																	//ESか否か
			double currentRate = populationManager.currentRuleSets.get(0).getMissRate();
			double newRate = populationManager.newRuleSets.get(0).getMissRate();

			if(currentRate >= newRate){
				populationManager.currentRuleSets.get(0).pitsCopy( populationManager.newRuleSets.get(0) );
				populationManager.bestOfAllGen = new RuleSet( populationManager.newRuleSets.get(0) );
			}
		}
		else{
			double bestRate = populationManager.bestOfAllGen.getMissRate();
			double newRate = populationManager.newRuleSets.get(0).getMissRate();

			if(bestRate >= newRate){
				populationManager.bestOfAllGen = new RuleSet( populationManager.newRuleSets.get(0) );
			}
			populationManager.currentRuleSets.get(0).pitsCopy( populationManager.newRuleSets.get(0) );
		}

		populationManager.newRuleSets.clear();
	}


	void nsga2TypeGa(DataSetInfo trainDataInfo, int gen) {

		geneticOperation(trainDataInfo);
		deleteUnnecessaryRules();

		offspringEvaluation(trainDataInfo, populationManager);

		if(objectiveNum == 1){
			populationUpdateOfSingleObj();
		}
		else{
			nsga2.populationUpdate(populationManager);
		}

	}

	void moeadTypeGa(DataSetInfo data, int gen) {

		for(int i = 0;i < populationManager.currentRuleSets.size(); i++){
			populationManager.currentRuleSets.get(i).setSize(data.DataSize);
		}
		List<Integer> UseVecNums = new ArrayList<Integer>();
		for (int i = 0; i < populationSize; i++) {
			UseVecNums.add(i);
		}
		//Gmethod.shuffle(UseVecNums, rnd);

		populationManager.newRuleSets.clear();
		populationManager.addNewPits(populationSize);

		for (int i = 0; i < populationSize; i++) {

			int nowVec = UseVecNums.get(i);

			populationManager.crossOverRandom(nowVec, populationSize, moead);
			populationManager.newRuleSetMutation(nowVec);
			populationManager.michiganOperation(nowVec, trainData, data, forkJoinPool);

			populationManager.newRuleSets.get(nowVec).removeRule();
			populationManager.newRuleSets.get(nowVec).evaluationRule(data, trainData, objectiveNum, secondObjType, forkJoinPool);
			//EvoluationOne(data, divideHyb.newPitsRules.get(nowVec));

			moead.updateReference(populationManager.newRuleSets.get(nowVec));
			moead.updateNeighbors(populationManager.newRuleSets.get(nowVec), populationManager.currentRuleSets,nowVec, emoType);
		}

	}

	void parentEvaluation(DataSetInfo dataSetInfo, PopulationManager popManager){

		if(trainData == null){
			popManager.currentRuleSets.stream()
					.forEach( rule -> rule.evaluationRule(dataSetInfo, trainData, objectiveNum, secondObjType, forkJoinPool) );
		}else{
			popManager.currentRuleSets	.parallelStream()
					.forEach( rule -> rule.evaluationRule(dataSetInfo, trainData, objectiveNum, secondObjType, forkJoinPool) );
		}

	}

	void offspringEvaluation(DataSetInfo dataSetInfo, PopulationManager popManager){

		if(trainData == null){
			popManager.newRuleSets.stream()
					.forEach( rule -> rule.evaluationRule(dataSetInfo, trainData, objectiveNum, secondObjType, forkJoinPool) );
		}else{
			popManager.newRuleSets	.parallelStream()
						.forEach( rule -> rule.evaluationRule(dataSetInfo, trainData, objectiveNum, secondObjType, forkJoinPool) );
		}

	}

	void deleteUnnecessaryRules() {
		for (int i = 0; i < populationManager.newRuleSets.size(); i++) {
			populationManager.newRuleSets.get(i).removeRule();
		}
	}

	void crossOverAll() {

		int length = populationManager.currentRuleSets.size();

		//子個体初期化
		populationManager.newRuleSets.clear();

		for (int s = 0; s < length; s++) {
			populationManager.newRuleSetsInit();
			populationManager.crossOver(s, populationSize);
			populationManager.newRuleSetMutation(s);
		}

	}

	void geneticOperation(DataSetInfo trainDataInfo){

		int length = populationManager.currentRuleSets.size();
		populationManager.newRuleSets.clear();

		for (int s = 0; s < length; s++) {
			populationManager.newRuleSetsInit();
			populationManager.crossOverAndMichiganOpe(s, populationSize, trainData, forkJoinPool, trainDataInfo);
			populationManager.newRuleSetMutation(s);
		}

	}

	void populationUpdateOfSingleObj() {

		Collections.sort(populationManager.currentRuleSets, new PittsComparator());
		Collections.sort(populationManager.newRuleSets, new PittsComparator());

		ArrayList<RuleSet> temp = new ArrayList<RuleSet>();

		StaticGeneralFunc.mergeSort(temp, populationManager.currentRuleSets, populationManager.newRuleSets);

		populationManager.currentRuleSets = new ArrayList<RuleSet>(temp);
		populationManager.newRuleSets.clear();

	}

	double out2objeAnother(RuleSet pit, int way){
		if(way == 0){
			return (double)(pit.getRuleLength());
		}else {
			return (double)(pit.getRuleNum());
		}
	}

	//ベスト系
	RuleSet calcBestRuleSet() {

		RuleSet best;
		best = new RuleSet(populationManager.currentRuleSets.get(0));
		if (objectiveNum == 1) {
			for (int i = 0; i < populationManager.currentRuleSets.size(); i++) {
				if (populationManager.currentRuleSets.get(i).GetFitness(0) < best.GetFitness(0)) {
					best = new RuleSet(populationManager.currentRuleSets.get(i));
				}
				else if (populationManager.currentRuleSets.get(i).GetFitness(0) == best.GetFitness(0)) {
					if (populationManager.currentRuleSets.get(i).getMissRate() < best.getMissRate()) {
						best = new RuleSet(populationManager.currentRuleSets.get(i));
					}
				}
			}
		}

		else {

			for (int i = 0; i < populationManager.currentRuleSets.size(); i++) {
				if (populationManager.currentRuleSets.get(i).GetRank() == 0) {
					if (populationManager.currentRuleSets.get(i).getMissRate() < best.getMissRate()) {
						best = new RuleSet(populationManager.currentRuleSets.get(i));
					}
					else if (populationManager.currentRuleSets.get(i).getMissRate() == best.getMissRate()) {
						if (populationManager.currentRuleSets.get(i).getRuleNum() <= best.getRuleNum()) {
							if (populationManager.currentRuleSets.get(i).getRuleLength() <= best.getRuleLength()) {
								best = new RuleSet(populationManager.currentRuleSets.get(i));
							}
						}
					}
				}
			}
		}

		//途中で評価用の結果出すのはちょっとしんどい
		//double accTest = (double) best.CalcAccuracyPal(tstData, Dpop) / tstData.DataSize;
		//best.SetTestMissRate((1 - accTest) * 100);

		return best;

	}

	public RuleSet calcBestRuleSet(int objectiveNum, PopulationManager popManager, ResultMaster resultMaster,
									DataSetInfo testDataInfo, Dataset<Row> testData, boolean isTest) {

		RuleSet bestRuleset;

		for (int i = 0; i < popManager.currentRuleSets.size(); i++) {

			double fitness = 0;

			if(popManager.currentRuleSets.get(i).getRuleNum() != 0){

				popManager.currentRuleSets.get(i).setNumAndLength();

				if(isTest){
					double acc = 0.0;
					if(testData == null){
						acc = (double) popManager.currentRuleSets.get(i).calcAndSetMissPatterns(testDataInfo, forkJoinPool);
					}else{
						acc = (double) popManager.currentRuleSets.get(i).calcMissPatterns(testData);
					}
					popManager.currentRuleSets.get(i).setTestMissRate( ( acc / (double)testDataInfo.DataSize ) * 100.0 );
				}

				popManager.currentRuleSets.get(i).setNumAndLength();

				if (objectiveNum == 1) {
					fitness = Consts.W1 * popManager.currentRuleSets.get(i).getMissRate()
							+ Consts.W2 * popManager.currentRuleSets.get(i).getRuleNum()
							+ Consts.W3 * popManager.currentRuleSets.get(i).getRuleLength();
					popManager.currentRuleSets.get(i).SetFitness(fitness, 0);
				} else if (objectiveNum == 2) {
					popManager.currentRuleSets.get(i).SetFitness(popManager.currentRuleSets.get(i).getMissRate(), 0);
					popManager.currentRuleSets.get(i).SetFitness(popManager.currentRuleSets.get(i).out2obje(secondObjType), 1);
				} else if (objectiveNum == 3) {
					popManager.currentRuleSets.get(i).SetFitness(popManager.currentRuleSets.get(i).getMissRate(), 0);
					popManager.currentRuleSets.get(i).SetFitness(popManager.currentRuleSets.get(i).getRuleNum(), 1);
					popManager.currentRuleSets.get(i).SetFitness(popManager.currentRuleSets.get(i).getRuleLength(), 2);
				} else {
					System.out.println("not be difined");
				}
			}

			else {
				for (int o = 0; o < objectiveNum; o++) {
					fitness = 100000;
					popManager.currentRuleSets.get(i).SetFitness(fitness, o);
				}
			}
		}


		bestRuleset = new RuleSet(popManager.currentRuleSets.get(0));
		if (objectiveNum == 1) {
			for (int i = 0; i < popManager.currentRuleSets.size(); i++) {

				if (popManager.currentRuleSets.get(i).GetFitness(0) < bestRuleset.GetFitness(0)) {
					bestRuleset = new RuleSet(popManager.currentRuleSets.get(i));
				}

				else if (popManager.currentRuleSets.get(i).GetFitness(0) == bestRuleset.GetFitness(0)) {
					if (popManager.currentRuleSets.get(i).getMissRate() < bestRuleset.getMissRate()) {
						bestRuleset = new RuleSet(popManager.currentRuleSets.get(i));
					}
				}

			}
		}

		else {

			//DisideRank(all.pitsRules);
			for (int i = 0; i < popManager.currentRuleSets.size(); i++) {
				int claNum = popManager.currentRuleSets.get(i).mulCla();		//そのルール集合の識別するクラス数

				if (popManager.currentRuleSets.get(i).GetRank() == 0) {

					resultMaster.setSolution(popManager.currentRuleSets.get(i).out2obje(secondObjType),
									popManager.currentRuleSets.get(i).GetFitness(0),
									popManager.currentRuleSets.get(i).getTestMissRate(),
									out2objeAnother(popManager.currentRuleSets.get(i), secondObjType),
									claNum);


					if (popManager.currentRuleSets.get(i).GetFitness(0) < bestRuleset.GetFitness(0)) {
						bestRuleset = new RuleSet(popManager.currentRuleSets.get(i));
					}
					else if (popManager.currentRuleSets.get(i).GetFitness(0) == bestRuleset.GetFitness(0)) {
						if (popManager.currentRuleSets.get(i).GetFitness(1) <= bestRuleset.GetFitness(1)) {
							if (popManager.currentRuleSets.get(i).getRuleLength() <= bestRuleset.getRuleLength()) {
								bestRuleset = new RuleSet(popManager.currentRuleSets.get(i));
							}
						}
					}

				}
			}

		}
		if(isTest){
			double accTest = 0.0;
			if(testData == null){
				accTest = (double) bestRuleset.calcMissPatterns(testDataInfo, forkJoinPool) / testDataInfo.DataSize;
			}else{
				accTest = (double) bestRuleset.calcMissPatterns(testData)	/ testDataInfo.DataSize;
			}
			bestRuleset.setTestMissRate(accTest * 100);
		}

		bestRuleset.setNumAndLength();

		return bestRuleset;

	}

	void RandomShuffle(ArrayList<RuleSet> rules) {
		for (int i = rules.size() - 1; i > 0; i--) {
			int t = rnd.nextInt(i + 1);

			RuleSet tmp = rules.get(i);
			rules.get(i).pitsCopy(rules.get(t));
			rules.get(t).pitsCopy(tmp);

		}
	}

	public class PittsComparator implements Comparator<RuleSet> {
	    //比較メソッド（データクラスを比較して-1, 0, 1を返すように記述する）
	    public int compare(RuleSet a, RuleSet b) {
	        double no1 = a.GetFitness(0);
	        double no2 = b.GetFitness(0);

	        //昇順でソート
	        if (no1 > no2) {
	            return 1;

	        } else if (no1 == no2) {
	            return 0;

	        } else {
	            return -1;

	        }
	    }

	}

}
