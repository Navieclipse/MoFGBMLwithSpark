package gbml;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import methods.MersenneTwisterFast;
import methods.ResultMaster;
import methods.StaticGeneralFunc;
import moead.Moead;
import nsga2.Nsga2;


public class GaManager {

	Classifier divideHyb;

	Nsga2 nsg;

	Moead moe;

	MersenneTwisterFast rnd;

	Dataset<Row> df;

	int way = Consts.SECOND_OBJECTIVE_TYPE;

	ResultMaster result;

	int objectives;
	long GenNum;

	int func;
	int popSize;

	public static final int Parent = 1;
	public static final int Son = 0;


	public GaManager( int popSize, Classifier divideHyb, Nsga2 nsga2, Moead sai ,MersenneTwisterFast rnd,
				int objectives,int gen, Dataset<Row> df, int func, ResultMaster res) {

		this.divideHyb = divideHyb;
		nsg = nsga2;
		moe = sai;

		this.df = df;

		this.result = res;

		this.objectives = objectives;
		this.GenNum = gen;
		this.func = func;
		this.popSize = popSize;

	}

	public void GAini(DataSetInfo data) {
		Evoluation_Parent(data, divideHyb);
		if (objectives != 1 && func == 0){
			nsg.DisideRank(divideHyb.pitsRules);
		}
	}

	public void GAFrame(DataSetInfo traData, int repeat, int cv){

		//初期個体群
		GAini(traData);

		//MOEAD初期化 （２目的のみ）
		if(func > 0){
			moe.ini();
			moe.inidvi(divideHyb.pitsRules);
		}

		boolean isNewGen = Consts.DO_LOG_PER_LOG;

		for (int gen = 0; gen < GenNum; gen++) {

			if(gen % Consts.PER_SHOW_GENERATION_NUM == 0){
				System.out.print(".");
			}

			if(isNewGen){		//途中結果保持（テストデータは無理）
				genCheck(gen, repeat, cv);
			}

			//GA操作
			if(func == 0||objectives == 1){
				GAStartNS(traData, gen);

			}else{
				GAStartMO(traData, gen);
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
		bestb = bestGenCalc();
		double trat = bestb.getMissRate();
		double tstt = bestb.GetTestMissRate();
		double numr = bestb.getRuleNum();
		double lengtht = bestb.getRuleLength();
		result.writeBestLog(trat, tstt, numr, lengtht, gen+1, repeat, cv);

		}
	}

	public void GAStartNS(DataSetInfo data, int gen) {

		GeneticOperation();
		Delete();

		Evoluation_Child(data, divideHyb);

		if(objectives == 1){
			oneObjUpdate();
		}
		else{
			nsg.GenChange(divideHyb);
		}

	}

	public void GAStartMO(DataSetInfo data, int gen) {

		for(int i = 0;i < divideHyb.pitsRules.size(); i++){
			divideHyb.pitsRules.get(i).setSize(data.DataSize);
		}
		List<Integer> UseVecNums = new ArrayList<Integer>();
		for (int i = 0; i < popSize; i++) {
			UseVecNums.add(i);
		}
		//Gmethod.shuffle(UseVecNums, rnd);

		divideHyb.newPitsRules.clear();
		divideHyb.addNewPits(popSize);

		for (int i = 0; i < popSize; i++) {

			int nowVec = UseVecNums.get(i);

			divideHyb.pitsCrossRam(nowVec, popSize, moe);
			divideHyb.pitsMutation(nowVec);
			divideHyb.micGA(nowVec, df);

			divideHyb.newPitsRules.get(nowVec).removeRule();
			divideHyb.newPitsRules.get(nowVec).EvoluationOne(data, df, objectives, way);
			//EvoluationOne(data, divideHyb.newPitsRules.get(nowVec));

			moe.updateReference(divideHyb.newPitsRules.get(nowVec));
			moe.updateNeighbors(divideHyb.newPitsRules.get(nowVec), divideHyb.pitsRules,nowVec, func);
		}

	}

	public void Evoluation_Parent(DataSetInfo data, Classifier r){

		r.pitsRules	.parallelStream()
					.forEach( rule -> rule.EvoluationOne(data, df, objectives, way) );
	}

	public void Evoluation_Child(DataSetInfo data, Classifier r){

		r.newPitsRules	.parallelStream()
						.forEach( rule -> rule.EvoluationOne(data, df, objectives, way) );
	}

	public void Michigan() {
		int size =  divideHyb.newPitsRules.size();
		for (int i = 0; i < size; i++) {
			divideHyb.micGA(i, df);
		}
	}

	public void Delete() {
		for (int i = 0; i < divideHyb.newPitsRules.size(); i++) {
			divideHyb.newPitsRules.get(i).removeRule();
		}
	}

	public void UniformCross() {

		int length = divideHyb.pitsRules.size();

		//子個体初期化
		divideHyb.newPitsRules.clear();

		for (int s = 0; s < length; s++) {
			divideHyb.newPitsCreat();
			divideHyb.pitsCross(s, popSize);
			divideHyb.pitsMutation(s);
		}

	}

	public void GeneticOperation(){

		int length = divideHyb.pitsRules.size();
		divideHyb.newPitsRules.clear();

		for (int s = 0; s < length; s++) {
			divideHyb.newPitsCreat();
			divideHyb.pitsAndMic(s, popSize, df);
			divideHyb.pitsMutation(s);
		}

	}

	public void oneObjUpdate() {

		Collections.sort(divideHyb.pitsRules, new PittsComparator());
		Collections.sort(divideHyb.newPitsRules, new PittsComparator());

		ArrayList<RuleSet> temp = new ArrayList<RuleSet>();

		StaticGeneralFunc.mergeSort(temp, divideHyb.pitsRules, divideHyb.newPitsRules);

		divideHyb.pitsRules = new ArrayList<RuleSet>(temp);
		divideHyb.newPitsRules.clear();

	}

	double out2objeAnother(RuleSet pit, int way){
		if(way == 0){
			return (double)(pit.getRuleLength());
		}else {
			return (double)(pit.getRuleNum());
		}
	}

	//ベスト系
	public RuleSet bestGenCalc() {

		RuleSet best;
		best = new RuleSet(divideHyb.pitsRules.get(0));
		if (objectives == 1) {
			for (int i = 0; i < divideHyb.pitsRules.size(); i++) {
				if (divideHyb.pitsRules.get(i).GetFitness(0) < best.GetFitness(0)) {
					best = new RuleSet(divideHyb.pitsRules.get(i));
				}
				else if (divideHyb.pitsRules.get(i).GetFitness(0) == best.GetFitness(0)) {
					if (divideHyb.pitsRules.get(i).getMissRate() < best.getMissRate()) {
						best = new RuleSet(divideHyb.pitsRules.get(i));
					}
				}
			}
		}

		else {

			for (int i = 0; i < divideHyb.pitsRules.size(); i++) {
				if (divideHyb.pitsRules.get(i).GetRank() == 0) {
					if (divideHyb.pitsRules.get(i).getMissRate() < best.getMissRate()) {
						best = new RuleSet(divideHyb.pitsRules.get(i));
					}
					else if (divideHyb.pitsRules.get(i).getMissRate() == best.getMissRate()) {
						if (divideHyb.pitsRules.get(i).getRuleNum() <= best.getRuleNum()) {
							if (divideHyb.pitsRules.get(i).getRuleLength() <= best.getRuleLength()) {
								best = new RuleSet(divideHyb.pitsRules.get(i));
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

	public RuleSet GetBestRuleSet(int objectives, Classifier all, ResultMaster res, DataSetInfo data, Dataset<Row> df, boolean isTest) {

		RuleSet best;

		for (int i = 0; i < all.pitsRules.size(); i++) {

			double fitness = 0;

			if(all.pitsRules.get(i).getRuleNum() != 0){

				all.pitsRules.get(i).setNumAndLength();

				if(isTest){
					double acc = (double) all.pitsRules.get(i).CalcAccuracyPalKai(df);
					all.pitsRules.get(i).SetTestMissRate( ( acc / (double)data.DataSize ) * 100.0 );
				}

				all.pitsRules.get(i).setNumAndLength();

				if (objectives == 1) {
					fitness = Consts.W1 * all.pitsRules.get(i).getMissRate() + Consts.W2 * all.pitsRules.get(i).getRuleNum() + Consts.W3 * all.pitsRules.get(i).getRuleLength();
					all.pitsRules.get(i).SetFitness(fitness, 0);
				} else if (objectives == 2) {
					all.pitsRules.get(i).SetFitness(all.pitsRules.get(i).getMissRate(), 0);
					all.pitsRules.get(i).SetFitness(all.pitsRules.get(i).out2obje(way), 1);
				} else if (objectives == 3) {
					all.pitsRules.get(i).SetFitness(all.pitsRules.get(i).getMissRate(), 0);
					all.pitsRules.get(i).SetFitness(all.pitsRules.get(i).getRuleNum(), 1);
					all.pitsRules.get(i).SetFitness(all.pitsRules.get(i).getRuleLength(), 2);
				} else {
					System.out.println("not be difined");
				}
			}

			else {
				for (int o = 0; o < objectives; o++) {
					fitness = 100000;
					all.pitsRules.get(i).SetFitness(fitness, o);
				}
			}
		}


		best = new RuleSet(all.pitsRules.get(0));
		if (objectives == 1) {
			for (int i = 0; i < all.pitsRules.size(); i++) {

				if (all.pitsRules.get(i).GetFitness(0) < best.GetFitness(0)) {
					best = new RuleSet(all.pitsRules.get(i));
				}

				else if (all.pitsRules.get(i).GetFitness(0) == best.GetFitness(0)) {
					if (all.pitsRules.get(i).getMissRate() < best.getMissRate()) {
						best = new RuleSet(all.pitsRules.get(i));
					}
				}

			}
		}

		else {

			//DisideRank(all.pitsRules);
			for (int i = 0; i < all.pitsRules.size(); i++) {
				int claNum = all.pitsRules.get(i).mulCla();		//そのルール集合の識別するクラス数

				if (all.pitsRules.get(i).GetRank() == 0) {

					res.setSolution(all.pitsRules.get(i).out2obje(way),
									all.pitsRules.get(i).GetFitness(0),
									all.pitsRules.get(i).GetTestMissRate(),
									out2objeAnother(all.pitsRules.get(i), way),
									claNum);


					if (all.pitsRules.get(i).GetFitness(0) < best.GetFitness(0)) {
						best = new RuleSet(all.pitsRules.get(i));
					}
					else if (all.pitsRules.get(i).GetFitness(0) == best.GetFitness(0)) {
						if (all.pitsRules.get(i).GetFitness(1) <= best.GetFitness(1)) {
							if (all.pitsRules.get(i).getRuleLength() <= best.getRuleLength()) {
								best = new RuleSet(all.pitsRules.get(i));
							}
						}
					}

				}
			}

		}
		if(isTest){
			double accTest = (double) best.CalcAccuracyPalKai(df)	/ data.DataSize;
			best.SetTestMissRate((1 - accTest) * 100);
		}

		best.setNumAndLength();

		return best;

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
