package gbml;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import methods.FuzzyFunc;
import methods.MersenneTwisterFast;

public class Rule implements java.io.Serializable{

		/******************************************************************************/
		//コンストラクタ

		Rule(){}

		public Rule(Rule mic){
			this.rnd = mic.rnd;

			this.Ndim = mic.Ndim;
			this.Cnum = mic.Cnum;
			this.DataSize = mic.DataSize;
			this.TstDataSize = mic.TstDataSize;

			this.rule = Arrays.copyOf(mic.rule, mic.Ndim);

			this.conclution = mic.conclution;
			this.cf = mic.cf;
			this.ruleLength = mic.ruleLength;

		}

		Rule(MersenneTwisterFast rnd, int Ndim, int Cnum, int DataSize, int TstDataSize){
			this.rnd = new MersenneTwisterFast(rnd.nextInt());
			this.Ndim = Ndim;
			this.Cnum = Cnum;
			this.DataSize = DataSize;
			this.TstDataSize = TstDataSize;
		}

		Rule(int conc){
			this.conclution = conc;
		}

		/******************************************************************************/
		//引数
		MersenneTwisterFast rnd;

		//******************************************************************************//
	    //学習用
		int Ndim;												//次元
		int Cnum;												//クラス数
		int DataSize;											//パターン数
		int TstDataSize;										//パターン数

		//基本値
		int rule[];

		int conclution;
		double cf;
		int ruleLength;

		/******************************************************************************/
		//method

		//ルール作成
		public void setMic(){
			rule = new int[Ndim];
		}

		public void setTest(int size){
			this.TstDataSize = size;
		}

		public void makeRuleSingle(Row line, MersenneTwisterFast rnd2){
			rule = FuzzyFunc.selectSingle(line, Ndim, rnd2);
		}

		public void calcRuleConc(Dataset<Row> df){

			double[] trust = FuzzyFunc.calcTrust(df, rule, Cnum);
			conclution = FuzzyFunc.calcConclusion(trust, Cnum);
			cf = FuzzyFunc.calcCf(conclution, trust, Cnum);

	        ruleLength = ruleLengthCalc();
		}

		public void makeRuleRnd1(MersenneTwisterFast rnd2){
			rule = FuzzyFunc.selectRnd(Ndim, rnd2);
		}

		public void makeRuleRnd2(){
	    	conclution = rnd.nextInt(Cnum);
	    	cf = rnd.nextDouble();
	        ruleLength = ruleLengthCalc();
		}

		public void makeRuleNoCla(int[] noClass){
	    	conclution = noClass[rnd.nextInt(noClass.length)];
	    	cf = rnd.nextDouble();
	        ruleLength = ruleLengthCalc();
		}

		public void makeRuleCross(int ansCla, double cf){
	    	conclution = ansCla;
	    	this.cf = cf;
	        ruleLength = ruleLengthCalc();
		}

		public int getRuleLength(){
			return ruleLength;
		}

		public double calcAdaptationPureSpark(Row lines){
	    	return  FuzzyFunc.menberMulPureSpark(lines, rule);
		}

		public double getCf(){
			return cf;
		}

		public void changeCF(){
			if(cf!=0){
				cf += rnd.nextDouble()/50 - 0.01;
				if(cf<0){
					cf = 0;
				}else if(cf > 1){
					cf = 1;
				}
			}
		}

		public int getConc(){
			return conclution;
		}

		public int getLength(){
			return ruleLength;
		}

		public int ruleLengthCalc(){
			int ans=0;
			for(int i=0;i<Ndim;i++){
				if(rule[i]!=0){
					ans++;
				}
			}
			return ans;
		}

		public void setRule(int num, int ruleN){
			rule[num] = ruleN;
		}

		public int getRule(int num){
			return rule[num];
		}

		public void mutation(int i, MersenneTwisterFast rnd2){

			int v;
			do {
				v = rnd2.nextInt(Consts.FUZZY_SET_NUM + 1);
			} while (v == rule[i]);
			rule[i] = v;

		    cf = rnd.nextDouble();

		    //総ルール長
		    ruleLength = ruleLengthCalc();

		}

		//NSGA2用
		public void CalcMuData2(double pattern[][], int newDataSize){
			FuzzyFunc kk = new FuzzyFunc();
			kk.KKkk(Consts.MAX_FUZZY_DIVIDE_NUM);
			this.DataSize = newDataSize;
		}

		public void setSize(int m){
			this.DataSize = m;
		}

		public void setConc(int conc){
			this.conclution = conc;
		}

		public int getNdim(){
			return Ndim;
		}

		public void micCopy(Rule mic){
			this.rnd = new MersenneTwisterFast(rnd.nextInt());

			this.Ndim = mic.Ndim;
			this.Cnum = mic.Cnum;
			this.DataSize = mic.DataSize;
			this.TstDataSize = mic.TstDataSize;

			this.rule = Arrays.copyOf(mic.rule, mic.Ndim);

			this.conclution = mic.conclution;;
			this.cf = mic.cf;
			this.ruleLength = mic.ruleLength;

		}



}
