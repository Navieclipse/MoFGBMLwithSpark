package gbml;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import methods.GeneralFunc;
import methods.MersenneTwisterFast;
import moead.Moead;


public class Classifier{

	//コンストラクタ
	Classifier(){}

	public Classifier(MersenneTwisterFast rnd, int objectives){

		this.rnd = rnd;
		this.rnd2 = new MersenneTwisterFast(rnd.nextInt());
		this.objectives = objectives;
		//this.Dpop = Dpop;

	}

	/******************************************************************************/
	//引数
	MersenneTwisterFast rnd;
	MersenneTwisterFast rnd2;

	//******************************************************************************//

	//基本
	public ArrayList<RuleSet> pitsRules = new ArrayList<RuleSet>();

	public ArrayList<RuleSet> newPitsRules = new ArrayList<RuleSet>();

	public ArrayList<RuleSet> allPitsRules = new ArrayList<RuleSet>();

	//読み取った値
	int GenNum;
	int os;

	int Ndim;
	int Cnum;
	int DataSize;
	int DataSizeTst;

	int objectives;
	//ForkJoinPool Dpop;

	/******************************************************************************/
	//メソッド

	public void initialPal(DataSetInfo data, Dataset<Row> df, int popSize){

		Ndim = data.getNdim();
		Cnum = data.getCnum();
		DataSize = data.getDataSize();

		for(int i=0;i<popSize;i++){
			pitsRules.add( new RuleSet( rnd, Ndim, Cnum, DataSize, DataSizeTst, objectives) );
			pitsRules.get(i).initialMic(data, df);
		}

		for(int i=0;i<popSize;i++){
			newPitsRules.add( new RuleSet( rnd2, Ndim, Cnum, DataSize, DataSizeTst, objectives) );
		}

	}

	void pitsMutation(int num){
		int len = newPitsRules.get(num).getRuleNum();

		for(int i =0; i<len; i++){
			for(int j =0; j<Ndim; j++){
				if(rnd.nextInt(len * Ndim) == 0){
					newPitsRules.get(num).micMutation(i, j);
				}
			}
		}
	}

	void micGA(int num, Dataset<Row> df){
		if(rnd.nextDouble() < Consts.RULE_OPE_RT && newPitsRules.get(num).getRuleNum() != 0){
			newPitsRules.get(num).micGenHeuris(df);
		}
	}

	void newPitsCreat(){
		newPitsRules.add( new RuleSet(rnd, Ndim, Cnum, DataSize, DataSizeTst, objectives) );
	}

	void pitsCross(int num, int popSize){

		int mom, pop;
		int Nmom, Npop;

		boolean hasParent = Consts.HAS_PARENT;
		if(!hasParent){
			mom = GeneralFunc.binaryT4(pitsRules, rnd, popSize, objectives);
			pop = GeneralFunc.binaryT4(pitsRules, rnd, popSize, objectives);
		}
		else{
			int[] parent = GeneralFunc.binaryTRand(pitsRules, rnd, popSize, objectives);
			mom = parent[0];
			pop = parent[1];
		}

		if(rnd.nextDouble() < (double)(Consts.RULESET_CROSS_RT)){

			Nmom = rnd.nextInt(pitsRules.get(mom).getRuleNum()) + 1;
			Npop = rnd.nextInt(pitsRules.get(pop).getRuleNum()) + 1;

			if((Nmom + Npop) > Consts.MAX_RULE_NUM){
				int delNum = Nmom + Npop - Consts.MAX_RULE_NUM;
				for(int v=0;v<delNum;v++){
					if(rnd.nextBoolean()){
						Nmom--;
					}
					else{
						Npop--;
					}
				}
			}

	        int pmom[] = new int[Nmom];
	        int ppop[] = new int[Npop];

	        pmom = GeneralFunc.sampringWithout2(Nmom, pitsRules.get(mom).getRuleNum(), rnd);
	        ppop = GeneralFunc.sampringWithout2(Npop, pitsRules.get(pop).getRuleNum(), rnd);

	        newPitsRules.get(num).micRules.clear();

	        for(int j=0;j<Nmom;j++){
	        	newPitsRules.get(num).setMicRule(pitsRules.get(mom).getMicRule(pmom[j]));
	        }
	        for(int j=0;j<Npop;j++){
	        	newPitsRules.get(num).setMicRule(pitsRules.get(pop).getMicRule(ppop[j]));
	        }

		}
		else{//親をそのまま子個体に
			if(rnd.nextBoolean()){
				RuleSet deep = new RuleSet(pitsRules.get(mom));
				newPitsRules.get(num).pitsCopy(deep);
			}
			else{
				RuleSet deep = new RuleSet(pitsRules.get(pop));
				newPitsRules.get(num).pitsCopy(deep);
			}
		}
		newPitsRules.get(num).setRuleNum();

	}

	void pitsAndMic(int num, int popSize){

		int mom, pop;
		int Nmom, Npop;

		//親選択
		mom = GeneralFunc.binaryT4(pitsRules,rnd, popSize, objectives);
		pop = GeneralFunc.binaryT4(pitsRules,rnd, popSize, objectives);

		if(rnd.nextDouble() < (double)Consts.RULE_OPE_RT){									//半分ミシガン
			RuleSet deep = new RuleSet(pitsRules.get(mom));
			newPitsRules.get(num).pitsCopy(deep);
			newPitsRules.get(num).setRuleNum();

			if(newPitsRules.get(num).getRuleNum() != 0){
				newPitsRules.get(num).micGenRandom();
			}
		}else{
			if(rnd.nextDouble() < (double)(Consts.RULESET_CROSS_RT)){							//半分ピッツ
				Nmom = rnd.nextInt(pitsRules.get(mom).getRuleNum()) + 1;
				Npop = rnd.nextInt(pitsRules.get(pop).getRuleNum()) + 1;

				if((Nmom + Npop) > Consts.MAX_RULE_NUM){
					int delNum = Nmom + Npop - Consts.MAX_RULE_NUM;
					for(int v=0;v<delNum;v++){
						if(rnd.nextBoolean()){
							Nmom--;
						}
						else{
							Npop--;
						}
					}
				}

		        int pmom[] = new int[Nmom];
		        int ppop[] = new int[Npop];

		        pmom = GeneralFunc.sampringWithout2(Nmom, pitsRules.get(mom).getRuleNum(), rnd);
		        ppop = GeneralFunc.sampringWithout2(Npop, pitsRules.get(pop).getRuleNum(), rnd);

		        newPitsRules.get(num).micRules.clear();

		        for(int j=0;j<Nmom;j++){
		        	newPitsRules.get(num).setMicRule(pitsRules.get(mom).getMicRule(pmom[j]));
		        }
		        for(int j=0;j<Npop;j++){
		        	newPitsRules.get(num).setMicRule(pitsRules.get(pop).getMicRule(ppop[j]));
		        }

			}
			else{//親をそのまま子個体に
				if(rnd.nextBoolean()){
					RuleSet deep = new RuleSet(pitsRules.get(mom));
					newPitsRules.get(num).pitsCopy(deep);
				}
				else{
					RuleSet deep = new RuleSet(pitsRules.get(pop));
					newPitsRules.get(num).pitsCopy(deep);
				}
			}
			newPitsRules.get(num).setRuleNum();
		}

	}

	void addNewPits(int num){
		for(int i = 0; i<num; i++){
			newPitsRules.add( new RuleSet(rnd,Ndim,Cnum,DataSize,DataSizeTst,objectives) );
		}
	}

	void pitsCrossRam(int num, int popSize, Moead moe){

		int mom, pop;
		int Nmom, Npop;

		int[] numOfParents;
		do{
			numOfParents = moe.matingSelection(num, 2);
		}while(pitsRules.get(numOfParents[0]).getRuleNum() == 0||pitsRules.get(numOfParents[1]).getRuleNum() == 0);

		mom = numOfParents[0];
		pop = numOfParents[1];

		if(rnd.nextDouble() < (double)(Consts.RULESET_CROSS_RT)){

			Nmom = rnd.nextInt(pitsRules.get(mom).getRuleNum()) + 1;
			Npop = rnd.nextInt(pitsRules.get(pop).getRuleNum()) + 1;

			if((Nmom + Npop) > Consts.MAX_RULE_NUM){
				int delNum = Nmom + Npop - Consts.MAX_RULE_NUM;
				for(int v=0;v<delNum;v++){
					if(rnd.nextBoolean()){
						Nmom--;
					}
					else{
						Npop--;
					}
				}
			}

	        int pmom[] = new int[Nmom];
	        int ppop[] = new int[Npop];

	        pmom = GeneralFunc.sampringWithout2(Nmom, pitsRules.get(mom).getRuleNum(), rnd);
	        ppop = GeneralFunc.sampringWithout2(Npop, pitsRules.get(pop).getRuleNum(), rnd);

	        for(int j=0;j<Nmom;j++){
	        	newPitsRules.get(num).setMicRule(pitsRules.get(mom).getMicRule(pmom[j]));
	        }
	        for(int j=0;j<Npop;j++){
	        	newPitsRules.get(num).setMicRule(pitsRules.get(pop).getMicRule(ppop[j]));
	        }

		}
		else{//親をそのまま子個体に
			if(rnd.nextBoolean()){
				newPitsRules.get(num).replace(pitsRules.get(mom));
			}
			else{
				newPitsRules.get(num).replace(pitsRules.get(pop));
			}
		}
		newPitsRules.get(num).setRuleNum();

	}

}
