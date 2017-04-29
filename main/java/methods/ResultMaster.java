package methods;

import java.util.ArrayList;

import gbml.Consts;
import gbml.RuleSet;
import gbml.Classifier;

public class ResultMaster {

	public ResultMaster() {}

	public ResultMaster(String nameDir, int os){
		this.os = os;
		this.nameDir = nameDir;
	}

	/******************************************************************************/
	int os;
	public String nameDir;

	//時間
	ArrayList<Double> times = new ArrayList<Double>();

	//最終結果用
	ArrayList<Double> Trains = new ArrayList<Double>();
	ArrayList<Double> Tests = new ArrayList<Double>();
	ArrayList<Double> Rules = new ArrayList<Double>();
	ArrayList<Double> Lengths = new ArrayList<Double>();

	//非劣解
	ArrayList<Double> Tra = new ArrayList<Double>();
	ArrayList<Double> Tst = new ArrayList<Double>();
	ArrayList<Double> Rul = new ArrayList<Double>();
	ArrayList<Double> Len = new ArrayList<Double>();
	ArrayList<Integer> Clanum = new ArrayList<Integer>();

	public String getDirName(){
		return nameDir;
	}

	/******************************************************************************/

	public void setSolution( double rul, double traErr, double tstErr, double leng , int claNum){
			this.Rul.add(rul);
			this.Tra.add(traErr);
			this.Tst.add((tstErr));
			this.Len.add(leng);
			this.Clanum.add(claNum);
	}

	public void resetSolution(){
		Tra.clear();
		Tst.clear();
		Rul.clear();
		Len.clear();
		Clanum.clear();
	}

	public void outSolution(int cc, int rr, int pp){

		String fileName;
		if(os == Consts.WINDOWS){
			fileName = nameDir + "\\solution\\solution_"+pp+"_"+rr+"_"+cc+ "_" + Tra.size() +".txt";

		}
		else{
			fileName = nameDir + "/solution/solution_"+pp+"_"+rr+"_"+cc+ "_" + Tra.size() +".txt";
		}

		String[] strs = new String[Tra.size()];
		for(int i=0; i<Tra.size(); i++){
			strs[i] = Rul.get(i) + " " + Tra.get(i)+ " " + Tst.get(i)+ " " + Len.get(i)+ " " + Clanum.get(i);
		}
		Output.writeln(fileName, strs, os);

	}

	/******************************************************************************/
	public void setTime(double time){
		this.times.add(time);
	}

	public void writeTime(double sec, double ns, int cv, int rep, int pp){

		String fileName;
		if(os == Consts.WINDOWS){
			fileName = nameDir + "\\write\\Atime_" + pp + rep + cv + ".txt";
		}
		else{
			fileName = nameDir + "/write/Atime_" + pp + rep + cv + ".txt";
		}

		String str = sec + " " + ns;

		Output.writeln(fileName, str, os);
	}

	public void writeAveTime(){

		Double timeAve = 0.0;
		for(int i=0; i<times.size(); i++){
			timeAve += times.get(i);
		}
		timeAve /= times.size();

		String fileName;
		if(os == Consts.WINDOWS){
			fileName = nameDir + "\\write\\Avetime" + ".txt";
		}else{
			fileName = nameDir + "/write/Avetime" + ".txt";
		}

		Output.writeln(fileName, String.valueOf(timeAve), os);

	}

	/******************************************************************************/
	public void outputRules(Classifier ruleset, int cc, int rr, int pp){
		String fileName;
		if(os == Consts.WINDOWS){
			fileName = nameDir + "\\ruleset\\rules"  +"_"+pp+"_"+rr+"_"+cc+ ".txt";
		}else{
			fileName = nameDir + "/ruleset/rules"  +"_"+pp+"_"+rr+"_"+cc+ ".txt";
		}

		ArrayList<String> strs = new ArrayList<String>();
		for(int i=0;i<ruleset.pitsRules.size();i++){
			strs.add( "rule " + i + " " + ruleset.pitsRules.get(i).getVecNum() );
        	for(int f=0;f<ruleset.pitsRules.get(i).getMics().size();f++){
    			String str = "";
        		for(int g=0;g<ruleset.pitsRules.get(i).getMics().get(f).getNdim();g++){
		        	str += ruleset.pitsRules.get(i).getMics().get(f).getRule(g) + " " ;
		        }
        		str += ruleset.pitsRules.get(i).getMics().get(f).getConc() + " ";
        		str += ruleset.pitsRules.get(i).getMics().get(f).getCf();
        		strs.add( str );
	        }
        }

		String[] array = (String[]) strs.toArray(new String[0]);
		Output.writeln(fileName, array, os);
	}

	public void outputVec(Classifier ruleset, int cc, int rr, int pp){
		String fileName;
		if(os == Consts.WINDOWS){
			fileName = nameDir + "\\vecset\\vecs"  +"_"+pp+"_"+rr+"_"+cc+ ".txt";
		}else{
			fileName = nameDir+ "/vecset/vecs"  +"_"+pp+"_"+rr+"_"+cc+ ".txt";
		}

		ArrayList<String> strs = new ArrayList<String>();
		for(int i=0;i<ruleset.pitsRules.size();i++){
			strs.add( i + " " + ruleset.pitsRules.get(i).getVecNum()
					+" "+ ruleset.pitsRules.get(i).getMissRate()
					+" "+ ruleset.pitsRules.get(i).GetTestMissRate()
					+" "+ ruleset.pitsRules.get(i).getRuleNum()
					+" "+ ruleset.pitsRules.get(i).getRuleLength() );
	    }

		String[] array = (String[]) strs.toArray(new String[0]);
		Output.writeln(fileName, array, os);
	}

	/******************************************************************************/
	public void writeBestLog(double tra, double tst, double num, double len, int Gen, int pon, int repeat, int cv){
		String fileName;
		if(os == Consts.WINDOWS){
			fileName = nameDir + "\\write\\" + pon + repeat + cv + "GEN" + ".txt";
		}else{
			fileName = nameDir + "/write/" + pon + repeat + cv + "GEN" + ".txt";
		}

		String str = Gen + " " + tra + " " + tst +" "+ num +" "+ len;
		Output.writeln(fileName, str, os);
	}

	public void setBest(RuleSet best){
		Trains.add( best.getMissRate() );
		Tests.add( best.GetTestMissRate() );
		Rules.add( (double)best.getRuleNum() );
		Lengths.add( (double)best.getRuleLength() );
	}

	public void writeAllbest(RuleSet best, int cv, int rep, int pp){

		String fileName;
		if(os == Consts.WINDOWS){
			fileName = nameDir + "\\write\\Allbest_" + pp + rep + cv + ".txt";
		}else{
			fileName = nameDir + "/write/Allbest_" + pp + rep + cv + ".txt";
		}

		String str = best.getMissRate() + " " +
					 best.GetTestMissRate() + " " +
					 best.getRuleNum() + " " +
					 best.getRuleLength();

		Output.writeln(fileName, str, os);
	}

	public void writeBestAve(){

		Double trainAve = 0.0;
		Double testAve = 0.0;
		Double ruleAve = 0.0;
		Double lengthAve = 0.0;

		for(int i=0; i<Trains.size(); i++){
			trainAve += Trains.get(i);
			testAve += Tests.get(i);
			ruleAve += Rules.get(i);
			lengthAve += Lengths.get(i);
		}
		trainAve /= Trains.size();
		testAve /= Tests.size();
		ruleAve /= Rules.size();
		lengthAve /= Lengths.size();

		String fileName;
		if(os == Consts.WINDOWS){
			fileName = nameDir + "\\write\\" + "AllbestAve" + ".txt";
		}else{
			fileName = nameDir + "/write/" + "AllbestAve" + ".txt";
		}

		String str = trainAve +" "+  testAve +" "+  ruleAve +" "+  lengthAve;
		Output.writeln(fileName, str, os);
	}

}
