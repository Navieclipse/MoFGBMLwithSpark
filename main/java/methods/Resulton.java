package methods;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import navier.Cons;
import navier.Pittsburgh;
import navier.RuleSet;

public class Resulton {

	public Resulton() {}

	public Resulton(int pon, int repeat, int cv, int gen, String nameDir, int os){

		this.os = os;

		this.pon = pon;
		this.repeat = repeat;
		this.cv = cv;
		this.gen = gen;
		this.nameDir = nameDir;

		time = new double[cv * repeat * pon];
		timeCount = 0;
	}

	/******************************************************************************/
	int pon;
	int repeat;
	int cv;
	int gen;
	public String nameDir;

	int os;

	//時間
	double time[];
	double timeAve;
	int timeCount;

	//最終結果用
	double tra;
	double tst;
	double num;
	double len;

	double rareAve[] = new double[4];
	double rareAveRep[] = new double[4];
	double rareAveRepAll[] = new double[4];

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

	public void setSolution( double rul,double traErr,double tstErr,double leng ,int claNum){
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
		if(os == Cons.Uni){
			fileName = nameDir + "/solution/solution_"+pp+"_"+rr+"_"+cc+ "_" + Tra.size() +".txt";
			Configuration conf = new Configuration();
			Path hdfsPath = new Path( fileName);

			try {
				FileSystem fs = FileSystem.get(conf);
				OutputStream os = fs.create(hdfsPath);
			    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

			    for(int i=0;i<Tra.size();i++){
		        	br.write(Rul.get(i) + " " + Tra.get(i)+ " " + Tst.get(i)+ " " + Len.get(i)+ " " + Clanum.get(i) + "\n");
		        }

			    br.close();
			    fs.close();
			}
			catch (IOException ex){
				ex.printStackTrace();
		    }

		}
		else{
			fileName = nameDir + "\\solution\\solution_"+pp+"_"+rr+"_"+cc+ "_" + Tra.size() +".txt";
			try {
				FileWriter fw = new FileWriter(fileName, false);
				PrintWriter pw = new PrintWriter(new BufferedWriter(fw));

		        for(int i=0;i<Tra.size();i++){
		        	pw.println(Rul.get(i) + " " + Tra.get(i)+ " " + Tst.get(i)+ " " + Len.get(i)+ " " + Clanum.get(i));
		        }

		        pw.close();
			}
			catch (IOException ex) {
				ex.printStackTrace();
		    }
		}

	}

	/******************************************************************************/
	public void setTime(double time){
		this.time[timeCount] = time;
		timeCount++;
	}

	public void setTimeAve(){
		for(int i = 0; i < time.length; i++){
			timeAve = 0.0;
			timeAve += time[i];
		}
	}

	public void writeTime(double sec, double ns){

		String fileName;
		if(os == Cons.Uni){

			fileName = nameDir + "/write/Atime" + ".txt";
			Configuration conf = new Configuration();
			Path hdfsPath = new Path( fileName );
			try {
				FileSystem fs = FileSystem.get(conf);
				OutputStream os = fs.create(hdfsPath);
			    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

			    br.write(sec + " " + ns + "\n");

			    br.close();
			    fs.close();
			}
			catch (IOException ex){
				ex.printStackTrace();
		    }

		}
		else{
			fileName = nameDir + "\\write\\Atime" + ".txt";
			try {
		        FileWriter fw = new FileWriter(fileName, true);
		        PrintWriter pw = new PrintWriter(new BufferedWriter(fw));

		        pw.println(sec + " " + ns);
		        pw.close();

		        }
			 catch (IOException ex) {
		           ex.printStackTrace();
			 }
		}

	}

	public void writeAveTime(){

		String fileName;

		if(os == Cons.Uni){
			fileName = nameDir + "/write/Avetime" + ".txt";

			Configuration conf = new Configuration();
			Path hdfsPath = new Path( fileName );
			try {
				FileSystem fs = FileSystem.get(conf);
				OutputStream os = fs.create(hdfsPath);
			    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
			    br.write(timeAve + "\n");
			    br.close();
			    fs.close();
			}
			catch (IOException ex){
				ex.printStackTrace();
		    }

		}else{

			fileName = nameDir + "\\write\\Avetime" + ".txt";
			try {
		        FileWriter fw = new FileWriter(fileName, false);
		        PrintWriter pw = new PrintWriter(new BufferedWriter(fw));

		        pw.println(timeAve);

		        pw.close();

		        }
			 catch (IOException ex) {
		           ex.printStackTrace();
			 }

		}

	}

	/******************************************************************************/

	public void outputRules(RuleSet ruleset, int cc, int rr, int pp){
		String fileName;
		if(os == Cons.Uni){
			fileName = nameDir + "/ruleset/rules"  +"_"+pp+"_"+rr+"_"+cc+ ".txt";

			Configuration conf = new Configuration();
			Path hdfsPath = new Path( fileName );
			try {
				FileSystem fs = FileSystem.get(conf);
				OutputStream os = fs.create(hdfsPath);
			    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

			    for(int i=0;i<ruleset.pitsRules.size();i++){
		        	br.write("rule: " + i + " " + ruleset.pitsRules.get(i).getVecNum() + "\n");
		        	for(int f=0;f<ruleset.pitsRules.get(i).getMics().size();f++){
		        		for(int g=0;g<ruleset.pitsRules.get(i).getMics().get(f).getNdim();g++){
		        			br.write(ruleset.pitsRules.get(i).getMics().get(f).getRule(g)+" ");
				        }
		        		br.write(ruleset.pitsRules.get(i).getMics().get(f).getConc()+" ");
		        		br.write(ruleset.pitsRules.get(i).getMics().get(f).getCf()+" ");
		        		br.write("\n");
			        }
		        }
			    br.close();
			    fs.close();
			}
			catch (IOException ex){
				ex.printStackTrace();
		    }

		}else{
			fileName = nameDir + "\\ruleset\\rules"  +"_"+pp+"_"+rr+"_"+cc+ ".txt";

			try {
				FileWriter fw = new FileWriter(fileName, false);
				PrintWriter pw = new PrintWriter(new BufferedWriter(fw));

		        for(int i=0;i<ruleset.pitsRules.size();i++){
		        	pw.println("rule: " + i + " " + ruleset.pitsRules.get(i).getVecNum());
		        	for(int f=0;f<ruleset.pitsRules.get(i).getMics().size();f++){
		        		for(int g=0;g<ruleset.pitsRules.get(i).getMics().get(f).getNdim();g++){
				        	pw.print(ruleset.pitsRules.get(i).getMics().get(f).getRule(g)+" ");
				        }
		        		pw.print(ruleset.pitsRules.get(i).getMics().get(f).getConc()+" ");
		        		pw.print(ruleset.pitsRules.get(i).getMics().get(f).getCf()+" ");
		        		pw.println();
			        }
		        }

		        pw.close();
			}
			catch (IOException ex) {
				ex.printStackTrace();
		    }

		}

	}

	public void outputVec(RuleSet ruleset, int cc, int rr, int pp){
		String fileName;
		if(os == Cons.Uni){
			fileName = nameDir+ "/vecset/vecs"  +"_"+pp+"_"+rr+"_"+cc+ ".txt";

			Configuration conf = new Configuration();
			Path hdfsPath = new Path( fileName );
			try {
				FileSystem fs = FileSystem.get(conf);
				OutputStream os = fs.create(hdfsPath);
			    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

			    br.write("#ruleNum vecNum tra tst num len /n");
		        for(int i=0;i<ruleset.pitsRules.size();i++){
		        	br.write(i + " " + ruleset.pitsRules.get(i).getVecNum()
		        			+" "+ ruleset.pitsRules.get(i).getMissRate()
		        			+" "+ ruleset.pitsRules.get(i).GetTestMissRate()
		        			+" "+ ruleset.pitsRules.get(i).getRuleNum()
		        			+" "+ ruleset.pitsRules.get(i).getRuleLength() + "\n");
		        }

			    br.close();
			    fs.close();
			}
			catch (IOException ex){
				ex.printStackTrace();
		    }

		}else{
			fileName = nameDir + "\\vecset\\vecs"  +"_"+pp+"_"+rr+"_"+cc+ ".txt";

			try {
				FileWriter fw = new FileWriter(fileName, false);
				PrintWriter pw = new PrintWriter(new BufferedWriter(fw));

				pw.println("#ruleNum vecNum tra tst num len");

		        for(int i=0;i<ruleset.pitsRules.size();i++){
		        	pw.println(i + " " + ruleset.pitsRules.get(i).getVecNum()
		        			+" "+ ruleset.pitsRules.get(i).getMissRate()
		        			+" "+ ruleset.pitsRules.get(i).GetTestMissRate()
		        			+" "+ ruleset.pitsRules.get(i).getRuleNum()
		        			+" "+ ruleset.pitsRules.get(i).getRuleLength());
		        }
		        pw.close();
			}
			catch (IOException ex) {
				ex.printStackTrace();
		    }

		}

	}

	/******************************************************************************/

	public void writeBestLog(double tra, double tst, double num, double len, int Gen, int pon2, int repeat2, int cv2){
		String fileName;
		if(os == Cons.Uni){
			fileName = nameDir + "/write/" +pon2+repeat2+cv2+ "GEN" + ".txt";

			Configuration conf = new Configuration();
			Path hdfsPath = new Path( fileName );
			try {
				FileSystem fs = FileSystem.get(conf);
				OutputStream os = fs.create(hdfsPath);
			    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

			    br.write(Gen +" "+tra+" "+tst+" "+num+" "+len + "\n");

			    br.close();
			    fs.close();
			}
			catch (IOException ex){
				ex.printStackTrace();
		    }

		}else{
			fileName = nameDir + "\\write\\" +pon2+repeat2+cv2+ "GEN" + ".txt";
			try {
		        FileWriter fw = new FileWriter(fileName, true);
		        PrintWriter pw = new PrintWriter(new BufferedWriter(fw));

		        pw.println(Gen +" "+tra+" "+tst+" "+num+" "+len);
		        pw.close();

		        }
			 catch (IOException ex) {
		           ex.printStackTrace();
			 }
		}

	}

	public void setRare(Pittsburgh best){
		double tra1 = best.getMissRate();
		double tst1 = best.GetTestMissRate();
		double num1 = best.getRuleNum();
		double len1 = best.getRuleLength();
		this.tra = tra1;
		this.tst = tst1;
		this.num = num1;
		this.len = len1;
	}

	public void writeRare(Pittsburgh best){
		double tra = best.getMissRate();
		double tst = best.GetTestMissRate();
		double num = best.getRuleNum();
		double len = best.getRuleLength();
		String fileName;
		if(os == Cons.Uni){
			fileName = nameDir + "/write/Allwrite" + ".txt";

			Configuration conf = new Configuration();
			Path hdfsPath = new Path( fileName );
			try {
				FileSystem fs = FileSystem.get(conf);
				OutputStream os = fs.create(hdfsPath);
			    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

			    br.write(tra+" "+tst+" "+num+" "+len+ "\n");

			    br.close();
			    fs.close();
			}
			catch (IOException ex){
				ex.printStackTrace();
		    }

		}else{
			fileName = nameDir + "\\write\\Allwrite" + ".txt";
			try {
		        FileWriter fw = new FileWriter(fileName, true);
		        PrintWriter pw = new PrintWriter(new BufferedWriter(fw));

		        pw.println(tra+" "+tst+" "+num+" "+len);

		        pw.close();

		        }
			 catch (IOException ex) {
		           ex.printStackTrace();
			 }
		}

	}

	public void setRareAve(){
		rareAve[0] += tra;
		rareAve[1] += tst;
		rareAve[2] += num;
		rareAve[3] += len;
	}

	public void setRareAveRep(int pp, int j){

		rareAve[0] /= (double)(cv);
		rareAve[1] /= (double)(cv);
		rareAve[2] /= (double)(cv);
		rareAve[3] /= (double)(cv);

		rareAveRep[0] += rareAve[0];
		rareAveRep[1] += rareAve[1];
		rareAveRep[2] += rareAve[2];
		rareAveRep[3] += rareAve[3];

		Gmethod.write("res", nameDir,rareAve, pp, j, os);

		rareAve[0] = 0;
		rareAve[1] = 0;
		rareAve[2] = 0;
		rareAve[3] = 0;

	}

	public void setRareAveRepAll(int pp){

		rareAveRep[0] /= (double)(repeat);
		rareAveRep[1] /= (double)(repeat);
		rareAveRep[2] /= (double)(repeat);
		rareAveRep[3] /= (double)(repeat);

		rareAveRepAll[0] += rareAveRep[0];
		rareAveRepAll[1] += rareAveRep[1];
		rareAveRepAll[2] += rareAveRep[2];
		rareAveRepAll[3] += rareAveRep[3];

		Gmethod.write("res", nameDir,rareAveRep, pp, 100, os);

		rareAveRep[0] = 0;
		rareAveRep[1] = 0;
		rareAveRep[2] = 0;
		rareAveRep[3] = 0;
	}

	public void setRareAveRepAllFinal(){

			rareAveRepAll[0] /= (double)(pon);
			rareAveRepAll[1] /= (double)(pon);
			rareAveRepAll[2] /= (double)(pon);
			rareAveRepAll[3] /= (double)(pon);

		Gmethod.write("res", nameDir,rareAveRepAll, 1000, 1000, os);

	}

}
