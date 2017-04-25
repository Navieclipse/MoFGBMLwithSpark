package navier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import methods.Fmethod;
import methods.Gmethod;
import methods.MersenneTwisterFast;

public class Pittsburgh implements java.io.Serializable{
	/******************************************************************************/
	//コンストラクタ

	Pittsburgh(){}

	Pittsburgh(MersenneTwisterFast rnd,int Ndim, int Cnum, int DataSize, int DataSizeTst, int objectibes){
		this.rnd = rnd;
		this.rnd2 = new MersenneTwisterFast(rnd.nextInt());
		this.rnd3 = new MersenneTwisterFast(rnd.nextInt());
		this.rnd4 = new MersenneTwisterFast(rnd.nextInt());
		this.rnd5 = new MersenneTwisterFast(rnd.nextInt());
		this.rnd6 = new MersenneTwisterFast(rnd.nextInt());
		this.Ndim = Ndim;
		this.Cnum = Cnum;
		this.DataSize = DataSize;
		this.DataSizeTst = DataSizeTst;

		this.evaflag = 0;
		this.rank = 0;
		this.crowding = 0;
		this.vecNum =0;
		this.fitnesses = new double[objectibes];
		this.firstobj = new double[objectibes];
	}

	public Pittsburgh(Pittsburgh pits){

		this.rnd = pits.rnd;
		this.rnd2 = pits.rnd2;
		this.rnd3 = pits.rnd3;
		this.rnd4 = pits.rnd4;
		this.rnd5 = pits.rnd5;
		this.rnd6 = pits.rnd6;
		this.Ndim = pits.Ndim;
		this.Cnum = pits.Cnum;
		this.DataSize = pits.DataSize;
		this.DataSizeTst = pits.DataSizeTst;

		this.missRate = pits.missRate;
		this.ruleNum = pits.ruleNum;
		this.ruleLength = pits.ruleLength;
		this.fitness = pits.fitness;

		this.vecNum = pits.vecNum;

		Michigan a;
		this.micRules.clear();
		for(int i=0;i<pits.micRules.size();i++){
			 a = new Michigan(pits.micRules.get(i));
			this.micRules.add(a);
		}

		this.evaflag = pits.evaflag;
		this.testMissRate = pits.testMissRate;
		this.rank = pits.rank;
		this.crowding = pits.crowding;
		fitnesses = Arrays.copyOf(pits.fitnesses, pits.fitnesses.length);
		firstobj = Arrays.copyOf(pits.firstobj, pits.fitnesses.length);

	}

	public Pittsburgh(Pittsburgh pits, int vec){

		this.rnd = pits.rnd;
		this.rnd2 = pits.rnd2;
		this.rnd3 = pits.rnd3;
		this.rnd4 = pits.rnd4;
		this.rnd5 = pits.rnd5;
		this.rnd6 = pits.rnd6;
		this.Ndim = pits.Ndim;
		this.Cnum = pits.Cnum;
		this.DataSize = pits.DataSize;
		this.DataSizeTst = pits.DataSizeTst;

		this.missRate = pits.missRate;
		this.ruleNum = pits.ruleNum;
		this.ruleLength = pits.ruleLength;
		this.fitness = pits.fitness;

		this.vecNum = vec;

		Michigan a;
		this.micRules.clear();
		for(int i=0;i<pits.micRules.size();i++){
			 a = new Michigan(pits.micRules.get(i));
			this.micRules.add(a);
		}

		this.evaflag = pits.evaflag;
		this.testMissRate = pits.testMissRate;
		this.rank = pits.rank;
		this.crowding = pits.crowding;
		fitnesses = Arrays.copyOf(pits.fitnesses, pits.fitnesses.length);
		firstobj = Arrays.copyOf(pits.firstobj, pits.fitnesses.length);
	}

	/******************************************************************************/
	//引数
	MersenneTwisterFast rnd;
	MersenneTwisterFast rnd2;
	MersenneTwisterFast rnd3;
	MersenneTwisterFast rnd4;
	MersenneTwisterFast rnd5;
	MersenneTwisterFast rnd6;

	//******************************************************************************//
    //学習用
	int Ndim;												//次元
	int Cnum;												//クラス数
	int DataSize;											//パターン数
	int DataSizeTst;										//パターン数

	//基本値
	ArrayList<Michigan> micRules = new ArrayList<Michigan>();

	ArrayList<Michigan> newMicRules = new ArrayList<Michigan>();

	double missRate;
	double testMissRate;
	int ruleNum;
	int ruleLength;

	//並べ替えの基準(1obj)
	double fitness;
	//2目的以上
	double fitnesses[];

	//MOEAD用
	int vecNum;
	double otherDataRate;

	//NSGA用
	int evaflag;
	int rank;
	double crowding;
	double firstobj[];

	/******************************************************************************/
	//メソッド
	public void initialMic(DataSetInfo data, Dataset<Row> df){
		//ヒューリスティック生成を行う場合
		boolean isHeuris = Cons.isHeuris;
        Dataset<Row> dfsample;
        List<Row> samples = null;
        if(isHeuris){ //サンプリング
			double sampleSize = (double)(Cons.Nini+10) / (double)DataSize;
			dfsample = df.sample( false, sampleSize, rnd4.nextInt() );
			samples = dfsample.collectAsList();
        }
        do{ //while( micRules.size() == 0)
        	for(int i=0; i<Cons.Nini; i++){
        		micRules.add( new Michigan(rnd2, Ndim, Cnum, DataSize, DataSizeTst) );
        		micRules.get(i).setMic();

        		if(isHeuris){		//ヒューリスティック生成
					micRules.get(i).makeRuleSingle(samples.get(i), rnd4);
					micRules.get(i).calcRuleConc(df);
        		}else{				//完全ランダム生成
					micRules.get(i).makeRuleRnd1(rnd4);
					micRules.get(i).makeRuleRnd2();
        		}
        	}
        	removeRule();

        }while( micRules.size()==0 );

		setFitness(data, df);
	}

	public void setFitness(DataSetInfo data, Dataset<Row> df){

		removeRule();

		ruleNum = micRules.size();
		ruleLength = ruleLengthCalc();

		if(ruleNum==0 || ruleLength==0){
			fitness = 1000000;
			missRate = 1000000;
		}
		else{
			double ans = CalcAccuracyPalKai(df);
			double acc = ans / data.getDataSize();
			missRate = ( (1.0 - acc) * 100 );
			fitness = Fmethod.fitness(missRate, (double)ruleNum, (double)ruleLength);
		}

	}

	public void removeRule(){

		for (int i = 0; i < micRules.size(); i++) {
			int size = 0;
			while (micRules.size() > size) {
				if (micRules.get(size).getCf() <= 0 || micRules.get(size).getRuleLength() == 0) {
				//if(micRules.get(size).getConc() == -1){
					micRules.remove(size);
				}
				else {
					size++;
				}
			}
		}
	}

	public int ruleLengthCalc(){
		int ans = 0;
		for(int i=0; i<ruleNum; i++){
			ans += micRules.get(i).getLength();
		}

		ruleLength = ans;

		return ans;
	}

	public int getRuleNum(){
		return ruleNum;
	}

	public int getRuleLength(){
		return ruleLength;
	}

	public double getMissRate(){
		return missRate;
	}

	public void setRuleNum(){
		ruleNum = micRules.size();
	}

	public void setLength(){
		ruleLength = ruleLengthCalc();
	}

	public double getFitness(){
		return fitness;
	}

	public Michigan getMicRule(int num){
		return micRules.get(num);
	}

	public void setMicRule(Michigan micrule){

		Michigan mic = new Michigan(micrule);
		micRules.add(mic);

	}

	public void micMutation(int num, int i){
		micRules.get(num).mutation(i, rnd5);
	}

	public void micGenRandom(){

		//交叉個体数（ルールの20％）あるいは１個
		int snum;
		if(rnd.nextDouble() < (double)Cons.Micope){
			snum = (int)((ruleNum - 0.00001) * Cons.MicNum) + 1;
		}else{
			snum = 1;
		}

		//合計生成個体数
		int heuNum, genNum = 0;
		if(snum % 2 == 0){
			heuNum = snum/2;
			genNum = snum/2;
		}
		else{
			int plus = rnd4.nextInt(2);
			heuNum = (snum-1)/2 + plus;
			genNum = snum - heuNum;
		}

		for(int i=0;i<genNum;i++){
			micge(i);
		}

		for(int i=genNum; i<snum; i++){
			randomGeneration(i);
		}

		//旧個体の削除，新個体の追加
		micUpdate(snum);

	}

	public void micGenHeuris(Dataset<Row> df){

		//交叉個体数（ルールの20％）あるいは１個
		int snum;
		if(rnd.nextDouble() < (double)Cons.Micope){
			snum = (int)((ruleNum - 0.00001) * Cons.MicNum) + 1;
		}else{
			snum = 1;
		}

		//合計生成個体数
		int heuNum, genNum = 0;
		if(snum % 2 == 0){
			heuNum = snum/2;
			genNum = snum/2;
		}
		else{
			int plus = rnd4.nextInt(2);
			heuNum = (snum-1)/2 + plus;
			genNum = snum - heuNum;
		}

		//ヒューリスティック生成の誤識別パターン(クソ重い処理なんじゃ)
		Dataset<Row> dfmisspat = df.filter( s -> CalcWinClassPalSpark(s) != s.getInt(Ndim) );
		int MissDataNum = (int)dfmisspat.count();
		if(MissDataNum < heuNum) dfmisspat = df;  //ミスパターンがない場合はパターン全体から

		double sampleSize = (double)(heuNum) / (double)MissDataNum;
		int increment = 0;
		Dataset<Row> dfmisspatSample;
		do{
			dfmisspatSample = dfmisspat.sample( false, sampleSize, rnd2.nextInt() );
			sampleSize = (double)(heuNum+increment++) / (double)MissDataNum;
		}while(dfmisspatSample.count() < heuNum);
		List<Row> misspat = dfmisspatSample.collectAsList();

		for(int i=0;i<genNum;i++){
			micge(i);
		}
		int missPatIndex = 0;
		for(int i=genNum; i<snum; i++){
			heuristicGeneration( i, misspat.get(missPatIndex++), df);
		}

		//旧個体の削除，新個体の追加
		micUpdate(snum);

	}

	public void micge(int num){

		newMicRules.add( new Michigan(rnd2, Ndim, Cnum, DataSize, DataSizeTst) );
		newMicRules.get(num).setMic();

		//mom
		int mom = rnd4.nextInt(ruleNum);
		int pop = rnd3.nextInt(ruleNum);

		if(rnd2.nextDouble() < (Cons.CrossM)){
			//一様交叉
			int k=0;
			int k2=0;
			int o = 0;
			for(int i=0;i<Ndim;i++){
				k = rnd6.nextInt(2);
				if(k==0){
					newMicRules.get(num).setRule(i, micRules.get(mom).getRule(i));
				}
				else{
					newMicRules.get(num).setRule(i, micRules.get(pop).getRule(i));
				}
				k2 = rnd5.nextInt(Ndim);
				//突然変異
				if(k2==0){
					do{
						o = rnd4.nextInt(Cons.Fnum +1);
					}while(o == newMicRules.get(num).getRule(i));
					newMicRules.get(num).setRule(i, o);
				}
			}
		}

		else{
			int o = 0;
			int k2 = 0;
			for(int i=0;i<Ndim;i++){
				newMicRules.get(num).setRule(i, micRules.get(mom).getRule(i));
				//突然変異
				k2 = rnd3.nextInt(Ndim);
				if(k2==0){
					do{
						o = rnd2.nextInt(Cons.Fnum +1);
					}while(o == newMicRules.get(num).getRule(i));
					newMicRules.get(num).setRule(i, o);
				}
			}
		}

		//後件部momに合わす．
		newMicRules.get(num).makeRuleCross(micRules.get(mom).getConc(), micRules.get(mom).getCf());

		//newMicRules.get(num).makeRuleRnd2();

	}

	int[] calcHaveClass(){
		int noCla[] = micRules.stream()
					.mapToInt(r ->r.getConc())
					.distinct()
					.sorted()
					.toArray();
		return noCla;
	}

	int[] calcNoClass(){
		int haveClass[] = calcHaveClass();

		List<Integer> noCla = new ArrayList<Integer>();
		for (int num= 0; num < Cnum; num++) {
			boolean ishave = false;
			for (int have_i = 0; have_i < haveClass.length; have_i++) {
				if(num == haveClass[have_i]) ishave =true;
			}
			if(!ishave) noCla.add(num);
		}

		int noClass[] = noCla.stream().mapToInt(s->s).toArray();

		return noClass;
	}

	public void randomGeneration(int num){
		//足りていないクラスの個体生成を優先
		//識別器中のクラス判別
		int noCla[] = calcNoClass();
		newMicRules.add( new Michigan(rnd2, Ndim, Cnum, DataSize, DataSizeTst) );
		newMicRules.get(num).setMic();
		newMicRules.get(num).makeRuleRnd1(rnd2);
		if(noCla.length == 0){
			newMicRules.get(num).makeRuleRnd2();
		}else{
			newMicRules.get(num).makeRuleNoCla(noCla);
		}

	}

	public void heuristicGeneration(int num, Row line, Dataset<Row> df){
		newMicRules.add( new Michigan(rnd2, Ndim, Cnum, DataSize, DataSizeTst) );
		newMicRules.get(num).setMic();
		newMicRules.get(num).makeRuleSingle(line, rnd4);
		newMicRules.get(num).calcRuleConc(df);
	}

	public void micUpdate(int snum){

		int repNum[] = Gmethod.sampringWithout2(snum, micRules.size(), rnd);
		for(int i=0; i<snum; i++){
			micRules.get(repNum[i]).micCopy(newMicRules.get(i));
		}

	}

	public void pitsCopy(Pittsburgh pits){
		this.rnd = pits.rnd;
		this.rnd2 = pits.rnd2;
		this.rnd3 = pits.rnd3;
		this.rnd4 = pits.rnd4;
		this.rnd5 = pits.rnd5;
		this.rnd6 = pits.rnd6;
		this.Ndim = pits.Ndim;
		this.Cnum = pits.Cnum;
		this.DataSize = pits.DataSize;
		this.DataSizeTst = pits.DataSizeTst;

		this.missRate = pits.missRate;
		this.ruleNum = pits.ruleNum;
		this.ruleLength = pits.ruleLength;
		this.fitness = pits.fitness;

		this.vecNum = pits.vecNum;

		this.evaflag = pits.evaflag;
		this.testMissRate = pits.testMissRate;
		this.rank = pits.rank;
		this.crowding = pits.crowding;
		this.fitnesses = Arrays.copyOf(pits.fitnesses, pits.fitnesses.length);
		firstobj = Arrays.copyOf(pits.firstobj, pits.fitnesses.length);

		Michigan a;
		this.micRules.clear();
		for(int i=0;i<pits.micRules.size();i++){
			 a = new Michigan(pits.micRules.get(i));
			this.micRules.add(a);
		}

	}

	//MOEAD
	public void replace(Pittsburgh rules) {
		this.pitsCopy(rules);
	}

	public int mulCla(){
		int CnumNum[] = new int[Cnum];
		int claNum = 0;
		for(int i =0; i<micRules.size(); i++){
			CnumNum[micRules.get(i).getConc()]++;
		}
		for(int i = 0; i<Cnum; i++){
			if(CnumNum[i]>0){
				claNum++;
			}
		}
		return claNum;
	}

	public void setVecNum(int n){
		vecNum = n;
	}

	public int getVecNum(){
		return vecNum;
	}

	public int GetEvaflag() {
		return evaflag;
	}

	public void SetEvaflag(int a) {
		this.evaflag = a;
	}

	public void SetFitness(double fitness, int o) {
		this.fitnesses[o] = fitness;
	}

	public double GetFitness(int o) {
		return fitnesses[o];
	}

	public void setSize(int newDataSize){
		ruleNum = micRules.size();
		ruleLength = ruleLengthCalc();
		this.DataSize = newDataSize;
	}

	//NSGAII
	public void SetRank(int r) {
		rank = r;
	}

	public int GetRank() {
		return rank;
	}

	public void SetCrowding(double crow) {
		crowding = crow;
	}

	public double GetCrowding() {
		return crowding;
	}

	public void setFirstObj(double firstobj){
		this.firstobj[0] = firstobj;
		for(int i=1; i<fitnesses.length;i++){
			this.firstobj[i] = fitnesses[i];
		}
	}

	public double getFirstObj(int num){
		return firstobj[num];
	}

	double out2obje(int way){
		if(way == 4){
			return (double)(getRuleLength() / getRuleNum());
		}else if(way == 3){
			return (double)(getRuleNum() + getRuleLength());
		}else if(way == 2){
			return (double)(getRuleNum() * getRuleLength());
		}else if(way == 1){
			return (double)(getRuleLength());
		}else {
			return (double)(getRuleNum());
		}
	}

	public void EvoluationOne(DataSetInfo data, Dataset<Row> df, int objectives, int way) {

		if (getRuleNum() != 0) {
			double ans = CalcAccuracyPalKai(df);
			double acc = ans / data.getDataSize();
			SetMissRate( (1 - acc) * 100 );
			setNumAndLength();

			if (objectives == 1) {
				double fitness = Cons.W1 * getMissRate() + Cons.W2 * getRuleNum() + Cons.W3 * getRuleLength();
				SetFitness(fitness, 0);
			}
			else if (objectives == 2) {
				SetFitness( (getMissRate() ), 0 );
				SetFitness( (out2obje(way) ), 1 );
			}
			else if (objectives == 3) {
				SetFitness( getMissRate(), 0 );
				SetFitness( getRuleNum(), 1 );
				SetFitness( getRuleLength(), 2 );
			}
			else {
				System.out.println("not be difined");
			}
			if(getRuleLength() == 0){
				for (int o = 0; o < objectives; o++) {
					SetFitness(100000, o);
				}
			}
		}
		else {
			for (int o = 0; o < objectives; o++) {
				SetFitness(100000, o);
			}
		}

	}

	public int CalcAccuracyPalKai(Dataset<Row> df) {

		long collectNum = 0;

		collectNum = df.toJavaRDD()
		.map( lines -> CalcWinClassPalSpark(lines) == lines.getInt(Ndim) )
		.filter(s -> s)
		.count();

		return (int) collectNum;
	}

	public int CalcWinClassPalSpark(Row lines){

		int ans = 0;
		int kati = 0;

		int noGameSign = 0;
		double maax = 0;

		for(int r=0;r<micRules.size();r++){
			double seki = micRules.get(r).getCf() * micRules.get(r).calcAdaptationPureSpark(lines);

			if (maax < seki){
				maax = seki;
				kati = r;
				noGameSign = 0;
			}
			else if(maax == seki && micRules.get(r).getConc() != micRules.get(kati).getConc()){
				noGameSign = 1;
			}

		}
		if(noGameSign==0 && maax != 0 ){
			ans = micRules.get(kati).getConc();
		}
		else{
			ans = -1;
		}

		return ans;
	}

	public void SetTestMissRate(double m){
		testMissRate = m;
	}

	public double GetTestMissRate(){
		return testMissRate;
	}

	public void SetMissRate(double m) {
		missRate = m;
	}

	public void setNumAndLength(){
		removeRule();
		ruleNum = micRules.size();
		ruleLength = ruleLengthCalc();
	}

	public ArrayList<Michigan> getMics(){
		return micRules;
	}

}

