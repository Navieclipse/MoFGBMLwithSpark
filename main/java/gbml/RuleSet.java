package gbml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import methods.MersenneTwisterFast;
import methods.StaticFuzzyFunc;
import methods.StaticGeneralFunc;

public class RuleSet implements java.io.Serializable{
	/******************************************************************************/
	//コンストラクタ

	RuleSet(){}

	RuleSet(MersenneTwisterFast rnd,int Ndim, int Cnum, int DataSize, int DataSizeTst, int objectibes){
		this.rnd = rnd;
		this.uniqueRnd = new MersenneTwisterFast( rnd.nextInt() );
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

	public RuleSet(RuleSet pits){

		this.rnd = pits.rnd;
		this.uniqueRnd = pits.uniqueRnd;
		this.Ndim = pits.Ndim;
		this.Cnum = pits.Cnum;
		this.DataSize = pits.DataSize;
		this.DataSizeTst = pits.DataSizeTst;

		this.missRate = pits.missRate;
		this.ruleNum = pits.ruleNum;
		this.ruleLength = pits.ruleLength;
		this.fitness = pits.fitness;

		this.vecNum = pits.vecNum;

		Rule a;
		this.micRules.clear();
		for(int i=0;i<pits.micRules.size();i++){
			 a = new Rule(pits.micRules.get(i));
			this.micRules.add(a);
		}

		this.evaflag = pits.evaflag;
		this.testMissRate = pits.testMissRate;
		this.rank = pits.rank;
		this.crowding = pits.crowding;
		fitnesses = Arrays.copyOf(pits.fitnesses, pits.fitnesses.length);
		firstobj = Arrays.copyOf(pits.firstobj, pits.fitnesses.length);

		this.dfmisspat = pits.dfmisspat;
		if(pits.missPatterns != null){
			this.missPatterns = new ArrayList<Pattern>();
			Pattern p;
			for(int i=0; i<pits.missPatterns.size(); i++){
				 p = new Pattern( pits.missPatterns.get(i) );
				this.missPatterns.add(p);
			}
		}
		this.MissPatNum = pits.MissPatNum;

	}

	public RuleSet(RuleSet pits, int vec){

		this.rnd = pits.rnd;
		this.uniqueRnd = pits.uniqueRnd;
		this.Ndim = pits.Ndim;
		this.Cnum = pits.Cnum;
		this.DataSize = pits.DataSize;
		this.DataSizeTst = pits.DataSizeTst;

		this.missRate = pits.missRate;
		this.ruleNum = pits.ruleNum;
		this.ruleLength = pits.ruleLength;
		this.fitness = pits.fitness;

		this.vecNum = vec;

		Rule a;
		this.micRules.clear();
		for(int i=0;i<pits.micRules.size();i++){
			 a = new Rule(pits.micRules.get(i));
			this.micRules.add(a);
		}

		this.evaflag = pits.evaflag;
		this.testMissRate = pits.testMissRate;
		this.rank = pits.rank;
		this.crowding = pits.crowding;
		fitnesses = Arrays.copyOf(pits.fitnesses, pits.fitnesses.length);
		firstobj = Arrays.copyOf(pits.firstobj, pits.fitnesses.length);

		this.dfmisspat = pits.dfmisspat;
		if(pits.missPatterns != null){
			this.missPatterns = new ArrayList<Pattern>();
			Pattern p;
			for(int i=0; i<pits.missPatterns.size(); i++){
				 p = new Pattern( pits.missPatterns.get(i) );
				this.missPatterns.add(p);
			}
		}
		this.MissPatNum = pits.MissPatNum;
	}

	public void pitsCopy(RuleSet pits){
		this.rnd = pits.rnd;
		this.uniqueRnd = pits.uniqueRnd;
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

		Rule a;
		this.micRules.clear();
		for(int i=0;i<pits.micRules.size();i++){
			 a = new Rule(pits.micRules.get(i));
			this.micRules.add(a);
		}

		this.dfmisspat = pits.dfmisspat;
		if(pits.missPatterns != null){
			this.missPatterns = new ArrayList<Pattern>();
			Pattern p;
			for(int i=0; i<pits.missPatterns.size(); i++){
				 p = new Pattern( pits.missPatterns.get(i) );
				this.missPatterns.add(p);
			}
		}
		this.MissPatNum = pits.MissPatNum;
	}

	/******************************************************************************/
	//ランダム
	MersenneTwisterFast rnd;
	MersenneTwisterFast uniqueRnd;

    //学習用
	int Ndim;												//次元
	int Cnum;												//クラス数
	int DataSize;											//パターン数
	int DataSizeTst;										//パターン数

	//基本値
	ArrayList<Rule> micRules = new ArrayList<Rule>();

	ArrayList<Rule> newMicRules = new ArrayList<Rule>();

	double missRate;
	double testMissRate;
	int ruleNum;
	int ruleLength;

	//ミスパターン保存用リスト
	Dataset<Row> dfmisspat;
	List<Pattern> missPatterns;
	int MissPatNum;

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
	public void initialMic(DataSetInfo trainDataInfo, Dataset<Row> trainData, ForkJoinPool forkJoinPool){
		//ヒューリスティック生成を行う場合
		boolean isHeuris = Consts.DO_HEURISTIC_GENERATION;
        Dataset<Row> dfsample;
        List<Row> samples = null;
        int sampleNums[] = null;

        if(isHeuris){ //サンプリング
        	if(trainData == null){
				sampleNums = new int[Consts.INITIATION_RULE_NUM];
				sampleNums = StaticGeneralFunc.sampringWithout(Consts.INITIATION_RULE_NUM, DataSize, uniqueRnd);
        	}
        	else{
				double sampleSize = (double)(Consts.INITIATION_RULE_NUM+10) / (double)DataSize;
				dfsample = trainData.sample( false, sampleSize, uniqueRnd.nextInt() );
				samples = dfsample.collectAsList();
        	}
        }

        do{ //while( micRules.size() == 0)

        	for(int i=0; i<Consts.INITIATION_RULE_NUM; i++){
        		micRules.add( new Rule(uniqueRnd, Ndim, Cnum, DataSize, DataSizeTst) );
        		micRules.get(i).setMic();

        		if(isHeuris){		//ヒューリスティック生成
        			if(trainData == null){
        				micRules.get(i).makeRuleSingle( trainDataInfo.getPattern(sampleNums[i]), uniqueRnd );
        				micRules.get(i).calcRuleConc(trainDataInfo, forkJoinPool);
        			}else{
						micRules.get(i).makeRuleSingle(samples.get(i), uniqueRnd);
						micRules.get(i).calcRuleConc(trainData);
        			}
        		}else{				//完全ランダム生成
					micRules.get(i).makeRuleRnd1(uniqueRnd);
					micRules.get(i).makeRuleRnd2();
        		}
        	}
        	removeRule();

        }while( micRules.size()==0 );

		setFitness(trainDataInfo, trainData, forkJoinPool);
	}

	public void setFitness(DataSetInfo trainDataInfo, Dataset<Row> trainData, ForkJoinPool forkJoinPool){

		removeRule();

		ruleNum = micRules.size();
		ruleLength = ruleLengthCalc();

		if(ruleNum==0 || ruleLength==0){
			fitness = 1000000;
			missRate = 1000000;
		}
		else{
			double ans = 0.0;
			boolean doHeuris = Consts.DO_HEURISTIC_GENERATION;
			if(doHeuris){
				if(trainData == null){
					ans = calcAndSetMissPatterns(trainDataInfo, forkJoinPool);
				}else{
					ans = calcAndSetMissPatterns(trainData);
				}
			}
			else{
				if(trainData == null){
					ans = calcMissPatterns(trainDataInfo, forkJoinPool);
				}else{
					ans = calcMissPatterns(trainData);
				}
			}
			double acc = ans / trainDataInfo.getDataSize();
			missRate = ( acc * 100.0 );
			fitness = StaticFuzzyFunc.fitness(missRate, (double)ruleNum, (double)ruleLength);
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

	public Rule getMicRule(int num){
		return micRules.get(num);
	}

	public void setMicRule(Rule micrule){

		Rule mic = new Rule(micrule);
		micRules.add(mic);

	}

	public void micMutation(int num, int i){
		micRules.get(num).mutation(i, uniqueRnd);
	}

	public void micGenRandom(){

		//交叉個体数（ルールの20％）あるいは１個
		int snum;
		if(uniqueRnd.nextDouble() < (double)Consts.RULE_OPE_RT){
			snum = (int)((ruleNum - 0.00001) * Consts.RULE_CHANGE_RT) + 1;
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
			int plus = uniqueRnd.nextInt(2);
			heuNum = (snum-1)/2 + plus;
			genNum = snum - heuNum;
		}

		for(int i=0;i<genNum;i++){
			ruleCross(i);
		}

		for(int i=genNum; i<snum; i++){
			randomGeneration(i);
		}

		//旧個体の削除，新個体の追加
		micUpdate(snum);

	}

	public void micGenHeuris(Dataset<Row> trainData, DataSetInfo trainDataInfo, ForkJoinPool forkJoinPool){

		//交叉個体数（ルールの20％）あるいは１個
		int snum;
		if(uniqueRnd.nextDouble() < (double)Consts.RULE_OPE_RT){
			snum = (int)((ruleNum - 0.00001) * Consts.RULE_CHANGE_RT) + 1;
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
			int plus = uniqueRnd.nextInt(2);
			heuNum = (snum-1)/2 + plus;
			genNum = snum - heuNum;
		}

		//ヒューリスティック生成の誤識別パターン
		double sampleSize = (double)(heuNum) / (double)MissPatNum;
		int usingDataSize = MissPatNum;
		if(MissPatNum < heuNum){
			if(trainData == null){
				missPatterns = trainDataInfo.getPattern();	//ミスパターンがない場合はパターン全体から
			}else{
				dfmisspat = trainData;  //ミスパターンがない場合はパターン全体から
			}
			sampleSize = (double)(heuNum) / (double)usingDataSize;
		}

		int increment = 0;
		Dataset<Row> dfmisspatSample = null;
		int missPatternsSampleIdx[] = new int[heuNum];
		List<Row> misspat = null;

		if(trainData == null){
			missPatternsSampleIdx = StaticGeneralFunc.sampringWithout(heuNum, missPatterns.size(), uniqueRnd);
		}else{
			do{
				dfmisspatSample = dfmisspat.sample( false, sampleSize, uniqueRnd.nextInt() );
				sampleSize = (double)( heuNum+increment++ ) / (double)usingDataSize;
			}while(dfmisspatSample.count() < heuNum);
			misspat = dfmisspatSample.collectAsList();
		}

		for(int i=0;i<genNum;i++){
			ruleCross(i);
			if(trainData == null){
				newMicRules.get(i).calcRuleConc(trainDataInfo, forkJoinPool);
			}else{
				newMicRules.get(i).calcRuleConc(trainData);
			}
		}
		int missPatIndex = 0;
		for(int i=genNum; i<snum; i++){
			if(trainData == null){
				heuristicGeneration( i, missPatterns.get(missPatternsSampleIdx[missPatIndex++]), trainDataInfo, forkJoinPool);
			}else{
				heuristicGeneration( i, misspat.get(missPatIndex++), trainData);
			}
		}

		//旧個体の削除，新個体の追加
		micUpdate(snum);

	}

	public void ruleCross(int num){

		newMicRules.add( new Rule(uniqueRnd, Ndim, Cnum, DataSize, DataSizeTst) );
		newMicRules.get(num).setMic();

		//親個体選択（バイナリトーナメントは計算量が異常にかかるので，同じ結論部の個体同士で交叉，無ければ諦める(ルール数回で）
		int mom = uniqueRnd.nextInt(ruleNum);
		int pop = uniqueRnd.nextInt(ruleNum);
		int count = 0;
		while( micRules.get(pop).getConc() != micRules.get(mom).getConc() && count < ruleNum){
			pop = uniqueRnd.nextInt(ruleNum);
			count++;
		}

		if(uniqueRnd.nextDouble() < (Consts.RULE_CROSS_RT)){
			//一様交叉
			int k=0;
			int k2=0;
			int o = 0;
			for(int i=0;i<Ndim;i++){
				k = uniqueRnd.nextInt(2);
				if(k==0){
					newMicRules.get(num).setRule(i, micRules.get(mom).getRule(i));
				}
				else{
					newMicRules.get(num).setRule(i, micRules.get(pop).getRule(i));
				}
				k2 = uniqueRnd.nextInt(Ndim);
				//突然変異
				if(k2==0){
					do{
						o = uniqueRnd.nextInt(Consts.FUZZY_SET_NUM +1);
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
				k2 = uniqueRnd.nextInt(Ndim);
				if(k2==0){
					do{
						o = uniqueRnd.nextInt(Consts.FUZZY_SET_NUM +1);
					}while(o == newMicRules.get(num).getRule(i));
					newMicRules.get(num).setRule(i, o);
				}
			}
		}

		//結論部はmomに合わす．ルール重みはランダムな割合で合計
		double cfRate = uniqueRnd.nextDouble();
		double newCf = micRules.get(mom).getCf() * cfRate + micRules.get(pop).getCf() * (1.0-cfRate);

		newMicRules.get(num).makeRuleCross( micRules.get(mom).getConc(), newCf );

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
		newMicRules.add( new Rule(uniqueRnd, Ndim, Cnum, DataSize, DataSizeTst) );
		newMicRules.get(num).setMic();
		newMicRules.get(num).makeRuleRnd1(uniqueRnd);
		if(noCla.length == 0){
			newMicRules.get(num).makeRuleRnd2();
		}else{
			newMicRules.get(num).makeRuleNoCla(noCla);
		}

	}

	//HDFSを使う
	public void heuristicGeneration(int num, Row line, Dataset<Row> df){
		newMicRules.add( new Rule(uniqueRnd, Ndim, Cnum, DataSize, DataSizeTst) );
		newMicRules.get(num).setMic();
		newMicRules.get(num).makeRuleSingle(line, uniqueRnd);
		newMicRules.get(num).calcRuleConc(df);
	}

	//HDFSを使うわない場合
	public void heuristicGeneration(int num, Pattern line, DataSetInfo trainDataInfo, ForkJoinPool forkJoinPool){
		newMicRules.add( new Rule(uniqueRnd, Ndim, Cnum, DataSize, DataSizeTst) );
		newMicRules.get(num).setMic();
		newMicRules.get(num).makeRuleSingle(line, uniqueRnd);
		newMicRules.get(num).calcRuleConc(trainDataInfo, forkJoinPool);
	}

	public void micUpdate(int snum){

		boolean doAddRules = Consts.DO_ADD_RULES;
		if(!doAddRules){ //ルールを入れ替える

			boolean isHeuris = Consts.DO_HEURISTIC_GENERATION;
			if(isHeuris){ //CF順に入れ替え
				Collections.sort( micRules, new ruleComparator() );	//CF順ソート（降順）
				int num = 0;
				for(int i=ruleNum-snum; i<ruleNum; i++){
					micRules.get(i).changeRule( newMicRules.get(num) );
					num++;
				}
			}
			else{ //ランダムに入れ替え
				int repNum[] = StaticGeneralFunc.sampringWithout2(snum, micRules.size(), uniqueRnd);
				for(int i=0; i<snum; i++){
					micRules.get(repNum[i]).micCopy(newMicRules.get(i));
				}
			}
		}
		else{ //ルールを追加する（MAXを超えた場合CF順に破棄する）
			int overRuleNum = micRules.size() + newMicRules.size() - Consts.MAX_RULE_NUM;
			if( overRuleNum > 0 ){
				Collections.sort( newMicRules, new ruleComparator() );
				for(int i=0; i<overRuleNum; i++){
					newMicRules.remove( newMicRules.size()-1 );
				}
			}
			for(int i=0; i<newMicRules.size(); i++){
				micRules.add(  new Rule( newMicRules.get(i) )  );
			}
		}

	}

	public class ruleComparator implements Comparator<Rule> {
	    public int compare(Rule a, Rule b) {
	        double no1 = a.getCf();
	        double no2 = b.getCf();

	        //降順でソート
	        if (no1 < no2) {
	            return 1;

	        } else if (no1 == no2) {
	            return 0;

	        } else {
	            return -1;

	        }
	    }
	}

	//MOEAD
	public void replace(RuleSet rules) {
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

	public void evaluationRule(DataSetInfo trainDataInfo, Dataset<Row> trainData, int objectives, int way, ForkJoinPool forkJoinPool) {

		if (getRuleNum() != 0) {
			double ans = 0;

			boolean doHeuris = Consts.DO_HEURISTIC_GENERATION;
			if(doHeuris){
				if(trainData == null){
					ans = calcAndSetMissPatterns(trainDataInfo, forkJoinPool);
				}else{
					ans = calcAndSetMissPatterns(trainData);
				}
			}
			else{
				if(trainData == null){
					ans = calcMissPatterns(trainDataInfo, forkJoinPool);
				}else{
					ans = calcMissPatterns(trainData);
				}
			}

			double acc = ans / trainDataInfo.getDataSize();
			setMissRate( acc * 100.0 );
			setNumAndLength();

			if (objectives == 1) {
				double fitness = Consts.W1 * getMissRate() + Consts.W2 * getRuleNum() + Consts.W3 * getRuleLength();
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
			setMissRate(100);
			for (int o = 0; o < objectives; o++) {
				SetFitness(100000, o);
			}
		}

	}

	//HDFS使う場合
	public int calcAndSetMissPatterns(Dataset<Row> df) {

		dfmisspat = df.filter( line -> calcWinClassPalSpark(line) != line.getInt(Ndim) );
		MissPatNum = (int)dfmisspat.count();

		return MissPatNum;
	}

	public int calcMissPatterns(Dataset<Row> df) {

		MissPatNum = (int)df.filter( line -> calcWinClassPalSpark(line) != line.getInt(Ndim) ).count();

		return MissPatNum;
	}

	//HDFS使わない場合
	public int calcAndSetMissPatterns(DataSetInfo dataSetInfo, ForkJoinPool forkJoinPool) {
		try{
			missPatterns = forkJoinPool.submit( () ->
				dataSetInfo.getPattern().stream()
				.filter( line -> calcWinClassPal(line) != line.getConClass() )
				.collect( Collectors.toList() )
				).get();

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		MissPatNum = missPatterns.size();
		return MissPatNum;
	}

	public int calcMissPatterns(DataSetInfo dataSetInfo, ForkJoinPool forkJoinPool) {

		try{
			MissPatNum = forkJoinPool.submit( () ->
				(int)dataSetInfo.getPattern().stream()
				.filter( line -> calcWinClassPal(line) != line.getConClass() )
				.count()
				).get();

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		return MissPatNum;
	}

	//HDFS使う場合
	public int calcWinClassPalSpark(Row line){

		int answerClass = 0;
		int winClassIdx = 0;

		int ruleSize = micRules.size();
		boolean canClassify = true;
		double maxMul = 0.0;
		for(int r=0; r<ruleSize; r++){

			double multiValue = micRules.get(r).getCf() * micRules.get(r).calcAdaptationPureSpark(line);

			if (maxMul < multiValue){
				maxMul = multiValue;
				winClassIdx = r;
				canClassify = true;
			}
			else if( maxMul == multiValue && micRules.get(r).getConc() != micRules.get(winClassIdx).getConc() ){
				canClassify = false;
			}

		}
		if( canClassify && maxMul != 0.0 ){
			answerClass = micRules.get(winClassIdx).getConc();
		}
		else{
			answerClass = -1;
		}

		return answerClass;
	}

	//HDFS使わない場合
	public int calcWinClassPal(Pattern line){

		int answerClass = 0;
		int winClassIdx = 0;

		int ruleSize = micRules.size();
		boolean canClassify = true;
		double maxMul = 0.0;
		for(int r=0; r<ruleSize; r++){

			double multiValue = micRules.get(r).getCf() * micRules.get(r).calcAdaptationPureSpark(line);

			if (maxMul < multiValue){
				maxMul = multiValue;
				winClassIdx = r;
				canClassify = true;
			}
			else if( maxMul == multiValue && micRules.get(r).getConc() != micRules.get(winClassIdx).getConc() ){
				canClassify = false;
			}

		}
		if( canClassify && maxMul != 0.0 ){
			answerClass = micRules.get(winClassIdx).getConc();
		}
		else{
			answerClass = -1;
		}

		return answerClass;
	}

	public void setTestMissRate(double m){
		testMissRate = m;
	}

	public double getTestMissRate(){
		return testMissRate;
	}

	public void setMissRate(double m) {
		missRate = m;
	}

	public void setNumAndLength(){
		removeRule();
		ruleNum = micRules.size();
		ruleLength = ruleLengthCalc();
	}

	public ArrayList<Rule> getMics(){
		return micRules;
	}

}

