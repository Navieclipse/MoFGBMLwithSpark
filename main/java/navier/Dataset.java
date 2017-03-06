package navier;

import java.util.ArrayList;


public class Dataset implements java.io.Serializable{

	//コンストラクタ
	Dataset(){}

	public Dataset(String[] params){

		this.DataSize = Integer.parseInt(params[0]);
		this.Ndim = Integer.parseInt(params[1]);
		this.Cnum = Integer.parseInt(params[2]);

	}

	public Dataset(int Ndim, int Cnum, int DataSize, ArrayList<Pattern2> patterns){

		this.Ndim = Ndim;
		this.Cnum = Cnum;
		this.DataSize = DataSize;

		this.patterns = patterns;

	}

	/******************************************************************************/

	int Ndim;
	int Cnum;
	int DataSize;

	ArrayList<Pattern2> patterns = new ArrayList<Pattern2>();

	/******************************************************************************/
	//メソッド

	public void setPattern(ArrayList<Pattern2> patterns){
		this.patterns = patterns;
	}

	public void addPattern(Double[] pattern){
		patterns.add(new Pattern2(pattern));
	}

	public void setNdim(int num){
		Ndim = num;
	}

	public void setCnum(int num){
		Cnum = num;
	}

	public void setDataSize(int num){
		DataSize = num;
	}

	public ArrayList<Pattern2> getPattern(){
		return patterns;
	}

	public int getNdim(){
		return Ndim;
	}

	public int getCnum(){
		return Cnum;
	}

	public int getDataSize(){
		return DataSize;
	}

}
