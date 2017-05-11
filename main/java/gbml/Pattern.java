package gbml;

import java.util.Arrays;

public class Pattern implements java.io.Serializable {

	//コンストラクタ
	Pattern(){}

	Pattern(Pattern pat){
		this.conClass = pat.conClass;
		this.x = Arrays.copyOf(pat.x, pat.x.length);
	}

	public Pattern(Double[] pattern){

		int Ndim = pattern.length - 1;

		x = new double [Ndim];
		for (int i = 0; i < Ndim; i++) {
			x[i] = pattern[i];
		};
		conClass = pattern[Ndim].intValue();

	}

	/******************************************************************************/

	double[] x;
	int conClass;

	/******************************************************************************/

	public double getX(int i){
		return x[i];
	}

	public int getConClass(){
		return conClass;
	}

}
