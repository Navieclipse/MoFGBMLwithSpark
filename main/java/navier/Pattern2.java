package navier;

public class Pattern2 implements java.io.Serializable {

	//コンストラクタ
	Pattern2(){}

	public Pattern2(Double[] pattern){

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
