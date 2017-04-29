package gbml;

import java.lang.reflect.Field;

public class Consts {

	//GBML's parameter
    public static final int ANTECEDENT_LEN  = 5;	//ドントケアにしない条件部の数
    public static final double DONT_CAlE_RT = 0.8;	//どんとケア適応確率（合わせ用）
    public static final boolean IS_PROBABILITY_DONT_CARE = false;	//ドントケアを確率で行う
    public static final boolean DO_HEURISTIC_GENERATION_INIT = true;	//ヒューリスティック生成法を行う(初期生成）
    public static final boolean DO_HEURISTIC_GENERATION_IN_GA = true;	//ヒューリスティック生成法（途中での世代）

    public static final int FUZZY_SET_NUM  = 14;	//ファジィ集合数
    public static final int MAX_FUZZY_DIVIDE_NUM = 5;	//条件部の分割数の最大値
    public static final int INITIATION_RULE_NUM = 30;	//初期ルール数
    public static final int MAX_RULE_NUM  = 60;	//最大ルール数
    public static final int MIN_RULE_NUM = 1;	//最小ルール数

    public static final double RULESET_CROSS_RT = 0.9;	//ピッツバーグ交叉確率
    public static final double RULE_CROSS_RT = 0.9;	//ミシガン交叉確率
    public static final double RULE_OPE_RT = 0.5;	//ミシガン適用確率
    public static final double RULE_CHANGE_RT = 0.2;	//ルール入れ替え割合

	public static final boolean DO_LOG_PER_LOG = false;	//ログでログを出力．

    //MOEAD's parameter
    public static final int VECTOR_DIVIDE_NUM  = 59;	//分割数 //2:209,3:19
    public static final double MOEAD_ALPHA = 0.9;	//参照点のやつ
    public static final double MOEAD_THETA = 5.0;	//シータ
    public static final boolean IS_NEIGHBOR_SIZE = false;	//近傍サイズ 0:個数指定, 1:パーセント指定
    public static final int NEIGHBOR_SIZE_RT = 10;	//近傍サイズ％
    public static final int SELECTION_NEIGHBOR_NUM  = 21;	//選択近傍サイズ
    public static final int UPDATE_NEIGHBOR_NUM  = 1;	//更新近傍サイズ

    public static final int WS  = 1;	//weighted sum
    public static final int TCHEBY = 2;	//Tchebycheff
    public static final int PBI = 3;	//PBI
    public static final int IPBI = 4;	//InvertedPBI
    public static final int SSF = 5;	//special scalarizing function.

    public static final int SECOND_OBJECTIVE_TYPE = 0;	//2目的め 0:rule, 1:length, 2:rule * length, 4:length/rule
    public static final boolean DO_NORMALIZE = false;	//正規化するかどうか
    public static final boolean IS_BIAS_VECTOR = false;	//false: NObiasVector, 1: biasVector

    public static final double IS_FIRST_IDEAL_DOWN = 0.0;	//１目的目のみ下に動かす．（やらない場合は０に）
    public static final boolean IS_WS_FROM_NADIA = false;	//WSをナディアポイントから

    //NSGA2's parameter
    public static final int NSGA2 = 0;	//NSGA2の番号
    public static final int OBJECTIVE_DEGREES = 0;	//目的関数の回転度
    public static final boolean DO_CD_NORMALIZE = false;	//Crowding distanceを正規化する
    public static final boolean HAS_PARENT = false;

    //Others
    public static final int PER_SHOW_GENERATION_NUM  = 100;	//表示する世代

    //One objective weights
    public static final int W1 = 100;
    public static final int W2  = 1;
    public static final int W3 = 1;

    public static final int TRAIN = 0;	//学習用
    public static final int TEST = 1;	//評価用
    //OS
    public static final int WINDOWS = 0;	//windows
    public static final int UNIX = 1;	//unix
    public static final int HDFS= 2;	//HDFS


    public String getStaticValues(){
    	StringBuilder sb = new StringBuilder();
        sb.append("Class: " + this.getClass().getCanonicalName() + "\n");
        sb.append("Settings:\n");
        for (Field field : this.getClass().getDeclaredFields()) {
            try {
                field.setAccessible(true);
                sb.append(field.getName() + " = " + field.get(this) + "\n");
            } catch (IllegalAccessException e) {
                sb.append(field.getName() + " = " + "access denied\n");
            }
        }

    	return sb.toString();
    }

}
