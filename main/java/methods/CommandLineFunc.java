package methods;

public class CommandLineFunc {

	public static void lessArgs(String[] args, int argsLen){
		if (args.length < argsLen){
			System.out.println("=======Check args=======");
			System.out.println("0 DataName: String");
			System.out.println("1 Generations: int");
			System.out.println("2 Objectives: int");
			System.out.println("3 DivideNum: int");
			System.out.println("4 EmoAlgolithm: int (0:NSGAII, 1:WS, 2:TCH, 3:PBI, 4:IPBI, 5:AOF");
			System.out.println("5 Population: int");
			System.out.println("6 CV: int");
			System.out.println("7 CVNum: int");
			System.out.println("8 Seed int");
			System.out.println("9 master param:String");
			System.out.println("10 AppName: String");
			System.out.println("11 PartitionNum	int");
			System.out.println("12 isDitributed: boolean");
			System.out.println("13 isSpark boolean");
			System.out.println("========================");

			System.exit(-1);
		}
	}

}
