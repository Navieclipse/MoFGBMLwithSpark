package socket;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;

import gbml.DataSetInfo;
import gbml.RuleSet;

public class ServerUnit {

    public static void main(String[] args) throws IOException {
    	//名前とポート番号と最大スレッド数
        ServerUnit grid = new ServerUnit(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        grid.start();
    }

    private String name = null;
    private int port = -1;
    private int maxThreadNum = 1;

    public ServerUnit(String name, int port, int maxThreadNum) {
        this.name = name;
        this.port = port;
        this.maxThreadNum = maxThreadNum;
    }

    public void start() throws IOException {

        System.out.println(System.currentTimeMillis() + " " + this.name + ": grid started");

        //データの読み込み(できればHDFSから）
        DataSetInfo trainDataInfo = new DataSetInfo();

        //フォークジョイン準備
        ForkJoinPool forkJoinPool = new ForkJoinPool(maxThreadNum);

        Socket socket;

        //無限ループ！
        while (true) {
			try {
				ServerSocket server = new ServerSocket(this.port);

				//受付まで待機
				socket = server.accept();

				//個体評価開始（並列）
				evaluationProcess(socket, trainDataInfo, forkJoinPool);

				server.close();
			}
			catch(Exception e){
				System.out.println(e);
			}
        }

    }

    @SuppressWarnings("unchecked")
	public void evaluationProcess(Socket socket, DataSetInfo trainData, ForkJoinPool forkJoinPool){

    	 System.out.println(System.currentTimeMillis() + " " + this.name + ": grid process started");

         try {
             ObjectOutputStream send = new ObjectOutputStream( socket.getOutputStream() );
             ObjectInputStream recieve = new ObjectInputStream( socket.getInputStream() );

             //ルールセットを受信
             ArrayList<RuleSet> subRuleSets = ( (ArrayList<RuleSet>) recieve.readObject() );

             System.out.println(System.currentTimeMillis() + "Evaluation Start" );
             //評価する
             subRuleSets.stream().forEach( rule -> rule.evaluationRule(trainData, forkJoinPool) );

             System.out.println(System.currentTimeMillis() + "Evaluation End" );

             //ルールセットを送信
             send.writeObject( subRuleSets );

             //クローズ
             send.close();
             recieve.close();
             socket.close();

         }
         catch(Exception e){
        	 System.out.println(e);
         }

    }

}