package socket;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GridTest implements Callable<String>{

	public static void main(String[] args) throws IOException {

        //仕事をさせる対象のGrid(サーバ＋ポート)のリスト							Mainの最初
        InetSocketAddress[] gridList = new InetSocketAddress[]{
            new InetSocketAddress("cano", 50000),
            new InetSocketAddress("collet", 50000),
            new InetSocketAddress("tsutsui", 50000),
            new InetSocketAddress("maitre", 50000),
        };

        ExecutorService service = Executors.newCachedThreadPool();
        GridTest gridTest;

        //すべてのGridに10回ずつ仕事を投げる 
        for (int i = 0; i < 1; i++) {
            for (InetSocketAddress gridList1 : gridList) {
                gridTest = new GridTest(gridList1);
                service.submit(gridTest);
            }
        }
    }

    private InetSocketAddress address = null;

    public GridTest(InetSocketAddress address) {
        this.address = address;
    }

    //GridへのSocketをオープンし、request(＝仕事)を投げ、response(＝結果)を受け取る
    @Override
    public String call() throws Exception {
        Socket socket = null;
        BufferedWriter writer = null;
        BufferedReader reader = null;
        String request = "Hello, who are you?";

        try {
            socket = new Socket();
            socket.connect(this.address, 300000);
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));

            writer.write(request + "\n");
            writer.flush();
            System.out.println(System.currentTimeMillis() + " " + "request to: " + this.address.getHostName() + ":" + this.address.getPort() + "[" + request + "]");
            String response = reader.readLine();
            System.out.println(System.currentTimeMillis() + " " + "response from: " + this.address.getHostName() + ":" + this.address.getPort() + " [" + response + "]");
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException e) {
                }
            }
            if (writer != null) {
                try {
                    writer.close();
                }
                catch (IOException e) {
                }
            }
            if (socket != null) {
                try {
                    socket.close();
                }
                catch (IOException e) {
                }
            }
        }

        //戻り値で呼び出し元の挙動に変化を及ぼすこともできる(今回は使っていない)
        return "ok";
    }

}
