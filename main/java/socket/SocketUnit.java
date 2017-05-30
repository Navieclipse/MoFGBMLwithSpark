package socket;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import gbml.Consts;
import gbml.RuleSet;

public class SocketUnit implements Callable<String>{

    private InetSocketAddress address = null;

    ArrayList<RuleSet> subRuleSets = new ArrayList<RuleSet>();

    ArrayList<RuleSet> newRuleSets;

    public SocketUnit(InetSocketAddress address, ArrayList<RuleSet> subRuleSets) {
        this.address = address;
        this.subRuleSets = subRuleSets;
    }

    public ArrayList<RuleSet> getRuleSets(){
    	return newRuleSets;
    }

    @SuppressWarnings("unchecked")
	@Override
    public String call() throws Exception {

        try {
            Socket socket = new Socket();
            socket.connect(this.address, Consts.WAIT_SECOND);

            ObjectOutputStream send = new ObjectOutputStream( socket.getOutputStream() );
            ObjectInputStream recieve = new ObjectInputStream( socket.getInputStream() );

            //ルールセットを送信
            send.writeObject( subRuleSets );
            System.out.println(System.currentTimeMillis() + " request to: " + this.address.getHostName() + ":" + this.address.getPort() );

            //ルールセットを受信
            newRuleSets.addAll ( (ArrayList<RuleSet>) recieve.readObject() );
            System.out.println(System.currentTimeMillis() + " response from: " + this.address.getHostName() + ":" + this.address.getPort() );

            //クローズ
            send.close();
            recieve.close();
            socket.close();
        }
        catch(Exception e){
        	System.out.println(e);
        }

        return "";
    }

}
