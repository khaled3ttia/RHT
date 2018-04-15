import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class Client {
    public static void main(String[] agrs) {

        String configFile = "config";
        String fileLine = null;
        int noNodes = 0;
        List<String> configLines = new ArrayList<String>();
        try {
            FileReader fr = new FileReader(configFile);
            BufferedReader fbr = new BufferedReader(fr);
            while ((fileLine = fbr.readLine()) != null){
                configLines.add(fileLine);
            }
        }catch (FileNotFoundException e1){
            e1.printStackTrace();
        }catch (IOException e2){
            e2.printStackTrace();
        }


    String[] configLinesArr = configLines.toArray(new String[configLines.size()]);
    noNodes = configLinesArr.length - 1;

    String userInput="";
    BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Hey, client. Write your message ");
    try {
        userInput = inFromUser.readLine();
    }
    catch (IOException e3){
        e3.printStackTrace();
    }

    while (userInput != null){
        String[] userInputSemantics = userInput.split(" ");
        int key = Integer.parseInt(userInputSemantics[1]);
        int nodeIndex = findNode(key,noNodes);
        String[] nodeData = configLinesArr[nodeIndex+1].split(",");
        String nodeHostName = nodeData[0];
        int nodePort = Integer.parseInt(nodeData[1]);
        connectionTrial: try {
            Socket clientSocket = new Socket(nodeHostName, nodePort);
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            outToServer.writeBytes(userInput+'\n');
            outToServer.flush();
            System.out.println(inFromServer.readLine());
        }
        catch (IOException e4){
            System.out.println("Not all nodes are up yet, let's try again later");

        }
        try {
            userInput = inFromUser.readLine();
        }
        catch (IOException e5){
            e5.printStackTrace();
        }

    }
    }

    public static int findNode(int key, int noNodes){

        return key%noNodes;
    }
}
