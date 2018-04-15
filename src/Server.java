import java.io.*;
import java.lang.reflect.Array;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Server {
    //A counter for the number of successful put operations
    static AtomicInteger successfulOps =new AtomicInteger(), putOps = new AtomicInteger(), put3Ops = new AtomicInteger(), getOps = new AtomicInteger();
    public static void main(String[] args) throws IOException{


        if (args.length != 1){
            System.out.println("Usage: java Server <port number>");
            System.exit(1);
        }

        int portNumber = Integer.parseInt(args[0]);

        String configFile = "config";
        String fileLine = null;

        List<String> configLines = new ArrayList<String>();

        try {
            FileReader fr = new FileReader(configFile);
            BufferedReader fbr = new BufferedReader(fr);
            while ((fileLine = fbr.readLine()) != null){
                configLines.add(fileLine);
            }

        }catch (FileNotFoundException e1){
            e1.printStackTrace();
        }
        String[] configLinesArr = configLines.toArray(new String[configLines.size()]);

        DHT mydht = new DHT(configLinesArr.length-1, getMyIndex(configLinesArr,portNumber) );

        ServerSocket ss = new ServerSocket(portNumber);

        Socket clientSocket = null;

        System.out.println("Node is up and listening ....");

        while(true){
            clientSocket = ss.accept();

            RunnableServer rs = new RunnableServer(clientSocket, mydht, portNumber,configLinesArr);
            Thread serverThread = new Thread(rs);
            serverThread.start();
        }
    }


    public static int getMyIndex(String[] configMap, int portNumber){

        for (int i=1; i<configMap.length; i++){
            String[] host = configMap[i].split(",");
            if (host[1].equals(Integer.toString(portNumber))) {
                    return i;
            }
        }
       return -1;
    }


    public static void readLogAndRecover(String logFileName){
        Path logPath = Paths.get("log");
        try {
            List<String> logLines = Files.readAllLines(logPath);
            for (int i=0;i<logLines.size();i++){
                String[] line = logLines.get(i).split(" ");
                if (line[0].equals("prep-rcv")){
                    for (int j=i+1;j<logLines.size();j++){
                        String[] newLine = logLines.get(j).split(" ");
                        if (newLine[0].equals("vote") && newLine[1].equals(line[1])){
                            break;
                        }else {
                            for (int k=j+1;k<logLines.size();k++){
                                String[] thirdLine = logLines.get(k).split(" ");
                                if ((thirdLine[0].equals("abort-rcv")) && thirdLine[1].equals(line[1])){
                                    for (int l=k+1; l<logLines.size();l++){
                                        String[] forthLine = logLines.get(l).split(" ");
                                        if (forthLine[0].equals("abort")&& forthLine[1].equals(line[1])){
                                            break;
                                        }else {
                                            //abort-rcv but not executed
                                        }
                                    }
                                }else {
                                    //no abort-rcv and no vote
                                }
                            }
                        }
                    }
                }else if (line[0].equals("put") || line[0].equals("put3")){

                }
            }
        }catch (IOException ioex){
            System.out.println("Cannot access log file");
        }


    }
}
