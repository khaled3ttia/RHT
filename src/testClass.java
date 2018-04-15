import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;

public class testClass {

    public static void main(String[] args){
        int totalSocketNumnber = 0;

        int noOperations = 1000;
        int noPutOps = 0;
        long totalOpTime = 0;
        String configFile = "config";
        String fileLine = null;
        int noNodes = 0;
        List<Socket> sockets = new ArrayList<Socket>();
        System.out.println("Numnber of available sockets " + sockets.size());
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
        String[] range = configLinesArr[0].split("-");
        int rangeStart = Integer.parseInt(range[0]);
        int rangeEnd = Integer.parseInt(range[1]);

        Socket clientSocket = null;
        DataOutputStream outToServer = null;
        BufferedReader inFromServer = null;

        Socket endSocket = null;
//        clientSocket = new Socket(nodeHostName, nodePort);
        Random putOrget = new Random();

        long sTime = System.nanoTime();
        for (int i=0; i< 2000 ; i++){





            long opsTime = System.nanoTime();
            boolean put = putOrget.nextInt(100)<40;
            int generatedKey = 0;
            String generatedValue = null;
            StringBuilder sb = new StringBuilder();
            if (put){
                sb.append("put ");
                generatedKey = generateRandomNumber(rangeStart, rangeEnd);
                sb.append(generatedKey + " ");
                generatedValue = generateRandomValue(generateRandomNumber(4,25));
                sb.append(generatedValue);
                noPutOps++;
            }else {
                sb.append("get ");
                generatedKey = generateRandomNumber(rangeStart, rangeEnd);
                sb.append(generatedKey);
            }
            String userInput = sb.toString();
            int nodeIndex = findNode(generatedKey, noNodes);
            String[] nodeData = configLinesArr[nodeIndex+1].split(",");
            String nodeHostName = nodeData[0];
            int nodePort = Integer.parseInt(nodeData[1]);


            if (clientSocket != null){
                if (clientSocket.getPort()==nodePort && clientSocket.getInetAddress().getHostName().equals(nodeHostName)){
                    try {
                        outToServer.writeBytes(userInput+'\n');
                        outToServer.flush();

                        //System.out.println(inFromServer.readLine());
                        if (put){
                            System.out.println(inFromServer.read()==1?"put operation of value " + generatedValue + " at key " + generatedKey + " was successful": "put operation failed");
                        }
                        else {
                            System.out.println("Value at key " + generatedKey + " : " +  inFromServer.readLine());
                        }
                    }
                    catch (IOException ee){
                        ee.printStackTrace();
                        continue;
                    }

                }
                else {
                    try {
                        clientSocket.close();
                    }
                    catch (IOException ex){
                        System.out.println("Problem closing socket");
                    }

                    try {
                        clientSocket = new Socket(nodeHostName, nodePort);
                        totalSocketNumnber++;
                        outToServer = new DataOutputStream(clientSocket.getOutputStream());
                        inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        outToServer.writeBytes(userInput+'\n');
                        outToServer.flush();

                        //System.out.println(inFromServer.readLine());
                        if (put){
                            System.out.println(inFromServer.read()==1?"put operation of value " + generatedValue + " at key " + generatedKey + " was successful": "put operation failed");
                        }
                        else {
                            System.out.println("Value at key " + generatedKey + " : " +  inFromServer.readLine());
                        }

                    }
                    catch (IOException e4){

                        e4.printStackTrace();
                        continue;

                    }


                }
            } else {

                try {
                    clientSocket = new Socket(nodeHostName, nodePort);
                    totalSocketNumnber++;
                    outToServer = new DataOutputStream(clientSocket.getOutputStream());
                    inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    outToServer.writeBytes(userInput+'\n');
                    outToServer.flush();

                    //System.out.println(inFromServer.readLine());
                    if (put){
                        System.out.println(inFromServer.read()==1?"put operation of value " + generatedValue + " at key " + generatedKey + " was successful": "put operation failed");
                    }
                    else {
                        System.out.println("Value at key " + generatedKey + " : " +  inFromServer.readLine());
                    }

                }
                catch (IOException e4){

                    e4.printStackTrace();
                    continue;

                }

            }



            long opeTime = System.nanoTime();
            long opDuration = opeTime - opsTime;
            totalOpTime += opDuration;

        }

        try{
            clientSocket.close();
        }
        catch (Exception xxx){
            System.out.println("Client Socket already closed, no need to close");
        }
        for (int i=0; i< noNodes ; i++){
            String[] nodeData = configLinesArr[i+1].split(",");
            String nodeHostName = nodeData[0];
            int nodePort = Integer.parseInt(nodeData[1]);
            try {
                endSocket = new Socket(nodeHostName, nodePort);
                DataOutputStream EndoutToServer = new DataOutputStream(endSocket.getOutputStream());
                BufferedReader EndinFromServer = new BufferedReader(new InputStreamReader(endSocket.getInputStream()));
                EndoutToServer.writeBytes("end"+'\n');
                EndoutToServer.flush();
            }
            catch (IOException e4){

                e4.printStackTrace();
                continue;

            }
            try {
                endSocket.close();
            }
            catch (IOException ex){
                System.out.println("Problem closing end socket");
            }
        }

        System.out.println("Total number of opened sockets is : " + totalSocketNumnber);

        /*
        long eTime = System.nanoTime();
        long duration = eTime - sTime;
        System.out.println(noOperations + " committed in " + duration + " nanoseconds");
        System.out.println("Number of put operations is: " + noPutOps);
        System.out.println("Average operation time is: + " + totalOpTime/noOperations);
        */
    }

    public static int findNode(int key, int noNodes){

        return key%noNodes;
    }

    public static int generateRandomNumber(int min, int max){
        Random rndKey = new Random();
        return min + rndKey.nextInt(max) + 1;
    }

    public static String generateRandomValue(int length){
        String allChars = "aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQRrSsTtUuVvWwXxYyZ";
        StringBuilder randomString = new StringBuilder();
        Random rnd = new Random();
        while (randomString.length() < length){
            int i = (int)(rnd.nextFloat() * allChars.length());
            randomString.append(allChars.charAt(i));
        }
        return randomString.toString();
    }
}
