import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class testBench {

    public static void main (String[] args){

        int noOperations = Integer.parseInt(args[0]);

        String[] configLinesArr = twoPhase.loadConfig("config");

        int noNodes = configLinesArr.length - 1;
        String[] range = configLinesArr[0].split("-");
        int rangeStart = Integer.parseInt(range[0]);
        int rangeEnd = Integer.parseInt(range[1]);

        Socket clientSocket = null;
        DataOutputStream outToServer = null;
        BufferedReader inFromServer = null;

        Socket endSocket = null;
        Random putOrget = new Random();


//        for (int i=0;i<8;i++){
//
//
//            RunnableBench rbench = new RunnableBench(configLinesArr, rangeStart, rangeEnd, noOperations);
//            Thread serverThread = new Thread(rbench);
//            serverThread.start();
//        }

        for (int i=0; i<=noOperations; i++){
            int rndInt = putOrget.nextInt(100);
            boolean put = rndInt <= 20;
            boolean put3 = (rndInt > 20) && (rndInt <41);

            int generatedKey = 0;
            String generatedValue = null;
            StringBuilder sb = new StringBuilder();
            if (put){
                sb.append("put ");
                generatedKey = generateRandomNumber(rangeStart, rangeEnd);
                sb.append(generatedKey + " ");
                generatedValue = generateRandomValue(generateRandomNumber(4,25));
                sb.append(generatedValue);
            }else if (put3){
                List<Integer> nums = new ArrayList<>();

                sb.append("put3 ");
                int k1 = generateRandomNumber(rangeStart,rangeEnd);
                nums.add(k1);
                sb.append(k1 + " ");
                String v1 = generateRandomValue(generateRandomNumber(4,25));
                sb.append(v1+" ");

                int k2 = generateRandomNumber(rangeStart,rangeEnd);
                while (nums.contains(k2)){
                    k2 = generateRandomNumber(rangeStart, rangeEnd);
                }
                nums.add(k2);
                sb.append(k2 + " ");
                String v2 = generateRandomValue(generateRandomNumber(4,25));
                sb.append(v2+" ");

                int k3 = generateRandomNumber(rangeStart,rangeEnd);
                while (nums.contains(k3)){
                    k3 = generateRandomNumber(rangeStart, rangeEnd);
                }
                sb.append(k3 + " ");
                String v3 = generateRandomValue(generateRandomNumber(4,25));
                sb.append(v3);
            } else {
                sb.append("get ");
                generatedKey = generateRandomNumber(rangeStart, rangeEnd);
                sb.append(generatedKey);
            }
            String userInput = sb.toString();
            //System.out.println(userInput);


            String[] userInputSemantics = userInput.split(" ");

            if (userInputSemantics[0].equals("put") || userInputSemantics[0].equals("put3")){
                Boolean prepareSuccess =  twoPhase.prepare(twoPhase.extractKeys(userInput), configLinesArr);
                System.out.println(prepareSuccess?"Prepare phase succeeded":"Prepare failed!");
                if (prepareSuccess){
                    //move ahead to the commit phase
                    //since we know that the prepare phase is correct, we can directly put and remove the locks
                    twoPhase.commit(userInput,configLinesArr);

                }else {
                    //here we need to abort and release existing locks
                    twoPhase.abort(userInput, configLinesArr);
                }

            } else if (userInputSemantics[0].equals("get")){
                twoPhase.commit(userInput, configLinesArr);
            }


        }
        twoPhase.endCommandBatch(configLinesArr);

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

    public static void readLogAndRecover(String logFileName){
        Path logPath = Paths.get("cordinatorlog");
        try {
             List<String> logLines = Files.readAllLines(logPath);
             for (int i=0;i<logLines.size();i++){

             }
        }catch (IOException ioex){
            System.out.println("Cannot access log file");
        }


    }
}
