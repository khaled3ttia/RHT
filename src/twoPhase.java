import java.io.*;
import java.lang.reflect.Array;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class twoPhase {

    private static List<Socket> sockets = new ArrayList<>();

    public static Boolean prepare(int[] keys, String[] configLinesArr){
        Socket prepSocket = null;
        DataOutputStream ds = null;
        BufferedReader br = null;
        String[] keyLocations = new String[2];
        AtomicInteger noAcks = new AtomicInteger();


        for (int key:
             keys) {
                keyLocations = findLocations(key, configLinesArr);
            for (String location:
                    keyLocations) {
                System.out.println("key: " + key + "location: " + location);
                String[] locationData = location.split(",");
                String hostName = locationData[0];
                String inet = null;
                int portNo = Integer.parseInt(locationData[1]);
                try {
                    boolean reUse = false;
                     for (int i=0; i<sockets.size();i++){
                         if (sockets.get(i).getPort() == portNo){
                             reUse = true;
                             System.out.println("I AM IN A REUSE BLOCK");
                             ds = new DataOutputStream(sockets.get(i).getOutputStream());
                             br = new BufferedReader(new InputStreamReader(sockets.get(i).getInputStream()));
                             log("prepare", key ,sockets.get(i).getInetAddress().getHostAddress(), sockets.get(i).getPort());
                             inet = sockets.get(i).getInetAddress().getHostAddress();

                         }
                     }

                     if (!reUse) {
                         System.out.println("THIS SOCKET HAS NEVER BEEN USED BEFORE");
                         sockets.add(new Socket(hostName, portNo));
                         ds = new DataOutputStream(sockets.get(sockets.size()-1).getOutputStream());
                         br = new BufferedReader(new InputStreamReader(sockets.get(sockets.size()-1).getInputStream()));
                         log("prepare", key ,sockets.get(sockets.size()-1).getInetAddress().getHostAddress(), sockets.get(sockets.size()-1).getPort());
                         inet = sockets.get(sockets.size()-1).getInetAddress().getHostAddress();

                     }

                    ds.writeBytes("pr " + key + '\n');
                    ds.flush();

                    int voteReply = br.read();
                    log("vote"+voteReply, key, inet, portNo);
                    Boolean ack = (voteReply==1);
                    System.out.println("Ack is :" + ack);
                    if (ack) { noAcks.incrementAndGet(); }

                }catch (IOException connException){
                    connException.printStackTrace();
                    continue;
                }
//                try {
//                    prepSocket.close();
//                }
//                catch (IOException ex){
//                    System.out.println("Problem closing end socket");
//                }
            }
        }
        if (noAcks.get() !=  keys.length*keyLocations.length){
            return false;
        }
        return true;
    }

    public static Boolean commit(String command, String[] configLinesArr){
        AtomicInteger noSuccessfullOperations = new AtomicInteger();
        Socket clientSocket;
        DataOutputStream ds = null;
        BufferedReader br = null;
        String inet=null;

        //String[] keyLocations;
        List<String> unionOfLocations = new ArrayList<>();
        boolean success = true;
        int[] keys = extractKeys(command);


        //find the location of each key in the command, in case of replicated keys add target only once
        for (int i=0; i< keys.length; i++){
            String[] keyLocations = findLocations(keys[i], configLinesArr);
            for (String location: keyLocations) {
                if (! unionOfLocations.contains(location)){
                    unionOfLocations.add(location);
                }
            }
        }

        String[] sendTargets = unionOfLocations.toArray(new String[unionOfLocations.size()]);
        //System.out.println("Unionoflocations is : " + unionOfLocations);


        for (int i=0; i< sendTargets.length; i++){
            String[] targetData = sendTargets[i].split(",");

            String hostName = targetData[0];
            int portNo = Integer.parseInt(targetData[1]);
            try {
//                clientSocket = new Socket(hostName, portNo);
//                DataOutputStream ds = new DataOutputStream(clientSocket.getOutputStream());
//                BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//                ds.writeBytes(command + '\n');
//                ds.flush();

                boolean reUse = false;
                for (int j=0; j<sockets.size();j++){
                    if (sockets.get(j).getPort() == portNo){
                        reUse = true;
                        System.out.println("commit: I AM IN A REUSE BLOCK");
                        ds = new DataOutputStream(sockets.get(j).getOutputStream());
                        br = new BufferedReader(new InputStreamReader(sockets.get(j).getInputStream()));
                        inet = sockets.get(i).getInetAddress().getHostAddress();

                    }
                }

                if (!reUse) {
                    System.out.println("commit: THIS SOCKET HAS NEVER BEEN USED BEFORE");
                    sockets.add(new Socket(hostName, portNo));
                    ds = new DataOutputStream(sockets.get(sockets.size()-1).getOutputStream());
                    br = new BufferedReader(new InputStreamReader(sockets.get(sockets.size()-1).getInputStream()));
                    inet = sockets.get(sockets.size()-1).getInetAddress().getHostAddress();

                }

                    ds.writeBytes(command + '\n');

                    ds.flush();

                if (!command.startsWith("get")){
                    log(command,inet,portNo);

                    if (br.read() != 1) {
                        //operation failed
                        success = false;

//                        try{
//                            clientSocket.close();
//                        }catch (IOException ex){
//                            System.out.println("Problem closing clientSocket");
//                        }
                        break;
                    }

                }else {
                    System.out.println(br.readLine());
                }

            } catch (IOException ex){
                ex.printStackTrace();
                continue;
            }

//            try{
//                clientSocket.close();
//            }catch (IOException ex){
//                System.out.println("Problem closing clientSocket");
//            }
        }
        System.out.println("Operation " + ((success)?"succeeded!":"failed :("));
        return success;
        /*
        for (int i=0; i<keys.length;i++){
            keyLocations = findLocations(keys[i], configLinesArr);
            System.out.println("locations for key " + keys[i] + " are " + Arrays.toString(keyLocations) + " and number of locations is " + keyLocations.length);
            for (String location:
                 keyLocations) {
                String[] locationData = location.split(",");
                String hostName = locationData[0];
                int portNo = Integer.parseInt(locationData[1]);
                try {
                    clientSocket = new Socket(hostName,portNo);
                    DataOutputStream ds = new DataOutputStream(clientSocket.getOutputStream());
                    BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    ds.writeBytes(command +'\n');
                    ds.flush();

                    if (br.read() != 1){
                        //abort
                        success = false;
                        System.out.print("Operation " + command + " failed!");

                        try{
                            clientSocket.close();
                        }catch (IOException ex){
                            System.out.println("Problem closing clientSocket");
                        }

                        return success;
                    }

                    //noSuccessfullOperations.getAndAdd(br.read());

                    //int noSuccessfulLocalOperations = br.read();
                    //noSuccessfullOperations.getAndAdd(noSuccessfulLocalOperations);
                }catch (IOException commitError){
                    commitError.printStackTrace();
                    continue;
                }
                try {
                    clientSocket.close();
                }
                catch (IOException ex){
                    System.out.println("Problem closing clientSocket");
                }
            }
        }
        */


//        System.out.println("Operation " + command  + " succeeded");
//        return success;
//        if (noSuccessfullOperations.get() == keys.length * 2){
//            return true;
//        } else {
//            return false;
//        }
    }

    public static boolean abort(String command, String[] configMap ){
        int[] keys = extractKeys(command);
        boolean localsuccess, success=true;
        String inet= null;
        Socket clientSocket;
        DataOutputStream ds = null;
        BufferedReader br = null;
        for (int key: keys){
            String[] locations = findLocations(key, configMap);
            for (String loc: locations){
                String[] locData = loc.split(",");
                String hostName = locData[0];
                int portNo = Integer.parseInt(locData[1]);
                try {
//                    clientSocket = new Socket(hostName, portNo);
//                    DataOutputStream ds = new DataOutputStream(clientSocket.getOutputStream());
//                    BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//                    ds.writeBytes("abort " + key + '\n');
//                    ds.flush();

                    boolean reUse = false;
                    for (int i=0; i<sockets.size();i++){
                        if (sockets.get(i).getPort() == portNo){
                            reUse = true;
                            System.out.println("I AM IN A REUSE BLOCK");
                            ds = new DataOutputStream(sockets.get(i).getOutputStream());
                            br = new BufferedReader(new InputStreamReader(sockets.get(i).getInputStream()));
                            inet = sockets.get(i).getInetAddress().getHostAddress();
                        }
                    }

                    if (!reUse) {
                        System.out.println("THIS SOCKET HAS NEVER BEEN USED BEFORE");
                        sockets.add(new Socket(hostName, portNo));
                        ds = new DataOutputStream(sockets.get(sockets.size()-1).getOutputStream());
                        br = new BufferedReader(new InputStreamReader(sockets.get(sockets.size()-1).getInputStream()));
                        inet = sockets.get(sockets.size()-1).getInetAddress().getHostAddress();
                    }


                    ds.writeBytes("abort " + key + '\n');
                    log("abort",key,inet,portNo);
                    ds.flush();


                    localsuccess = br.read()==1;
                    if (localsuccess) { log("abort-rcv", key, inet, portNo); } else { log("abort-fail",key,inet,portNo); }
                    success = success && localsuccess;
                    System.out.println("lock on key " + key + " was" + (localsuccess?" ":" not ") + "released from location " + loc);

//                    try{
//                        clientSocket.close();
//                    }catch (IOException ex){
//                        System.out.println("Problem closing clientSocket");
//                    }

                }catch (IOException ex){
                    ex.printStackTrace();
                    continue;
                }
            }
        }
        return success;
    }

    public static String[] findLocations(int key, String[] configMap){


        int noNodes = configMap.length - 1;

        int firstIndex = key%noNodes;

        int secondIndex = 0;
        if (firstIndex+1 != noNodes ){
            secondIndex = firstIndex+1;
        }

        String n1Host = configMap[firstIndex+1];
        String n2Host = configMap[secondIndex+1];
        return new String[] {n1Host, n2Host};
    }

    public static String[] loadConfig(String configFile){
        List<String> configLines = new ArrayList<String>();
        String fileLine = null;
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

        return configLines.toArray(new String[configLines.size()]);
    }

    public static int[] extractKeys(String command){
        String[] parsedCommand = command.split(" ");
        if (parsedCommand[0].equals("put") || parsedCommand[0].equals("get")){
            return new int[]{Integer.parseInt(parsedCommand[1])};
        }else if (parsedCommand[0].equals("put3")){
            return new int[]{Integer.parseInt(parsedCommand[1]), Integer.parseInt(parsedCommand[3]), Integer.parseInt(parsedCommand[5])};
        }else {
            return new int[]{-1};
        }
    }

    public static String[] extractValues(String command){
        String[] parsedCommand = command.split(" ");
        if (parsedCommand[0].equals("put")){
            return new String[]{parsedCommand[2]};
        }else if (parsedCommand[0].equals("put3")){
            return new String[]{parsedCommand[2], parsedCommand[4], parsedCommand[6]};
        }else {
            return new String[]{null};
        }
    }

    public static void endCommandBatch(String[] configLinesArr){
        Socket endSocket;
        int noNodes = configLinesArr.length - 1;
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
    }

    public static void log(String event, Object... params){
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("coordinatorlog", true));
            bw.append(event);
            for (Object param: params){
                bw.append(" " + param);
            }
            bw.append('\n');
            bw.close();
        }
        catch (IOException ex){
            ex.printStackTrace();
            return;
        }


    }
 }
