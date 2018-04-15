import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;


public class RunnableServer implements Runnable {
    Socket serviceSocket;
    DHT mydht;
    int portNumber;
    String[] nodes;


    public RunnableServer(Socket s, DHT dht, int portNumber, String[] nodes){
        serviceSocket = s;
        mydht = dht;
        this.portNumber = portNumber;
        this.nodes = nodes;
    }

    @Override
    public void run() {
        int successOps =0 , putOps=0, getOps=0;
        boolean allIsUp = false;
        try {
            String msgFromClient;
            BufferedReader inputBuffer = new BufferedReader(new InputStreamReader(serviceSocket.getInputStream()));
            DataOutputStream out = new DataOutputStream(serviceSocket.getOutputStream());

            while ((msgFromClient = inputBuffer.readLine()) != null) {
                System.out.println(serviceSocket.getRemoteSocketAddress().toString() + " says " + msgFromClient);
                if (!allIsUp){
                    if (!checkNodesAvailability(nodes,portNumber)){
                        //out.writeBytes("Some of the nodes are not up yet, please try again later\n");
                        out.writeBoolean(false);
                        System.out.println("Some of the nodes are not up yet, please try again later");
                        continue;
                    }else {
                        allIsUp = true;
                    }
                }
//                if (!checkNodesAvailability(nodes,portNumber)){
//                    //out.writeBytes("Some of the nodes are not up yet, please try again later\n");
//                    out.writeBoolean(false);
//                    System.out.println("Some of the nodes are not up yet, please try again later");
//                    continue;
//                }
//                else {
                    long startTime = System.nanoTime();
                    String[] semantics = msgFromClient.split(" ");
                    if (semantics[0].equals("put")) {
                        mydht.log("put-rcv",semantics[1], semantics[2], serviceSocket.getInetAddress().getHostAddress(), serviceSocket.getRemoteSocketAddress());
                        if (mydht.put(Integer.parseInt(semantics[1]), semantics[2])) {
                            System.out.println("Successfully put " + semantics[1] + " with value " + semantics[2] + " in DHT");
                            //out.writeBytes("Successfully put " + semantics[1] + " with value " + semantics[2] + " in DHT\n");
                            out.writeBoolean(true);
                            //Increment the number of successful operations
                            successOps = Server.successfulOps.incrementAndGet();
                            putOps = Server.putOps.incrementAndGet();


                            //System.out.println("Number of successful put operations so far is : " + Server.successfulOps);
                        }
                    } /*else if (semantics[0].equals("put3")){
                        if (mydht.put3(Integer.parseInt(semantics[1]),semantics[2],Integer.parseInt(semantics[3]),semantics[4],Integer.parseInt(semantics[5]),semantics[6])){
                            System.out.println("Successfully put 3 key-value pairs");
                            out.writeBoolean(true);

                            successOps = Server.successfulOps.incrementAndGet();
                            putOps = Server.putOps.incrementAndGet();

                        }
                    } */else if (semantics[0].equals("pr")){
                        mydht.log("prep-rcv", semantics[1],serviceSocket.getInetAddress().getHostAddress(),serviceSocket.getPort());
                        //System.out.println("semantics length is: " + semantics.length );
                        for (int i=1;i<semantics.length;i++){
                            //System.out.println("semantics[i] is " + semantics[i]);
                            out.writeBoolean(mydht.vote(Integer.parseInt(semantics[i])));
                        }
                    } else if (semantics[0].equals("abort")){
                        mydht.log("abort-rcv",semantics[1],serviceSocket.getInetAddress().getHostAddress(), serviceSocket.getPort());
                        out.writeBoolean(mydht.abort(Integer.parseInt(semantics[1])));
                    }
                    else if (semantics[0].equals("put3")){
                        mydht.log("put3-rcv", semantics[1], semantics[2],semantics[3],semantics[4],semantics[5],semantics[6], serviceSocket.getInetAddress().getHostAddress(),serviceSocket.getPort());
                        out.writeBoolean(mydht.put3(Integer.parseInt(semantics[1]) , semantics[2], Integer.parseInt(semantics[3]) , semantics[4], Integer.parseInt(semantics[5]) , semantics[6]));
                        successOps = Server.successfulOps.incrementAndGet();
                        Server.put3Ops.incrementAndGet();
                    }
                    else if (semantics[0].equals("get")) {
                        if (mydht.get(Integer.parseInt(semantics[1])) != null){
                            out.writeBytes(mydht.get(Integer.parseInt(semantics[1])).toString()+'\n');

                            //out.writeBytes("The value at key: " + semantics[1] + " is " + mydht.get(Integer.parseInt(semantics[1])) + '\n');

                        }else {
                            out.writeBytes("f"+'\n');
                        }
                        successOps = Server.successfulOps.incrementAndGet();
                        getOps = Server.getOps.incrementAndGet();
                        System.out.println("The value at key: " + semantics[1] + " is " + mydht.get(Integer.parseInt(semantics[1])));

//                        out.writeBytes(mydht.get(Integer.parseInt(semantics[1])).toString()+'\n');
                        //out.writeBytes("The value at key: " + semantics[1] + " is " + mydht.get(Integer.parseInt(semantics[1])) + '\n');
//                        successOps = Server.successfulOps.incrementAndGet();
//                        getOps = Server.getOps.incrementAndGet();
                    }
                    else if (semantics[0].equals("end")){
                        long endTime = System.nanoTime();
                        long duration = endTime - startTime;

                        System.out.println("Total number of successful operations is : " + Server.successfulOps);
                        System.out.println("Total duration: " + duration + " nanoseconds");
                        System.out.println("Average operation time is " + duration/Server.successfulOps.get() + " nanoseconds");
                    }
                    else {
                        System.out.println("Invalid command, try again");
                    }
                //}
            }

        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

//    public static boolean checkNodesAvailability(String[] hosts, int portNumber){
//        boolean allHostsUp = true;
//        for (int i=1; i<hosts.length; i++){
//            String[] host = hosts[i].split(",");
//            if (host[1].equals(portNumber)) {
//                continue;
//            }else {
//                try (Socket S = new Socket(host[0], Integer.parseInt(host[1]))) {
//                    continue;
//                } catch (IOException ex) {
//
//                }
//                allHostsUp = false;
//            }
//        }
//        return allHostsUp;
//    }

    public static boolean checkNodesAvailability(String[] hosts, int portNumber){
        boolean allHostsUp = true;
        for (int i=1; i<hosts.length; i++){
            String[] host = hosts[i].split(",");
            //System.out.println("host[1] is " + host[1] + " and portNumber is " + portNumber);
            if (Integer.parseInt(host[1]) == portNumber) {
                continue;
            }else {
                try (Socket S = new Socket(host[0], Integer.parseInt(host[1]))){

//                    Socket S = new Socket(host[0], Integer.parseInt(host[1]));
//                    try {
//                        S.close();
//                    }catch (IOException ScloseException){
//                        System.out.println("unable to close socket");
//                    }
                    continue;
                } catch (IOException ex) {
                    ex.printStackTrace();
                    //System.out.println("Unable to connect to " + host[0] + " at port " + host[1]);
                    return false;
                }

            }
        }
        return allHostsUp;
    }


}
