import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class simpleManualTest {

    public static void main(String[] args) {
        String[] configLinesArr = twoPhase.loadConfig("config");

        String userInput="";
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Hey, client. Write your message ");
        try {
            userInput = inFromUser.readLine();
        }
        catch (IOException e3){
            e3.printStackTrace();
        }

        while (userInput != null) {

            String[] userInputSemantics = userInput.split(" ");

            if (userInputSemantics[0].equals("put")){
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

            } else if (userInputSemantics[0].equals("put3")){
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

            try {
                userInput = inFromUser.readLine();
            }
            catch (IOException e5){
                e5.printStackTrace();
            }

        }

        }


}