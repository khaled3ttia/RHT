import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.security.Key;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DHT {

private int noNodes;
private int myIndex;
private Hashtable<Integer,Object> dht;
private Hashtable<Integer,AtomicBoolean> inUse;

public DHT(int noNodes, int myIndex){
    dht = new Hashtable<>();
    inUse = new Hashtable<>();
    this.noNodes = noNodes;
    this.myIndex = myIndex;
}


public Boolean vote(int k){
    log("vote",k);
    //try to acquire lock on the specified key
    //if able to acquire lock, lock the entry and return true
    //otherwise return false
    AtomicBoolean locked = inUse.get(k);

    if (locked != null){
        while (locked.compareAndSet(false,true)){
            System.out.println("lock on key " + k + " acquired successfully (it was not null initially)" );
            return true;
        }
    }else {
        inUse.put(k, new AtomicBoolean(true ));
        System.out.println("lock on key " + k + " acquired successfully (it was null)" );
        return true;
    }
    System.out.println("Unable to acquire lock on key " + k);
    return false;
}

public boolean abort(int k){
    log("abort",k);
    AtomicBoolean locked = inUse.get(k);

    if (locked != null){
        while (locked.compareAndSet(true , false)){
            System.out.println("lock on key " + k + " was released");
            return true;
        }
    }
    return false;

}

//public boolean put(int k, Object v) {
//
//    AtomicBoolean locked = inUse.get(k);
//    if (locked != null){
//        while (locked.compareAndSet(false,true)){
//
//            dht.put(k,v);
//
//            locked.compareAndSet(true,false);
//
//            return true;
//        }
//    }else {
//        inUse.put(k,new AtomicBoolean(true));
//        dht.put(k,v);
//        inUse.get(k).compareAndSet(true,false);
//        return true;
//    }
//    return false;
//
//}

public boolean put(int k, Object v){
    log("put", k,v);
    AtomicBoolean locked = inUse.get(k);
    System.out.println("current value of locked of key " + k + " is: " + locked.get());
    try {
        dht.put(k,v);

        //release the lock and return true if successful
        return locked.compareAndSet(true,false);
    }
    catch (NullPointerException nullEx){
        nullEx.printStackTrace();
        System.out.println("key or value were null, returning false");
        return false;
    }
}

public boolean put3(int k1, Object v1, int k2, Object v2, int k3, Object v3){

    //int success=0;
    int[] keys = {k1,k2,k3};
    Object[] values = {v1, v2, v3};

    for (int i=0;i<keys.length;i++) {
        if (keyIsHere(keys[i])){
            log("put",keys[i],values[i]);
            System.out.println("Trying to put " + keys[i] + ":" + values[i]);
            if (!put(keys[i], values[i])){
                return false;
            }
            System.out.println("Successfully put " + keys[i] + ":" + values[i]);
        }
    }
    return true;
}

public boolean keyIsHere(int key){

    int firstIndex = key%noNodes;

    int secondIndex = 0;
    if (firstIndex+1 != noNodes ){
        secondIndex = firstIndex+1;
    }

    return (myIndex == firstIndex+1) || (myIndex == secondIndex+1) ? true : false;

}

//public boolean put3(int k1, Object v1, int k2, Object v2, int k3, Object v3){
//
//    AtomicBoolean locked1 = inUse.get(k1);
//    AtomicBoolean locked2 = inUse.get(k2);
//    AtomicBoolean locked3 = inUse.get(k3);
//
//    Boolean nullL1, nullL2, nullL3, noneIsLocked;
//    nullL1 = !(locked1 != null);
//    nullL2 = !(locked2 != null);
//    nullL3 = !(locked3 != null);
//
//    if (!nullL1 && !nullL2 && !nullL3){
//        while (locked1.compareAndSet(false,true) && locked2.compareAndSet(false,true) && locked3.compareAndSet(false,true)){
//            dht.put(k1,v1);
//            dht.put(k2,v2);
//            dht.put(k3,v3);
//
//            locked1.compareAndSet(true,false);
//            locked2.compareAndSet(true,false);
//            locked3.compareAndSet(true,false);
//
//            return true;
//        }
//    } else {
//
//
//        noneIsLocked = ((nullL1 && !locked2.get() && !locked3.get())
//                        || (nullL2 && !locked1.get() && !locked3.get())
//                        || (nullL3 && !locked1.get() && !locked2.get()));
//
//        if (noneIsLocked){
//            if (nullL1){
//                inUse.put(k1,new AtomicBoolean(true));
//                inUse.put(k2,new AtomicBoolean(true));
//                inUse.put(k3, new AtomicBoolean(true));
//                dht.put(k1,v1);
//                dht.put(k2,v2);
//                dht.put(k3,v3);
//                locked1.compareAndSet(true,false);
//                locked2.compareAndSet(true,false);
//                locked3.compareAndSet(true,false);
//                return true;
//            }
//        }
//    }
//    return false;
//}

public Object get(int k){
    Object value = false;

//
    AtomicBoolean locked = inUse.get(k);
    if (locked != null){
        while (locked.compareAndSet(false,true)){
            value = dht.get(k);
            locked.compareAndSet(true,false);
            return value;
        }
    } else {
        inUse.put(k,new AtomicBoolean(true));
        value = dht.get(k);
        inUse.get(k).compareAndSet(true,false);
        return value;
    }

return value;
}

public static void log(String event, Object... params){
    try {
        BufferedWriter bw = new BufferedWriter(new FileWriter("log", true));
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
