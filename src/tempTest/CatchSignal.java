package tempTest;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class CatchSignal implements SignalHandler {

    public static void main(String[] args) {
        new CatchSignal().handle(new Signal(""));
    }

    public void handle(Signal signal) {

    }
}