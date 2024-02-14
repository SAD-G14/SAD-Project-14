import java.util.concurrent.Callable;

public class Consumer implements Runnable{

    private Client client;
    private Callable<Void> func;

    public Consumer(Client client, Callable<Void> func) {
        this.client = client;
        this.func = func;
    }

    public void run() {
        try {
            Thread.sleep((long) Client.TIME_BETWEEN_PULLS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Pair<String, byte[]> pair = this.client.pull();
        if (!pair.getFirst().equals("exception")) {
            try {
                this.func.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
