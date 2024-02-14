public class Pair <E, V>{

    private final E first;
    private final V second;

    public Pair(E first, V second) {
        this.first = first;
        this.second = second;
    }

    public E getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }
}
