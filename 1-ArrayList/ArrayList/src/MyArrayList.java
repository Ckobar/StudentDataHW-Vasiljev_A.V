import java.util.Arrays;

public class MyArrayList {

    private static final int DEFAULT_SIZE = 10;
    private Object [] elements;
    private Integer REALLY_SIZE_MAX;
    private Integer CURRENT_SIZE_FOR_NEXT = 0;

    public MyArrayList(int size){
        this.elements = new Object[size];
        this.REALLY_SIZE_MAX = size;
    }

    public MyArrayList(){
        this.elements = new Object[DEFAULT_SIZE];
        this.REALLY_SIZE_MAX = 10;
    }

    public Object get(int index){
        return elements[index];
    }

    public void add(int index, Object object){
        checkSize();
        System.arraycopy(elements, index, elements, index+1, CURRENT_SIZE_FOR_NEXT -index);
        elements[index] = object;
        CURRENT_SIZE_FOR_NEXT++;
    }

    public void add(Object object){
        checkSize();
        elements[CURRENT_SIZE_FOR_NEXT] = object;
        CURRENT_SIZE_FOR_NEXT++;
    }

    /**
     * Examines the current size and increases the size of the array, if an overflow occurs
     */
    private void checkSize(){
        if(CURRENT_SIZE_FOR_NEXT >=REALLY_SIZE_MAX){
            REALLY_SIZE_MAX+=REALLY_SIZE_MAX/2;
            elements = Arrays.copyOf(elements,REALLY_SIZE_MAX);
        }
    }

    public boolean isEmpty(){
        return CURRENT_SIZE_FOR_NEXT == 0;
    }

    public int size(){
        return CURRENT_SIZE_FOR_NEXT - 1;
    }

    public boolean set(int index, Object object){
        if(index<CURRENT_SIZE_FOR_NEXT) elements[index] = object;
        else return false;
        return true;
    }

    public void remove(int index){
        if(index<CURRENT_SIZE_FOR_NEXT){
            System.arraycopy(elements, index+1, elements, index, CURRENT_SIZE_FOR_NEXT-index-1);
            CURRENT_SIZE_FOR_NEXT--;
        }
    }

    public void remove(Object object){
        if(object!=null) {
            for (int i = 0; i < CURRENT_SIZE_FOR_NEXT; i++) {
                if (object.equals(elements[i]))
                    remove(i);
            }
        }
    }

    public boolean contains(Object object){
        for (int i = 0; i < CURRENT_SIZE_FOR_NEXT; i++) {
            if (object.equals(elements[i]))
                return true;
        }
        return false;
    }

    public static void main(String arg[]){
        MyArrayList temp = new MyArrayList();
        System.out.println(temp.isEmpty());
        for (int i = 0; i < 12; i++) {
            temp.add(i);
        }
        for (int i = 0; i < 16; i++) {
            temp.add(2,30);
        }
        Integer ten = 10;
        System.out.println(temp.get(temp.size()));
        temp.set(temp.size(), 100);
        System.out.println(temp.get(temp.size()));
        temp.remove(temp.size());
        System.out.println(temp.get(temp.size()));
        System.out.println(temp.contains(ten));
        temp.remove(ten);
        System.out.println(temp.get(temp.size()));
        System.out.println(temp.contains(ten));
        System.out.println(temp.isEmpty());
    }
}
