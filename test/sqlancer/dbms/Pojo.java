package sqlancer.dbms;

import java.io.Serializable;

public class Pojo implements Serializable {
    private String name;
    private int age;

    public Pojo(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public Pojo(int key) {
        this.name = "Name" + key;
        this.age = 5 * key;
    }
}
