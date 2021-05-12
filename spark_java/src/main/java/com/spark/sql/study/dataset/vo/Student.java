package com.spark.sql.study.dataset.vo;

import java.io.Serializable;

/**
 * @author yangqian
 * @date 2021/5/12
 */
public class Student implements Serializable {

    private static final long serialVersionUID = 3983064889301465215L;
    public int id;
    public String name;
    public int age;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

}
