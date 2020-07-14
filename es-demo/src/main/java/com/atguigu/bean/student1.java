package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class student1 {
    private String stu_id;
    private String name;
    private int age;
    private String favo;

//    public student1(String stu_id, String name, int age, String favo) {
//        this.stu_id = stu_id;
//        this.name = name;
//        this.age = age;
//        this.favo = favo;
//    }
//
//    public void setStu_id(String stu_id) {
//        this.stu_id = stu_id;
//    }
//
//    public void setName(String name) {
//        this.name = name;
//    }
//
//    public void setAge(int age) {
//        this.age = age;
//    }
//
//    public void setFavo(String favo) {
//        this.favo = favo;
//    }
//
//    public String getStu_id() {
//        return stu_id;
//    }
//
//    public String getName() {
//        return name;
//    }
//
//    public int getAge() {
//        return age;
//    }
//
//    public String getFavo() {
//        return favo;
//    }
}
