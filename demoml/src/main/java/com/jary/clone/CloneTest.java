package com.jary.clone;

/**
 * Created by Administrator on 2016/10/25 0025.
 */
public class CloneTest {
    public static void main(String[] strings) throws CloneNotSupportedException {

        Teacher teacher = new Teacher();
        teacher.setAge(22);
        teacher.setName("jary");

        Student student = new Student();
        student.setAge(0);
        student.setName("zhen");
        student.setTeacher(teacher);

        Student student2 = (Student) student.clone();
        System.out.println(student2.getAge());
        System.out.println(student2.getName());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println(student.getTeacher().getAge());
        System.out.println(student.getTeacher().getName());

        // 修改student2的引用对象
        student2.getTeacher().setAge(50);
        student2.getTeacher().setName("Teacher Li");

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println(student.getTeacher().getAge());
        System.out.println(student.getTeacher().getName());




    }
}
