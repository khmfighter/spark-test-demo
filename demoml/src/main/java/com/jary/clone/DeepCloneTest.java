package com.jary.clone;

/**
 * Created by Administrator on 2016/10/25 0025.
 */
public class DeepCloneTest {
    public static void main(String[] args) throws Exception
    {
        // teacher对象将不被clone出来的Student2对象共享.
        Teacher2 teacher = new Teacher2();
        teacher.setAge(40);
        teacher.setName("Teacher2 zhang");

        Student2 student1 = new Student2();
        student1.setAge(20);
        student1.setName("zhangsan");
        student1.setTeacher2(teacher);

        // 复制出来一个对象student2
        Student2 student2 = (Student2) student1.clone();
        System.out.println(student2.getAge());
        System.out.println(student2.getName());

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println(student1.getTeacher2().getAge());
        System.out.println(student1.getTeacher2().getName());

        // 修改student2的引用对象
        student2.getTeacher2().setAge(50);
        student2.getTeacher2().setName("Teacher2 Li");

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println(student1.getTeacher2().getAge());
        System.out.println(student1.getTeacher2().getName());
    }
}
