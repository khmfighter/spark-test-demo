package com.jary.clone;

/**
 * Created by Administrator on 2016/10/25 0025.
 */
public class Student2 implements Cloneable{
    public int age;
    public String name;
    public Teacher2 teacher;

    public int getAge()
    {
        return age;
    }

    public void setAge(int age)
    {
        this.age = age;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Teacher2 getTeacher2()
    {
        return teacher;
    }

    public void setTeacher2(Teacher2 teacher)
    {
        this.teacher = teacher;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        Student2 student2 = (Student2) super.clone();
        student2.setTeacher2((Teacher2) student2.getTeacher2().clone());
        return student2;
    }
}
