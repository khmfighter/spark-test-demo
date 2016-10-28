package com.jary.clone;

/**
 * Created by Administrator on 2016/10/25 0025.
 */
public class Teacher2  implements Cloneable {
    public int age;
    public String name;

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

    @Override
    public Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }
}
