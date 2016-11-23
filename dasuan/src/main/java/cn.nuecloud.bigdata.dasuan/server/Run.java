package cn.nuecloud.bigdata.dasuan.server;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.support.ClassPathXmlApplicationContext;

@Slf4j
public class Run {

    private volatile static Run run;
    private volatile static ClassPathXmlApplicationContext context;

    private Run (){
        context = new ClassPathXmlApplicationContext(new String[]{"classpath:spring-*.xml"});
        context.start();
    }

    public static Run getInstance() {
        if (run == null) {
            synchronized (Run.class) {
                if (run == null) {
                    run = new Run();
                }
            }
        }
        return run;
    }

    // 获取spring管理的bean
    public <T> T getBean(Class<T> clazz){
        String className = toLowerCaseFirstOne(clazz.getSimpleName());
        return (T)context.getBean(className);
    }

    //首字母转小写
    public static String toLowerCaseFirstOne(String s)
    {
        if(Character.isLowerCase(s.charAt(0))){
            return s;
        } else {
            return (new StringBuilder()).append(Character.toLowerCase(s.charAt(0))).append(s.substring(1)).toString();
        }
    }
}
