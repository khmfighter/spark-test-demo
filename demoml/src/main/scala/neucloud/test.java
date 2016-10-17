package neucloud;

import org.dmg.pmml.True;

import java.util.StringTokenizer;

/**
 * Created by Administrator on 2016/10/10 0010.
 */
public class test {
    public static void main( String [] s ){
        String sss = new String("The bb=Java asf plat form:is the,idea computing");

        StringTokenizer st = new StringTokenizer(sss," \t\n\n\f=,:",false);
        System.out.println( "Token Total: " + st.countTokens() );
        while( st.hasMoreElements() ){
            System.out.println( st.nextToken() );
        }
    }
}
