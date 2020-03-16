package com.waniuzhang.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by 1 on 2020/3/10.
 */
public class StrLen extends UDF {

    public int evaluate(final Text col){
        return col.getLength();
    }
}
