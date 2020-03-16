package com.waniuzhang.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.text.DecimalFormat;

/**
 * Created by 1 on 2020/3/10.
 */
public class AvgScore extends GenericUDF {

    private static  final String FUN_NAME = "AvgScore";
    private transient MapObjectInspector map0i;

    DecimalFormat format = new DecimalFormat("#.##");

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //检测函数个数
        //检测函数类型
        map0i = (MapObjectInspector)objectInspectors[0];
        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object obj = deferredObjects[0].get();
        double avg = map0i.getMap(obj).values().stream().mapToDouble(a -> Double.parseDouble(a.toString())).average().orElse(0.0);
        return Double.parseDouble(format.format(avg));
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "Func(map)";
    }
}
