package com.atguigu.hiveudf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class NewUDF extends GenericUDF {

    /**
     * 初始化，检查输入输出的个数和类型
     * @param objectInspectors 检查输入参数的个数inspector
     * @return 检查输出参数的类型
     * @throws UDFArgumentException
     */
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 1){
            throw new UDFArgumentLengthException("Only accept one parameter");
        }
        if(!objectInspectors[0].getTypeName().equalsIgnoreCase("string")){
            throw new UDFArgumentTypeException(0,"Not a  string :" + objectInspectors[0].getTypeName());
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }


    /**
     * 主要的逻辑架构
     * @param deferredObjects
     * @return
     * @throws HiveException
     */
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object o = deferredObjects[0].get();
        if(o == null){
            return 0;
        }
        return o.toString().length();
    }

    public String getDisplayString(String[] strings) {
        return getStandardDisplayString("mylen",strings);
    }
}
