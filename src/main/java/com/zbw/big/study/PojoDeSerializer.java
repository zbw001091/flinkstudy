package com.zbw.big.study;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.gson.Gson;
import com.zbw.big.study.pojo.Pojo;

public class PojoDeSerializer implements DeserializationSchema<Pojo> {

	@Override
    public Pojo deserialize(byte[] message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN);
             
        String mess = this.byteBuffertoString(buffer);
        
        //封装为POJO类
        Gson gson = new Gson();
        Pojo data = gson.fromJson(mess, Pojo.class);
        return data;
    }
 
    @Override
    public boolean isEndOfStream(Pojo nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Pojo> getProducedType() {
    	return TypeInformation.of(new TypeHint<Pojo>(){});
    }
 
    /**
     * 将ByteBuffer类型转换为String类型
     * @param buffer
     * @return
     */
    public static String byteBuffertoString(ByteBuffer buffer)
    {
        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;
        try
        {
            charset = Charset.forName("UTF-8");
            decoder = charset.newDecoder();
            // charBuffer = decoder.decode(buffer);//用这个的话，只能输出来一次结果，第二次显示为空
            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
            return charBuffer.toString();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            return "";
        }
    }
}
