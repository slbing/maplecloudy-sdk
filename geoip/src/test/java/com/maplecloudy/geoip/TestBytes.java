package com.maplecloudy.geoip;
import org.junit.Test;

import com.maplecloudy.avro.reflect.ReflectDataEx;
import com.maplecloudy.geoip.model.IpRange;

/**
 * @Description: TODO
 * @author user email
 * @date 2013-5-14 上午10:24:54
 */

public class TestBytes {
  @Test
  public void testIntAndByte() {
    byte[] bytes = short2byte((short) 480);
    
    System.out.println(ReflectDataEx.get().getSchema(IpRange.class));
    System.out.println(byte2short(bytes));
  }
  
  public static byte[] short2byte(short data) {
    byte[] buf = new byte[2];
    buf[0] = (byte) ((data >> 8) & 0xff);
    buf[1] = (byte) (data & 0xff);
    return buf;
  }
  
  public static short byte2short(byte[] data) {
    if (data == null || data.length != 2) {
      return 0;
    }
    return (short) ((data[0] & 0x00FF) << 8 | data[1] & 0x00ff);
  }
  
  public static class ShortArr {
    short[] arr;
  }
  
  
}
