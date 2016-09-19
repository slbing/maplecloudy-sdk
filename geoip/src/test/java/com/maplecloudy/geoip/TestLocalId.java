package com.maplecloudy.geoip;
import org.junit.Test;

import com.maplecloudy.geoip.model.AreaVo;
import com.maplecloudy.geoip.model.LocalId;

/**
 * @Description: TODO
 * @author user email
 * @date 2013-5-14 上午10:24:54
 */

public class TestLocalId {
  @Test
  public void testLocalId() {
    AreaVo l1 = new AreaVo("1","2","3","4");
    AreaVo l2 = new AreaVo("1","2","3","4");
    
    System.out.println(l1.equals(l2));
    System.out.println(l1.compareTo(l2));
    System.out.println(new LocalId("086-0450").hashCode());
    System.out.println(LocalId.schema);
    
  }
  
}
