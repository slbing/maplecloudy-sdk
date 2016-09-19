package com.maplecloudy.geoip.service;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;
import com.maplecloudy.geoip.constant.Constants;
import com.maplecloudy.geoip.model.AreaVo;
import com.maplecloudy.geoip.model.IpRange;
import com.maplecloudy.geoip.model.LocalId;
import com.maplecloudy.geoip.util.SaveGeoIp2File;

public class IPmem {
  
  private static final Log logger = LogFactory.getLog(IPmem.class);
  
  /**
   * 存放地区详情的map，key-value 对应的是 area_code locId-AreaVo
   */
  public TreeMap<LocalId,AreaVo> mapLocal = Maps.newTreeMap();
  
  /**
   * 存放地区信息，索引为name
   */
  public Map<AreaVo,LocalId> mapArea = Maps.newTreeMap();
  
  /**
   * 存放ip的所有区间段，目前以腾讯为主 44W左右的区间 二分法查找 ，最坏的比较情况是20次 即 最坏经过20次比较即可准确定位区间
   */
  public TreeMap<IpRange,LocalId> mapIp = Maps.newTreeMap();
  
  private IPmem() {
    // System.out.println("调用初始化/..............");
    init(new Configuration());
  }
  
  private static IPmem im = null;
  
  public static IPmem get() {
    if (im == null) im = new IPmem();
    return im;
  }
  
  /**
   * 初始化ipmem这个类，把mysql中的地区信息已id-value的形式存到map里面，
   */
  private void init(Configuration conf) {
    try {
      mapLocal.put(Constants.DEFAULT_AREA_ID, new AreaVo("UNKNOW", "UNKNOW",
          "UNKNOW", "UNKNOW"));
      mapArea.put(new AreaVo("UNKNOW", "UNKNOW", "UNKNOW", "UNKNOW"),
          Constants.DEFAULT_AREA_ID);
      long s1 = System.currentTimeMillis();
      long s2 = 0;
      if (!SaveGeoIp2File.loadFromFile(mapIp, mapLocal, mapArea, conf)) {
        SaveGeoIp2File.loadFromTxtFile(mapIp, mapLocal, mapArea, conf);
        s2 = System.currentTimeMillis();
        // logger.info("total load:" + mapIp.size() + ":" + mapLocal.size() +
        // ":"
        // + mapArea.size() + ", 读入数据库信息到内存耗时:" + (s2 - s1) + "ms");
        SaveGeoIp2File.save(mapIp, mapLocal, conf);
      } else s2 = System.currentTimeMillis();
      logger.info("total load mapIp:" + mapIp.size() + " mapLocal:"
          + mapLocal.size() + " mapArea:" + mapArea.size() + ", 读入数据库信息到内存耗时:"
          + (s2 - s1) + "ms");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  /**
   * 查找ip地址对应力美的地区id
   * 
   * @param ip
   * @return
   */
  public LocalId loopUp(String ip) {
    LocalId ll = Constants.DEFAULT_AREA_ID;
    if (ip == null || !isLegalIP(ip)) {
      // logger.error("IPmem | 并发重用socket 产生了不合法的ip，返回8690 :" + ip);
      return Constants.DEFAULT_AREA_ID;
    }
    
    /* 转化ip地址 */
    // long s1 = System.currentTimeMillis();
    long lip = StringToLongIp(ip);
    Entry<IpRange,LocalId> entry = mapIp.floorEntry(new IpRange(lip, lip));
    if (entry == null) {
      logger.error("IPmem:找不到ip地址，返回了8690 :" + ip);
      
    } else {
      ll = entry.getValue();
      // System.out.println(entry.getKey());
    }
    return ll;
  }
  
  /**
   * 根据权重转化ip地址
   * 
   * @param ip
   * @return
   */
  protected static long StringToLongIp(String ip) {
    String[] s = ip.split("[.]");
    long re = 0;
    for (int i = 0; i < s.length; i++) {
      re += Math.pow(256, s.length - i - 1) * Integer.parseInt(s[i]);
    }
    return re;
  }
  
  public AreaVo getAreaInfo(LocalId id) {
    return mapLocal.get(id);
  }
  
  public AreaVo getAreaInfo(String local) {
    return mapLocal.get(new LocalId(local));
  }
  
  public static boolean isLegalIP(String ip) {
    return Pattern
        .compile(
            "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}")
        .matcher(ip).matches();
  }
  
  public static void main(String[] args) {
    
    // System.out.println("ok....................");
    // String startIp = "1.22.1.0";
    // String endIp = "1.23.255.0";
    // UpdateMemIp(startIp, endIp, "Iqaluit");
    // System.out.println("ok....................");
    // Integer s = loopUp("1.23.3.255");
    // System.out.println(s);
    
  }
}
