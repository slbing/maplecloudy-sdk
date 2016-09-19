package com.maplecloudy.geoip.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.maplecloudy.geoip.model.AreaVo;
import com.maplecloudy.geoip.service.IPmem;

public class TestIp {
  public static void main(String[] args) throws IOException {
    while (true) {
      try {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        System.out
            .println("pleas input a ip such as 1.57.99.0 or type quit to exit:");
        while (!"quit".equals((line = br.readLine()))) {	
          AreaVo av = IPmem.get().getAreaInfo(IPmem.get().loopUp(line));
          System.out.println("counry: " + av.getCountry() + " province: " + av.getProvince() + " city: " 
        		  + av.getCity() + " isp: " + av.getIsp());
          System.out
              .println("pleas input a ip such as 1.57.99.0 or type quit to exit:");
        }
        
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}