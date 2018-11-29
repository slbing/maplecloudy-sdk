package com.maplecloudy.bi.util;


import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.maplecloudy.bi.util.ShareConstants;

public class ConstantsUtils implements ShareConstants {
  public static synchronized String generateSegmentName() {
    try {
      Thread.sleep(1000);
    } catch (Throwable t) {}
    ;
    return sdf.format(new Date(System.currentTimeMillis()));
  }
  
  public static Path getTmpSegmentDir(String base) {
    return getTmpSegmentDir(new Path(base));
  }
  
  public static Path getTmpSegmentDir(Path base) {
    return new Path(base, TMP + "/" + ConstantsUtils.generateSegmentName());
  }
  
  public static Path getTmpSegmentDir(String base, String jobInfo) {
    return getTmpSegmentDir(new Path(base), jobInfo);
  }
  
  public static Path getTmpSegmentDir(Path base, String jobInfo) {
    return new Path(base, TMP + "/" + ConstantsUtils.generateSegmentName() + jobInfo);
  }
  
  public static Path getCurrentSegmentDir(String base) {
    return new Path(new Path(base), CURRENT + "/"
        + ConstantsUtils.generateSegmentName());
  }
  
  public static Path getBakDir(String base) {
    return new Path(base + "." + BAK);
  }
  
  public static Path getCurrentDir(String base) {
    return getCurrentDir(new Path(base));
  }
  
  public static Path getCurrentDir(Path base) {
    return new Path(base, CURRENT);
  }
  
  public static Path getStableDir(Path base) {
    return new Path(base, STABLE);
  }
  
  public static Path getCurrentUpdateDir(String base, Date startDate,
      Date endDate) {
    return new Path(new Path(new Path(base), CURRENT),
        FORMAT_JOB_DATE.format(startDate) + "-"
            + FORMAT_JOB_DATE.format(endDate));
  }
  
  public static Path getOldDir(String base) {
    return getOldDir(new Path(base));
  }
  
  public static Path getOldDir(Path base) {
    return new Path(base, OLD);
  }
  
  public static String getCurrentDirStr(String base) {
    return base + "/" + CURRENT;
  }
  
  public static boolean install(String base, Path tmpOutput, Configuration conf)
      throws IOException {
    Path curDir = getCurrentDir(base);
    Path old = getOldDir(base);
    return install(curDir, old, tmpOutput, conf);
  }
  
  public static boolean install(Path base, Path tmpOutput, Configuration conf)
      throws IOException {
    Path curDir = getCurrentDir(base);
    Path old = getOldDir(base);

    return install(curDir, old, tmpOutput, conf);
  }
  
  public static boolean installChild(Path base, Path tmpOutput,
      Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] fss = fs.listStatus(tmpOutput);
    for (FileStatus fst : fss) {
      Path al = new Path(base, fst.getPath().getName());
      install(al, fst.getPath(), conf);
    }
    return true;
  }
  
  public static boolean install(String base, Path path, Path tmpOutput,
      Configuration conf) throws IOException {
    Path old = getOldDir(base);
    return install(path, old, tmpOutput, conf);
  }
  
  public static boolean install(Path curDir, Path old, Path tmpOutput,
      Configuration conf) throws IOException {
    boolean bret = false;
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(curDir)) {
      fs.rename(curDir, old);
    } else {
      // if parents not exist then create them first
      Path parent = curDir.getParent();
      while (parent != null && !fs.exists(parent)) {
        fs.mkdirs(parent);
        parent = parent.getParent();
      }
    }
    if (fs.exists(tmpOutput)) bret = fs.rename(tmpOutput, curDir);
    if (bret) {
      if (fs.exists(old)) fs.delete(old, true);
    }
    return bret;
  }
  
  public static List<String> installAll(Path base, Path tmpOutput,
      Configuration conf) throws IOException {
    Map<String,String> map = Maps.newHashMap();
    List<String> list = Lists.newArrayList();
    ls(tmpOutput, base, new Configuration(), map);
    Set<String> keys = map.keySet();
    for (String key : keys) {
      install(new Path(map.get(key)), new Path(key), conf);
      list.add(map.get(key));
    }
    return list;
  }
  
  public static List<String> installAllPostfix(Path base, Path tmpOutput,
      Configuration conf, String postfix) throws IOException {
    Map<String,String> map = Maps.newHashMap();
    List<String> list = Lists.newArrayList();
    ls(tmpOutput, base, new Configuration(), map);
    Set<String> keys = map.keySet();
    for (String key : keys) {
      Path value = new Path(map.get(key), postfix);
      install(value, new Path(key), conf);
      list.add(value.toUri().getPath());
    }
    return list;
  }
  
  /**
   * Get a listing of all files in that match the file pattern <i>srcf</i>.
   * 
   * @param srcf
   *          a file pattern specifying source files
   * @param recursive
   *          if need to list files in subdirs
   * @throws IOException
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  private static int ls(Path srcBase, Path dstBase, Configuration conf,
      Map<String,String> listMap) throws IOException {
    FileSystem srcFs = FileSystem.get(conf);
    FileStatus[] srcs = srcFs.globStatus(srcBase);
    if (srcs == null || srcs.length == 0) {
      // throw new FileNotFoundException("Cannot access " +
      // srcBase.toUri().getPath() +
      // ": No such file or directory.");
      System.err.println("Cannot access " + srcBase.toUri().getPath()
          + ": No such file or directory.");
      return 0;
    }
    int numOfErrors = 0;
    for (int i = 0; i < srcs.length; i++) {
      numOfErrors += ls(srcs[i], srcFs, srcBase, dstBase, listMap);
    }
    return numOfErrors == 0 ? 0 : -1;
  }
  
  /*
   * list all files under the directory <i>src</i> ideally we should provide
   * "-l" option, that lists like "ls -l".
   */
  private static int ls(FileStatus src, FileSystem srcFs, Path srcBase,
      Path dstBase, Map<String,String> listMap) throws IOException {
    final FileStatus[] items = shellListStatus(srcFs, src, srcBase, dstBase,
        listMap);
    if (items == null) {
      return 1;
    } else {
      int numOfErrors = 0;
      boolean isLastDir = true;
      for (int i = 0; i < items.length; i++) {
        FileStatus stat = items[i];
//        Path cur = stat.getPath();
        // System.out.println(cur.toUri().getPath());
        if (stat.isDir()) {
          isLastDir = false;
          numOfErrors += ls(stat, srcFs, srcBase, dstBase, listMap);
        }
      }
      if (isLastDir) {
        String sp = src.getPath().toUri().getPath();
        String base = srcBase.toUri().getPath();
        String common = sp.substring(sp.indexOf(base) + base.length());
        String dst = dstBase.toUri().getPath() + common;
        listMap.put(sp, dst);
      }
      
      return numOfErrors;
    }
  }
  
  private static FileStatus[] shellListStatus(FileSystem srcFs, FileStatus src,
      Path srcBase, Path dstBase, Map<String,String> listMap) {
    if (!src.isDir()) {
      FileStatus[] files = {src};
      return files;
    }
    Path path = src.getPath();
    try {
      FileStatus[] files = srcFs.listStatus(path);
      if (files == null) {
        System.err.println("could not get listing for '" + path + "'");
      } else {
        // if(files.length == 0)
        // {
        // String sp = path.toUri().getPath();
        // String base = srcBase.toUri().getPath();
        // String common = sp.substring(base.length());
        // String dst = dstBase.toUri().getPath() + common;
        // listMap.put(sp, dst);
        // // System.out.println("Found: " + sp + "|" + dst);
        // }
      }
      return files;
    } catch (IOException e) {
      System.err.println("could not get get listing for '" + path + "' : "
          + e.getMessage().split("\n")[0]);
    }
    return null;
  }
  
  public static Path getUserHome(String user) {
    return new Path("/user", user);
  }
  
 
}
