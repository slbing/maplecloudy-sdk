/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.maplecloudy.flume.channel;

public class FileChannelPlusConfiguration {
  /**
   * Directory Checkpoints will be written in
   */
  public static final String CHECKPOINT_DIR = "checkpointDir";

  
  /**
   * Directories data files will be written in. Multiple directories
   * can be specified as comma separated values. Writes will
   * be written in a round robin fashion.
   */
  public static final String DATA_DIR = "dataDir";
  
  /**
   * how many log count at a log file
   */
  public static final String ROLL_COUNT = "rollCount";
  
  /**
   * how many files keep at local system
   */
  public static final String LOGFILE_KEEP_COUNT = "keepCount";
  
  /**
   * Max file size for data files, cannot exceed the default. Default~ 1.5GB
   */
  public static final String MAX_FILE_SIZE = "maxFileSize";
  public static final long DEFAULT_MAX_FILE_SIZE =
        Integer.MAX_VALUE - (500L * 1024L * 1024L); // ~1.52 G

 
}
