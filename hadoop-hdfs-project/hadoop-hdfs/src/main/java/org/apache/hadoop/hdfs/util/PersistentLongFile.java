/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.util;

import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * Class that represents a file on disk which persistently stores
 * a single <code>long</code> value. The file is updated atomically
 * and durably (i.e fsynced). 
 */
@InterfaceAudience.Private
public class PersistentLongFile {
  private static final Log LOG = LogFactory.getLog(
      PersistentLongFile.class);

  private final File file;
  private final long defaultVal;
  
  private long value;
  private boolean loaded = false;
  
  public PersistentLongFile(File file, long defaultVal) {
    this.file = file;
    this.defaultVal = defaultVal;
  }
  
  public long get() throws IOException {
    if (!loaded) {
      value = readFile(file, defaultVal);
      loaded = true;
    }
    return value;
  }
  
  public void set(long newVal) throws IOException {
    if (value != newVal || !loaded) {
      writeFile(file, newVal);
    }
    value = newVal;
    loaded = true;
  }

  /**
   * Atomically write the given value to the given file, including fsyncing.
   * 翻译 ： 把 给定的 value 写到给定的 file 中
   * @param file destination file
   * @param val value to write
   * @throws IOException if the file cannot be written
   */
  public static void writeFile(File file, long val) throws IOException {
    AtomicFileOutputStream fos = new AtomicFileOutputStream(file);
    try {
      fos.write(String.valueOf(val).getBytes(Charsets.UTF_8));
      fos.write('\n');
      fos.close();
      fos = null;
    } finally {
      if (fos != null) {
        fos.abort();        
      }
    }
  }

  public static long readFile(File file, long defaultVal) throws IOException {
    long val = defaultVal;
    if (file.exists()) {
      BufferedReader br = 
          new BufferedReader(new InputStreamReader(new FileInputStream(
              file), Charsets.UTF_8));
      try {
        val = Long.parseLong(br.readLine());
        br.close();
        br = null;
      } finally {
        IOUtils.cleanup(LOG, br);
      }
    }
    return val;
  }
}
