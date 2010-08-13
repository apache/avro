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

package org.apache.avro.ipc.trace;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file-based { @link SpanStorage } implementation for Avro's 
 * { @link TracePlugin }. This class has two modes, one in which writes are 
 * buffered and one in which they are not. Even without buffering, there will be
 * some delay between reporting of a Span and the actual disk write.
 */
public class FileSpanStorage implements SpanStorage {
  /*
   * We use rolling Avro data files that store Span data associated with ten 
   * minute chunks (this provides a simple way to index on time). Because we 
   * enforce an upper limit on the number of spans stored, we simply drop 
   * oldest file if and when the next write causes us to exceed that limit. This
   * approximates a FIFO queue of spans, which is basically what we want to 
   * maintain.
   * 
   * Focus is on efficiency since most logic occurs every
   * time a span is recorded (that is, every RPC call).
   * 
   * We never want to block on span adding operations, which occur in the same
   * thread as the Requestor. We are okay to block on span retrieving 
   * operations, since they typically run the RPCPlugin's own servlet. To
   * avoid blocking on span addition we use a separate WriterThread which reads
   * a BlockingQueue of Spans and writes span data to disk.  
   */
  
  private class DiskWriterThread implements Runnable {

    /** Shared Span queue. Read-only for this thread. */
    private BlockingQueue<Span> outstanding;
    
    /** Shared queue of files currently in view. Read/write for this thread. */
    private TreeMap<Long, File> files;
    
    /** How many Spans already written to each file. */
    private HashMap<File, Long> spansPerFile = new HashMap<File, Long>();  
    
    /** Total spans already written so far. */
    private long spansSoFar;
        
    /** DiskWriter for current file. */
    private DataFileWriter<Span> currentWriter;
    
    /** Timestamp of the current file. */
    private long currentTimestamp = (long) 0;
    
    /** Whether to buffer file writes.*/
    private boolean doBuffer;
    
    /** Compression level for files. */
    private int compressionLevel;
    
    /**
     * Thread that runs continuously and writes outstanding requests to
     * Avro files. This thread also deals with rolling files over and dropping
     * old files when the span limit is reached.
     * @param compressionLevel 
     */
    public DiskWriterThread(BlockingQueue<Span> outstanding,  
        TreeMap<Long, File> files, boolean buffer, 
        int compressionLevel) {
      this.outstanding = outstanding;
      this.files = files;
      this.doBuffer = buffer;
      this.compressionLevel = compressionLevel; 
      
      /* TODO(pwendell) load existing files here.
       * 
       * If we decide to re-factor TracePlugin such that only one exists, we 
       * can safely load old span files and include them here.*/ 
    }
    
    public void run() {
      while (true) {
        Span s = null;
        try {
          s = this.outstanding.take();
        } catch (InterruptedException e1) {
          LOG.warn("Thread interrupted");
          Thread.currentThread().interrupt();
        }
        try {
          assureCurrentWriter();
          this.currentWriter.append(s);
          if (!this.doBuffer) this.currentWriter.flush();
          this.spansSoFar += 1;
          File latest = this.files.lastEntry().getValue();
          long fileSpans = this.spansPerFile.get(latest);
          this.spansPerFile.put(latest, fileSpans + 1);
        } catch (IOException e) {
          LOG.warn("Error setting span file: " + e.getMessage());
        }
      }
    }
    
    /**
     * Assure that currentWriter is populated and refers to the correct
     * data file. This may roll-over the existing data file. Also assures
     * that writing one more span will not violate limits on Span storage.
     * @throws IOException 
     */
    private void assureCurrentWriter() throws IOException {
      boolean createNewFile = false;
      
      // Will we overshoot policy?
      while (this.spansSoFar >= maxSpans) {
        File oldest = null;
        // If spansSoFar is positive, there must be at least one file
        synchronized (this.files) {
          oldest = this.files.remove(this.files.firstKey());
        }
        this.spansSoFar -= spansPerFile.get(oldest);
        spansPerFile.remove(oldest);
        oldest.delete();
      }
      if (files.size() == 0) { 
        // In corner case we have removed the current file,
        // if that happened we need to clear current variables.
        currentTimestamp = (long) 0;
        currentWriter = null;
      }
      long rightNow = System.currentTimeMillis() / 1000L;
      
      // What file should we be in
      long cutOff = floorSecond(rightNow);
      
      if (currentWriter == null) {
        createNewFile = true;
      }
      // Test for roll-over.
      else if (cutOff >= (currentTimestamp + secondsPerFile)) {
        currentWriter.close();
        createNewFile = true;
      }
      
      if (createNewFile) {
        File newFile = new File(traceFileDir + "/" + 
            Thread.currentThread().getId() + "_" + cutOff + FILE_SUFFIX);
        synchronized (this.files) {
          this.files.put(cutOff, newFile);
        }
        this.spansPerFile.put(newFile, (long) 0);
        this.currentWriter = new DataFileWriter<Span>(SPAN_WRITER);
        this.currentWriter.setCodec(CodecFactory.deflateCodec(compressionLevel));
        this.currentWriter.create(Span.SCHEMA$, newFile);
        this.currentTimestamp = cutOff;
      }
    }
  }
  /** Directory of data files */
  private final static String FILE_SUFFIX = ".av";

  private final static SpecificDatumWriter<Span> SPAN_WRITER = 
    new SpecificDatumWriter<Span>(Span.class);
  private final static SpecificDatumReader<Span> SPAN_READER = 
    new SpecificDatumReader<Span>(Span.class);
  
  private static final Logger LOG = LoggerFactory.getLogger(FileSpanStorage.class);
  
  private long maxSpans = DEFAULT_MAX_SPANS;
  
  /** Granularity of file chunks. */
  private int secondsPerFile = 60 * 10; // default: ten minute chunks
  
  private String traceFileDir = "/tmp";
  
  /** Shared queue of files currently in view. This thread only reads.*/
  private TreeMap<Long, File> files = new TreeMap<Long, File>();
  
  /** Shared Span queue. This thread only writes. */
  LinkedBlockingQueue<Span> outstanding = new LinkedBlockingQueue<Span>();
  
  /** DiskWriter thread */
  private Thread writer;
    
  /**
   * Given a path to a data file of Spans, extract all spans and add them
   * to the provided list.
   */
  private static void readFileSpans(File f, List<Span> list)
    throws IOException {
    DataFileReader<Span> reader = new DataFileReader<Span>(f, SPAN_READER);
    Iterator<Span> it = reader.iterator();
    ArrayList<Span> spans = new ArrayList<Span>();
    while (it.hasNext()) {
      spans.add(it.next());
    }
    list.addAll(spans);
  }
  
  /**
   * Given a path to a data file of Spans, extract spans within a time period
   * bounded by start and end.
   */
  private static void readFileSpans(File f, List<Span> list, 
      long start, long end) throws IOException {
    DataFileReader<Span> reader = new DataFileReader<Span>(f, SPAN_READER);
    Iterator<Span> it = reader.iterator();
    ArrayList<Span> spans = new ArrayList<Span>();
    while (it.hasNext()) {
      Span s = it.next();
      if (Util.spanInRange(s, start, end)) {
        spans.add(s);
      }
    }
    list.addAll(spans);
  }
  
  public FileSpanStorage(boolean buffer, TracePluginConfiguration conf) {
    this.writer = new Thread(new DiskWriterThread(
        outstanding, files, buffer, conf.compressionLevel));
    this.secondsPerFile = conf.fileGranularitySeconds;
    this.traceFileDir = conf.spanStorageDir;
    this.writer.start();
  }
  
  /**
   * Return the head of the time bucket associated with this specific time.
   */
  private long floorSecond(long currentSecond) {
    return currentSecond - (currentSecond % this.secondsPerFile);
  }
  
  @Override
  public void addSpan(Span s) {
    this.outstanding.add(s);
  }

  @Override
  public List<Span> getAllSpans() {
    ArrayList<Span> out = new ArrayList<Span>();
    synchronized (this.files) { 
      for (File f: this.files.values()) {
        try {
          readFileSpans(f, out);
        } catch (IOException e) {
          LOG.warn("Error reading file: " + 
              f.getAbsolutePath());
        }
      }
    }
    return out;
  }
  
  /**
   * Clear all Span data stored by this plugin.
   */
  public void clear() {
    synchronized (this.files) { 
      for (Long l: new LinkedList<Long>(this.files.keySet())) {
        File f = this.files.remove(l);
        f.delete();
      }
    }
  }

  @Override
  public void setMaxSpans(long maxSpans) {
    this.maxSpans = maxSpans;
  }

  @Override
  public List<Span> getSpansInRange(long start, long end) {
    /*
     * We first find the book-end files (first and last) whose Spans may 
     * or may not fit in the the range. Intermediary files can be directly 
     * passed, since they are completely within the time range.
     * 
     *       [                            ]   <-- Time range
     *       
     * |-----++|+++++++|+++++++|+++++++|+++----|-------|--->
     *  \     /                         \     /
     *   start                            end
     *   file                             file
     * 
     */ 
    List<Span> out = new ArrayList<Span>();
    List<Long> middleFiles = new LinkedList<Long>();
    
    long startSecond = start / SpanStorage.NANOS_PER_SECOND;
    long endSecond = end / SpanStorage.NANOS_PER_SECOND;
    
    int numFiles = (int) (endSecond - startSecond) / secondsPerFile;
    for (int i = 1; i < (numFiles); i++) {
      middleFiles.add(startSecond + i * secondsPerFile);
    }
    
    synchronized (this.files) { 
      for (Long l: middleFiles) {
        if (files.containsKey(l)) {
          try {
            readFileSpans(files.get(l), out);
          } catch (IOException e) {
            LOG.warn("Error reading file: " + files.get(l).getAbsolutePath());
          }
        }
      }
      
      // Start file
      if (files.containsKey(startSecond)) {
        try {
          readFileSpans(files.get(startSecond), out, start, end);
        } catch (IOException e) {
          LOG.warn("Error reading file: " + 
              files.get(startSecond).getAbsolutePath());
        }
      }
      
      // End file
      if (files.containsKey(endSecond)) {
        try {
          readFileSpans(files.get(endSecond), out, start, end);
        } catch (IOException e) {
          LOG.warn("Error reading file: " + 
              files.get(endSecond).getAbsolutePath());
        }
      }
    }
    return out;
  }
}
