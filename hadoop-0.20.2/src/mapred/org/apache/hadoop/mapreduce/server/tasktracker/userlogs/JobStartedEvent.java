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
package org.apache.hadoop.mapreduce.server.tasktracker.userlogs;

import org.apache.hadoop.mapreduce.JobID;

/**
 * This is an {@link UserLogEvent} sent when the job starts.
 */
public class JobStartedEvent extends UserLogEvent {
  private JobID jobid;

  /**
   * Create the event to inform the job has started.
   * 
   * @param jobid
   *          The {@link JobID} which started
   */
  public JobStartedEvent(JobID jobid) {
    super(EventType.JOB_STARTED);
    this.jobid = jobid;
  }

  /**
   * Get the job id.
   * 
   * @return object of {@link JobID}
   */
  public JobID getJobID() {
    return jobid;
  }
}
