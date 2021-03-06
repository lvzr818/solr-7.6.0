/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.cloud.api.collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;

/**
 * This class implements the tests for local file-system integration for Solr backup/restore capability.
 * Note that the Solr backup/restore still requires a "shared" file-system. Its just that in this case
 * such file-system would be exposed via local file-system API.
 */
//commented 9-Aug-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
@LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-13039") // added 3-Dec-2018
public class TestLocalFSCloudBackupRestore extends AbstractCloudBackupRestoreTestCase {
  private static String backupLocation;

  @BeforeClass
  public static void setupClass() throws Exception {
    configureCluster(NUM_SHARDS)// nodes
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    boolean whitespacesInPath = random().nextBoolean();
    if (whitespacesInPath) {
      backupLocation = createTempDir("my backup").toAbsolutePath().toString();
    } else {
      backupLocation = createTempDir("mybackup").toAbsolutePath().toString();
    }
  }

  @Override
  public String getCollectionName() {
    return "backuprestore";
  }

  @Override
  public String getBackupRepoName() {
    return null;
  }

  @Override
  public String getBackupLocation() {
    return backupLocation;
  }

  @Override
  @Test
  //Commented 14-Oct-2018 @BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-12028") // added 09-Aug-2018
  public void test() throws Exception {
    super.test();
  }
  }
