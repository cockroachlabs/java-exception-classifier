// Copyright 2020 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cockroachlabs;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import static java.util.Map.entry;
import static org.junit.Assert.fail;

class SuperException extends Exception {
  SuperException() {
    this("super");
  }

  SuperException(String msg) {
    super(msg);
  }
}

class SibException extends SuperException {
  SibException() {
    this("sib");
  }

  SibException(String msg) {
    super(msg);
  }
}

class SubException extends SuperException {
  SubException() {
    this("sub");
  }

  SubException(String msg) {
    super(msg);
  }
}

class SubSubException extends SubException {
  SubSubException() {
    this("subSub");
  }

  SubSubException(String msg) {
    super(msg);
  }
}

public class ExceptionClassifierTest {
  final String superName = SuperException.class.getName();
  final String sibName = SibException.class.getName();
  final String subName = SubException.class.getName();
  final String subSubName = SubSubException.class.getName();

  @Test
  public void testBadAction() {
    Map<String, String> config = Map.ofEntries(
      entry(superName, "DoesNotExist")
    );
    try {
      ExceptionClassifier.fromMap(config);
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testEmpty() {
    Map<String, String> config = Map.ofEntries();

    ExceptionClassifier c = ExceptionClassifier.fromMap(config);
    Assert.assertFalse(c.shouldRetry(new RuntimeException()));
  }

  @Test
  public void testMissingClass() {
    Map<String, String> config = Map.ofEntries(
      entry("DoesNotExist", Action.RETRY.name())
    );

    try {
      ExceptionClassifier.fromMap(config);
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testPropertyLoad() throws IOException {
    ExceptionClassifier.fromResource("com/cockroachlabs/example.properties");
  }

  @Test
  public void testRetryMiddle() {
    Map<String, String> config = Map.ofEntries(
      entry(superName, Action.THROW.name()),
      entry(sibName, Action.RETRY.name()),
      entry(subName, Action.RETRY.name()),
      entry(subSubName, Action.THROW.name())
    );

    ExceptionClassifier c = ExceptionClassifier.fromMap(config);

    Assert.assertFalse(c.shouldRetry(new SuperException()));
    Assert.assertTrue(c.shouldRetry(new SibException()));
    Assert.assertTrue(c.shouldRetry(new SubException()));
    Assert.assertFalse(c.shouldRetry(new SubSubException()));
  }

  @Test
  public void testRetrySuper() {
    Map<String, String> config = Map.ofEntries(
      entry(superName, Action.RETRY.name()),
      entry(sibName, Action.THROW.name()),
      entry(subName, Action.THROW.name())
    );

    ExceptionClassifier c = ExceptionClassifier.fromMap(config);

    Assert.assertTrue(c.shouldRetry(new SuperException()));
    Assert.assertFalse(c.shouldRetry(new SibException()));
    Assert.assertFalse(c.shouldRetry(new SubException()));
    Assert.assertFalse(c.shouldRetry(new SubSubException()));
  }

  @Test
  public void testSQLState() {
    Map<String, String> config = Map.ofEntries(
      entry(ExceptionClassifier.sqlStatePrefix + "40001", Action.RETRY.name()),
      entry(ExceptionClassifier.sqlStatePrefix + "40001;throw", Action.THROW.name())
    );

    ExceptionClassifier c = ExceptionClassifier.fromMap(config);

    Assert.assertTrue(c.shouldRetry(new SQLException("kabooom", "40001")));
    Assert.assertFalse(c.shouldRetry(new SQLException("kabooom", "45678")));
    Assert.assertFalse(c.shouldRetry(new SQLException("This should throw.", "40001")));
    Assert.assertFalse(c.shouldRetry(new SQLException("This should throw.", (String)null)));
  }


  @Test
  public void testUnwrap() {
    Map<String, String> config = Map.ofEntries(
      entry(superName, Action.THROW.name()),
      entry(subSubName, Action.RETRY.name())
    );

    ExceptionClassifier c = ExceptionClassifier.fromMap(config);

    Assert.assertFalse(c.shouldRetry(new RuntimeException(new SibException())));
    Assert.assertTrue(c.shouldRetry(new RuntimeException(new SubSubException())));
    Assert.assertTrue(c.shouldRetry(new SuperException().initCause(new SubSubException())));
  }
}
