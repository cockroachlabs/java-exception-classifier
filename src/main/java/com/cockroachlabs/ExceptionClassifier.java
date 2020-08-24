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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * An ExceptionClassifier encapsulates a set of rules that classify
 * Throwables as retryable or not.
 * <p>
 * A classifier is driven by a key/value ruleset.
 * <p>
 * Keys are of the form:
 * <pre>
 *   className = ACTION
 *   className;regular expression = ACTION
 *   sqlState.12345 = ACTION
 *   sqlState.12345;regular expression = ACTION
 * </pre>
 * <p>
 * And <code>ACTION</code> is one of <code>RETRY</code> or <code>THROW</code>.
 * <p>
 * If a regular expression is provided, it will be matched against {@link Throwable#getMessage()}.
 * Matching is performed against causal (more specific) throwables before their enclosing
 * (more generic) throwables.
 *
 * @see Rule#DefaultPrecedence
 */
public final class ExceptionClassifier {
  /**
   * Construct a new ExceptionClassifier from a Map.
   */
  public static ExceptionClassifier fromMap(Map<String, String> map) {
    return new ExceptionClassifier(map);
  }

  /**
   * Construct a new ExceptionClassifier from a Properties object.
   */
  @SuppressWarnings("unchecked")
  public static ExceptionClassifier fromProperties(Properties props) {
    return fromMap((Map<String, String>) (Map<?, ?>) props);
  }

  /**
   * Construct a new ExceptionClassifier using a properties file loaded from the context classloader.
   */
  public static ExceptionClassifier fromResource(String name) throws IOException {
    InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    if (in == null) {
      throw new IOException("could not find resource from context classloader: " + name);
    }

    // Load the properties file
    Properties props = new Properties();
    props.load(in);

    return fromProperties(props);
  }

  static final String sqlStatePrefix = "sqlState.";
  private static final Logger logger = LoggerFactory.getLogger(ExceptionClassifier.class);


  /**
   * This field memoizes the results of {@link #findRulesFor}.
   */
  private final ConcurrentMap<Class<?>, List<Rule>> applicableRules = new ConcurrentHashMap<>();
  /**
   * All rules defined in the properties file.
   */
  private final List<Rule> sortedRules;

  ExceptionClassifier(Map<String, String> props) {
    List<Rule> rules = new ArrayList<>();
    props.forEach((key, value) -> {
      logger.trace("Raw key/value: {} = {}", key, value);
      String className;
      String pattern;

      Action action = Action.valueOf(value.toUpperCase().trim());

      int idx = key.indexOf(';');
      if (idx == -1) {
        className = key;
        pattern = "";
      } else {
        className = key.substring(0, idx);
        pattern = key.substring(idx + 1);
      }

      Pattern compiledPattern = null;
      if (!pattern.isEmpty()) {
        compiledPattern = Pattern.compile(pattern);
      }

      Class<?> target;
      String sqlState = null;
      if (className.startsWith(sqlStatePrefix)) {
        target = SQLException.class;
        sqlState = className.substring(sqlStatePrefix.length());
      } else {
        try {
          target = Thread.currentThread().getContextClassLoader().loadClass(className);
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException(e);
        }
      }
      rules.add(new Rule(action, compiledPattern, sqlState, target));
    });

    // Sort the resulting Rules into a priority order.
    rules.sort(Rule.DefaultPrecedence);
    sortedRules = Collections.unmodifiableList(rules);

    if (logger.isTraceEnabled()) {
      logger.trace("Sorted rule set follows");
      sortedRules.forEach(r -> logger.trace(r.toString()));
    }
  }

  /**
   * Returns true if the given throwable matches a retry rule.
   */
  public boolean shouldRetry(Throwable t) {
    // Push the throwable chain into a stack, we want to
    // examine the causal (detail) exceptions before the (generic)
    // enclosing exceptions.
    Stack<Throwable> stack = new Stack<>();
    for (Throwable x = t; x != null; x = x.getCause()) {
      stack.push(x);
    }

    while (!stack.empty()) {
      Throwable ex = stack.pop();
      // Calling getMessage() can be expensive.
      String msg = ex.getMessage();
      for (Rule r : findRulesFor(ex.getClass())) {
        Action action = r.decide(ex, msg);
        if (action != Action.IGNORE) {
          if (logger.isTraceEnabled()) {
            logger.trace("{} -> {}", t, action, t);
          } else {
            logger.debug("{} -> {}", t, action);
          }
          return action == Action.RETRY;
        }
      }
    }
    logger.trace("No match for exception", t);
    return false;
  }

  /**
   * Returns a memoized list of the rules that apply to the given type,
   * in evaluation order.
   */
  private List<Rule> findRulesFor(Class<?> clazz) {
    return applicableRules.computeIfAbsent(clazz, k -> {
        List<Rule> ret = sortedRules.stream()
          .filter(r -> r.appliesTo(k))
          .collect(Collectors.toUnmodifiableList());
        logger.trace("Found rules {} -> {}", clazz.getName(), ret);
        return ret;
      }
    );
  }
}