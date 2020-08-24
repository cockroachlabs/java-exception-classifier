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

import javax.validation.constraints.NotNull;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.regex.Pattern;

import static java.util.Comparator.*;

class Rule {
  static final Comparator<Rule> DefaultPrecedence =
    comparing(Rule::getTarget, (a, b) -> {
      if (a.equals(b)) {
        return 0;
      }
      if (a.isAssignableFrom(b)) {
        return 1;
      }
      if (b.isAssignableFrom(a)) {
        return -1;
      }
      return 0;
    })
      .thenComparing(Rule::getTarget, comparing(Class::getName))
      .thenComparing(Rule::getSqlState, nullsLast(naturalOrder()))
      .thenComparing(Rule::getPattern, nullsLast(comparing(Pattern::pattern)))
      .thenComparing(Rule::getAction);

  private final Action action;
  private final Pattern pattern;
  private final String sqlState;
  private final Class<?> target;

  Rule(@NotNull Action action, Pattern pattern, String sqlState, @NotNull Class<?> target) {
    this.action = action;
    this.pattern = pattern;
    this.sqlState = sqlState;
    this.target = target;
  }

  boolean appliesTo(@NotNull Class<?> clazz) {
    return target.isAssignableFrom(clazz);
  }

  @NotNull
  Action decide(@NotNull Throwable t, @NotNull String msg) {
    if (!target.isAssignableFrom(t.getClass())) {
      return Action.IGNORE;
    }
    if (sqlState != null && !sqlState.equalsIgnoreCase(((SQLException) t).getSQLState())) {
      return Action.IGNORE;
    }
    if (pattern != null && !pattern.matcher(msg).find()) {
      return Action.IGNORE;
    }
    return action;
  }

  Action getAction() {
    return action;
  }

  Pattern getPattern() {
    return pattern;
  }

  String getSqlState() {
    return sqlState;
  }

  Class<?> getTarget() {
    return target;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    if (sqlState != null) {
      sb.append(ExceptionClassifier.sqlStatePrefix).append(sqlState);
    } else {
      sb.append(getTarget().getName());
    }

    if (pattern != null) {
      sb.append(";").append(pattern.pattern());
    }

    sb.append("=").append(action.name());
    return sb.toString();
  }
}