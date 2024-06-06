/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;
import java.time.chrono.ChronoPeriod;
import java.time.chrono.IsoChronology;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * <p>
 * A temporal amount to model an {@link org.apache.avro.LogicalTypes.Duration
 * Avro duration} (the logical type).
 * </p>
 *
 * <p>
 * It consists of a number of months, days and milliseconds, all modelled as an
 * unsigned integer.
 * </p>
 *
 * <p>
 * Compared to {@link Period java.time.Period}, this class has a smaller range
 * ('only' supporting a little less than 358 million years), and cannot support
 * negative time periods.
 * </p>
 *
 * <p>
 * Compared to {@link Duration java.time.Duration}, this class has less
 * precision (milliseconds compared to nanoseconds), cannot support negative
 * durations, and has a much smaller range. Where {@code java.time.Duration}
 * supports fixed ranges up to about 68 years, {@code TimePeriod} can only
 * handle about 49 days.
 * </p>
 *
 * <p>
 * Comparison with the regular {@code java.time} classes:
 * </p>
 *
 * <table>
 * <tr>
 * <th></th>
 * <th>TimePeriod</th>
 * <th>{@link Period}</th>
 * <th>{@link Duration}</th>
 * </tr>
 * <tr>
 * <td>Precision</td>
 * <td>milliseconds</td>
 * <td>days</td>
 * <td>nanoseconds</td>
 * </tr>
 * <tr>
 * <td>Time range (approx.)</td>
 * <td>0 - 49 days</td>
 * <td>unsupported</td>
 * <td>-68 - 68 years</td>
 * </tr>
 * <tr>
 * <td>Date range (approx.)</td>
 * <td>0 to 370 million years</td>
 * <td>-2.3 to 2.3 billion years</td>
 * <td>unsupported</td>
 * </tr>
 * </table>
 *
 * @see <a href=
 *      "https://avro.apache.org/docs/1.11.1/specification/#duration">Avro 1.11
 *      specification on duration</a>
 */
public final class TimePeriod implements TemporalAmount, Serializable {
  private static final long MAX_UNSIGNED_INT = 0xffffffffL;
  private static final long MONTHS_PER_YEAR = 12;
  private static final long MONTHS_PER_DECADE = MONTHS_PER_YEAR * 10;
  private static final long MONTHS_PER_CENTURY = MONTHS_PER_DECADE * 10;
  private static final long MONTHS_PER_MILLENNIUM = MONTHS_PER_CENTURY * 10;
  private static final long MILLIS_PER_SECOND = 1_000;
  private static final long MILLIS_PER_MINUTE = MILLIS_PER_SECOND * 60;
  private static final long MILLIS_PER_HOUR = MILLIS_PER_MINUTE * 60;
  private static final long MILLIS_IN_HALF_DAY = MILLIS_PER_HOUR * 12;
  private static final long MICROS_PER_MILLI = 1_000;
  private static final long NANOS_PER_MILLI = 1_000_000;

  private final long months;
  private final long days;
  private final long millis;

  /**
   * Create a TimePeriod from another TemporalAmount, such as a {@link Period} or
   * a {@link Duration}.
   *
   * @param amount a temporal amount
   * @return the corresponding TimePeriod
   */
  public static TimePeriod from(TemporalAmount amount) {
    if (requireNonNull(amount, "amount") instanceof TimePeriod) {
      return (TimePeriod) amount;
    }
    if (amount instanceof ChronoPeriod) {
      if (!IsoChronology.INSTANCE.equals(((ChronoPeriod) amount).getChronology())) {
        throw new DateTimeException("TimePeriod requires ISO chronology: " + amount);
      }
    }
    long months = 0;
    long days = 0;
    long millis = 0;
    for (TemporalUnit unit : amount.getUnits()) {
      if (unit instanceof ChronoUnit) {
        long unitAmount = amount.get(unit);
        switch ((ChronoUnit) unit) {
        case MILLENNIA:
          months = unsignedInt(months + unitAmount * MONTHS_PER_MILLENNIUM);
          break;
        case CENTURIES:
          months = unsignedInt(months + unitAmount * MONTHS_PER_CENTURY);
          break;
        case DECADES:
          months = unsignedInt(months + unitAmount * MONTHS_PER_DECADE);
          break;
        case YEARS:
          months = unsignedInt(months + unitAmount * MONTHS_PER_YEAR);
          break;
        case MONTHS:
          months = unsignedInt(months + unitAmount);
          break;
        case WEEKS:
          days = unsignedInt(days + unitAmount * 7);
          break;
        case DAYS:
          days = unsignedInt(days + unitAmount);
          break;
        case HALF_DAYS:
          days = unsignedInt(days + (unitAmount / 2)); // Truncates halves
          if (unitAmount % 2 != 0) {
            millis = unsignedInt(millis + MILLIS_IN_HALF_DAY);
          }
          break;
        case HOURS:
          millis = unsignedInt(millis + unitAmount * MILLIS_PER_HOUR);
          break;
        case MINUTES:
          millis = unsignedInt(millis + unitAmount * MILLIS_PER_MINUTE);
          break;
        case SECONDS:
          millis = unsignedInt(millis + unitAmount * MILLIS_PER_SECOND);
          break;
        case MILLIS:
          millis = unsignedInt(millis + unitAmount);
          break;
        case MICROS:
          if (unitAmount % MICROS_PER_MILLI != 0) {
            throw new DateTimeException(
                "Cannot add " + unitAmount + " microseconds: not a whole number of milliseconds");
          }
          millis = unsignedInt(millis + unitAmount / MICROS_PER_MILLI);
          break;
        case NANOS:
          if (unitAmount % NANOS_PER_MILLI != 0) {
            throw new DateTimeException(
                "Cannot add " + unitAmount + " nanoseconds: not a whole number of milliseconds");
          }
          millis = unsignedInt(millis + unitAmount / NANOS_PER_MILLI);
          break;
        default:
          throw new UnsupportedTemporalTypeException("Unsupported unit: " + unit);
        }
      } else {
        throw new UnsupportedTemporalTypeException("Unsupported unit: " + unit);
      }
    }
    return new TimePeriod(months, days, millis);
  }

  /**
   * Create a TimePeriod from a number of months, days and milliseconds
   *
   * @param months a number of months
   * @param days   a number of days
   * @param millis a number of milliseconds
   * @return the corresponding TimePeriod
   * @throws ArithmeticException if any of the parameters does not fit an unsigned
   *                             long (0..4294967296)
   */
  public static TimePeriod of(long months, long days, long millis) {
    return new TimePeriod(unsignedInt(months), unsignedInt(days), unsignedInt(millis));
  }

  private static long unsignedInt(long number) {
    if (number != (number & MAX_UNSIGNED_INT)) {
      throw new ArithmeticException("Overflow/underflow of unsigned int");
    }
    return number;
  }

  private TimePeriod(long months, long days, long millis) {
    this.months = months;
    this.days = days;
    this.millis = millis;
  }

  public Duration toDuration() {
    return Duration.from(this);
  }

  public Period toPeriod() {
    if (isDateBased()) {
      // We use unsigned ints, which have double the range of a signed int that
      // Period uses. We can split months to years and months to ensure there's no
      // overflow. But we cannot split days, as both days and months have varying
      // lengths.
      int yearsAsInt = (int) (months / MONTHS_PER_YEAR);
      int monthsAsInt = (int) (months % MONTHS_PER_YEAR);
      int daysAsInt = (int) days;
      if (days != daysAsInt) {
        throw new DateTimeException("Too many days: a Period can contain at most " + Integer.MAX_VALUE + " days.");
      }
      return Period.ofYears(yearsAsInt).withMonths(monthsAsInt).withDays(daysAsInt);
    }
    throw new DateTimeException("Cannot convert this TimePeriod to a Period: is not date based");
  }

  /**
   * Determines if the TimePeriod is date based (i.e., if its milliseconds
   * component is 0).
   *
   * @return {@code true} iff the TimePeriod is date based
   */
  public boolean isDateBased() {
    return millis == 0;
  }

  /**
   * Determines if the TimePeriod is time based (i.e., if its months and days
   * components are 0).
   *
   * @return {@code true} iff the TimePeriod is time based
   */
  public boolean isTimeBased() {
    return months == 0 && days == 0;
  }

  public long getMonths() {
    return months;
  }

  public long getDays() {
    return days;
  }

  public long getMillis() {
    return millis;
  }

  @Override
  public long get(TemporalUnit unit) {
    if (unit == MONTHS) {
      return months;
    } else if (unit == DAYS) {
      return days;
    } else if (unit == MILLIS) {
      return millis;
    } else {
      throw new UnsupportedTemporalTypeException("Unsupported unit: " + unit);
    }
  }

  @Override
  public List<TemporalUnit> getUnits() {
    List<TemporalUnit> units = new ArrayList<>();
    // The zero-checks ensure compatibility with the Java Time classes Period and
    // Duration where possible.
    if (months != 0) {
      units.add(MONTHS);
    }
    if (days != 0) {
      units.add(DAYS);
    }
    if (millis != 0) {
      units.add(MILLIS);
    }
    return unmodifiableList(units);
  }

  @Override
  public Temporal addTo(Temporal temporal) {
    return addTo(temporal, months, days, millis);
  }

  @Override
  public Temporal subtractFrom(Temporal temporal) {
    return addTo(temporal, -months, -days, -millis);
  }

  private Temporal addTo(Temporal temporal, long months, long days, long millis) {
    // The zero-checks ensure we can add a TimePeriod to a Temporal even when it
    // does not support all fields, as long as the unsupported fields are zero.
    if (months != 0) {
      temporal = temporal.plus(months, MONTHS);
    }
    if (days != 0) {
      temporal = temporal.plus(days, DAYS);
    }
    if (millis != 0) {
      temporal = temporal.plus(millis, MILLIS);
    }
    return temporal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimePeriod that = (TimePeriod) o;
    return months == that.months && days == that.days && millis == that.millis;
  }

  @Override
  public int hashCode() {
    return Objects.hash(months, days, millis);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("P");
    if (months != 0) {
      int years = (int) (months / MONTHS_PER_YEAR);
      int monthsLeft = (int) (months % MONTHS_PER_YEAR);
      if (years != 0) {
        buffer.append(years).append("Y");
      }
      if (monthsLeft != 0) {
        buffer.append(monthsLeft).append("M");
      }
    }
    if (days != 0 || (months == 0 && millis == 0)) {
      buffer.append(days);
    }
    if (millis != 0) {
      long millisLeft = millis;
      int hours = (int) (millisLeft / MILLIS_PER_HOUR);
      millisLeft -= MILLIS_PER_HOUR * hours;
      int minutes = (int) (millisLeft / MILLIS_PER_MINUTE);
      millisLeft -= MILLIS_PER_MINUTE * minutes;
      int seconds = (int) (millisLeft / MILLIS_PER_SECOND);
      millisLeft -= MILLIS_PER_SECOND * seconds;
      if (millisLeft != 0) {
        buffer.append(String.format("T%02d:%02d:%02d.%03d", hours, minutes, seconds, millisLeft));
      } else if (seconds != 0) {
        buffer.append(String.format("T%02d:%02d:%02d", hours, minutes, seconds));
      } else if (minutes != 0) {
        buffer.append(String.format("T%02d:%02d", hours, minutes));
      } else {
        buffer.append(String.format("T%02d", hours));
      }
    }
    return buffer.toString();
  }
}
