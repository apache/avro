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

import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.chrono.IsoChronology;
import java.time.chrono.JapaneseChronology;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.ERAS;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimePeriodTest {
  // This Long is too large to fit into an unsigned int.
  private static final long TOO_LARGE = Integer.MAX_VALUE * 3L;

  @Test
  void validateConstruction() {
    TimePeriod timePeriod = TimePeriod.of(12, 34, 56);
    assertSame(timePeriod, TimePeriod.from(timePeriod));
    assertComponents(12, 34, 56, timePeriod);

    assertComponents(14, 3, 0, TimePeriod.from(IsoChronology.INSTANCE.period(1, 2, 3)));

    assertComponents(36_000, 0, 0, TimePeriod.from(TimeAmount.of(ChronoUnit.MILLENNIA, 3)));
    assertComponents(3_600, 0, 0, TimePeriod.from(TimeAmount.of(ChronoUnit.CENTURIES, 3)));
    assertComponents(360, 0, 0, TimePeriod.from(TimeAmount.of(ChronoUnit.DECADES, 3)));
    assertComponents(36, 0, 0, TimePeriod.from(TimeAmount.of(ChronoUnit.YEARS, 3)));
    assertComponents(3, 0, 0, TimePeriod.from(TimeAmount.of(MONTHS, 3)));

    assertComponents(0, 21, 0, TimePeriod.from(TimeAmount.of(ChronoUnit.WEEKS, 3)));
    assertComponents(0, 3, 0, TimePeriod.from(TimeAmount.of(DAYS, 3)));
    assertComponents(0, 2, 0, TimePeriod.from(TimeAmount.of(ChronoUnit.HALF_DAYS, 4)));
    assertComponents(0, 2, 43_200_000, TimePeriod.from(TimeAmount.of(ChronoUnit.HALF_DAYS, 5)));

    assertComponents(0, 0, 10_800_000, TimePeriod.from(TimeAmount.of(ChronoUnit.HOURS, 3)));
    assertComponents(0, 0, 180_000, TimePeriod.from(TimeAmount.of(ChronoUnit.MINUTES, 3)));
    assertComponents(0, 0, 3_000, TimePeriod.from(TimeAmount.of(ChronoUnit.SECONDS, 3)));
    assertComponents(0, 0, 3, TimePeriod.from(TimeAmount.of(MILLIS, 3)));
    assertComponents(0, 0, 3, TimePeriod.from(TimeAmount.of(MICROS, 3_000)));
    assertComponents(0, 0, 3, TimePeriod.from(TimeAmount.of(NANOS, 3_000_000)));

    // Micros and nanos must be a multiple of milliseconds
    assertThrows(DateTimeException.class, () -> TimePeriod.from(TimeAmount.of(ChronoUnit.MICROS, 3)));
    assertThrows(DateTimeException.class, () -> TimePeriod.from(TimeAmount.of(ChronoUnit.NANOS, 3)));
    // Unsupported cases (null, non-ISO chronology, unknown temporal unit,
    // non-ChronoUnit)
    assertThrows(NullPointerException.class, () -> TimePeriod.from(null));
    assertThrows(DateTimeException.class, () -> TimePeriod.from(JapaneseChronology.INSTANCE.period(1, 2, 3)));
    assertThrows(UnsupportedTemporalTypeException.class, () -> TimePeriod.from(TimeAmount.of(ChronoUnit.ERAS, 1)));
    assertThrows(UnsupportedTemporalTypeException.class, () -> TimePeriod.from(TimeAmount.of(DummyUnit.INSTANCE, 3)));
    // Arguments are long, but must fit an unsigned long
    assertThrows(ArithmeticException.class, () -> TimePeriod.of(TOO_LARGE, 0, 0));
    assertThrows(ArithmeticException.class, () -> TimePeriod.of(0, TOO_LARGE, 0));
    assertThrows(ArithmeticException.class, () -> TimePeriod.of(0, 0, TOO_LARGE));

    // Odd one out: querying an unsupported temporal unit
    // (assertComponents handles all valid cases)
    assertThrows(UnsupportedTemporalTypeException.class, () -> TimePeriod.of(1, 1, 1).get(ERAS));
  }

  @Test
  void checkConversionsFromJavaTime() {
    assertEquals(TimePeriod.of(12, 0, 0), TimePeriod.from(Period.ofYears(1)));
    assertEquals(TimePeriod.of(2, 0, 0), TimePeriod.from(Period.ofMonths(2)));
    assertEquals(TimePeriod.of(0, 21, 0), TimePeriod.from(Period.ofWeeks(3)));
    assertEquals(TimePeriod.of(0, 4, 0), TimePeriod.from(Period.ofDays(4)));

    assertEquals(TimePeriod.of(0, 0, 1), TimePeriod.from(Duration.ofNanos(1_000_000)));
    assertEquals(TimePeriod.of(0, 0, 2), TimePeriod.from(Duration.ofMillis(2)));
    assertEquals(TimePeriod.of(0, 0, 3_000), TimePeriod.from(Duration.ofSeconds(3)));
    assertEquals(TimePeriod.of(0, 0, 240000), TimePeriod.from(Duration.ofMinutes(4)));
    assertEquals(TimePeriod.of(0, 0, 18000000), TimePeriod.from(Duration.ofHours(5)));
    // Duration never takes into account things like daylight saving
    assertEquals(TimePeriod.of(0, 0, 518400000), TimePeriod.from(Duration.ofDays(6)));
  }

  @Test
  void checkConversionsToJavaTime() {
    TimePeriod months = TimePeriod.of(1, 0, 0);
    TimePeriod days = TimePeriod.of(0, 2, 0);
    TimePeriod time = TimePeriod.of(0, 0, 3);
    TimePeriod all = TimePeriod.of(1, 2, 3);

    assertTrue(months.isDateBased());
    assertTrue(days.isDateBased());
    assertFalse(all.isDateBased());
    assertFalse(time.isDateBased());

    assertEquals(Period.of(0, 1, 0), months.toPeriod());
    assertEquals(Period.of(0, 0, 2), days.toPeriod());
    assertThrows(DateTimeException.class, all::toPeriod);
    assertThrows(DateTimeException.class, time::toPeriod);

    assertThrows(DateTimeException.class, () -> TimePeriod.of(0, Integer.MAX_VALUE * 2L, 0).toPeriod());

    assertFalse(months.isTimeBased());
    assertFalse(days.isTimeBased());
    assertFalse(all.isTimeBased());
    assertTrue(time.isTimeBased());

    assertThrows(DateTimeException.class, months::toDuration);
    // Note: though Duration supports this, it uses a fixed 86400 seconds
    assertEquals(Duration.ofSeconds(172800), days.toDuration());
    assertThrows(DateTimeException.class, all::toDuration);
    assertEquals(Duration.ofMillis(3), time.toDuration());
  }

  @Test
  void checkAddingToTemporalItems() {
    TimePeriod monthAndTwoDays = TimePeriod.of(1, 2, 0);
    TimePeriod threeMillis = TimePeriod.of(0, 0, 3);
    TimePeriod complexTimePeriod = TimePeriod.of(1, 2, 3);

    LocalDateTime localDateTime = LocalDateTime.of(2001, 2, 3, 4, 5, 6, 7_000_000);
    LocalDate localDate = LocalDate.of(2001, 2, 3);
    LocalTime localTime = LocalTime.of(4, 5, 6, 7_000_000);

    assertEquals(localDateTime.plusMonths(1).plusDays(2), localDateTime.plus(monthAndTwoDays));
    assertEquals(localDateTime.plus(3, MILLIS), localDateTime.plus(threeMillis));
    assertEquals(localDateTime.plusMonths(1).plusDays(2).plus(3, MILLIS), localDateTime.plus(complexTimePeriod));

    assertEquals(localDate.plusMonths(1).plusDays(2), localDate.plus(monthAndTwoDays));

    assertEquals(localTime.plus(3, MILLIS), localTime.plus(threeMillis));

    assertEquals(localDateTime.minusMonths(1).minusDays(2), localDateTime.minus(monthAndTwoDays));
    assertEquals(localDateTime.minus(3, MILLIS), localDateTime.minus(threeMillis));
    assertEquals(localDateTime.minusMonths(1).minusDays(2).minus(3, MILLIS), localDateTime.minus(complexTimePeriod));

    assertEquals(localDate.minusMonths(1).minusDays(2), localDate.minus(monthAndTwoDays));

    assertEquals(localTime.minus(3, MILLIS), localTime.minus(threeMillis));
  }

  @Test
  void checkEqualityTests() {
    TimePeriod timePeriod1a = TimePeriod.of(1, 2, 3);
    TimePeriod timePeriod1b = TimePeriod.of(1, 2, 3);
    TimePeriod timePeriod2 = TimePeriod.of(9, 9, 9);
    TimePeriod timePeriod3 = TimePeriod.of(1, 9, 9);
    TimePeriod timePeriod4 = TimePeriod.of(1, 2, 9);

    // noinspection EqualsWithItself
    assertEquals(timePeriod1a, timePeriod1a);
    assertEquals(timePeriod1a, timePeriod1b);
    assertEquals(timePeriod1a.hashCode(), timePeriod1b.hashCode());

    assertNotEquals(timePeriod1a, null);
    // noinspection AssertBetweenInconvertibleTypes
    assertNotEquals(timePeriod1a, "not equal");
    assertNotEquals(timePeriod1a, timePeriod2);
    assertNotEquals(timePeriod1a.hashCode(), timePeriod2.hashCode());
    assertNotEquals(timePeriod1a, timePeriod3);
    assertNotEquals(timePeriod1a.hashCode(), timePeriod3.hashCode());
    assertNotEquals(timePeriod1a, timePeriod4);
    assertNotEquals(timePeriod1a.hashCode(), timePeriod4.hashCode());
  }

  @Test
  void checkStringRepresentation() {
    assertEquals("P0", TimePeriod.of(0, 0, 0).toString());
    assertEquals("P1Y", TimePeriod.of(12, 0, 0).toString());
    assertEquals("P2M", TimePeriod.of(2, 0, 0).toString());
    assertEquals("P3", TimePeriod.of(0, 3, 0).toString());
    assertEquals("P1Y2M3", TimePeriod.of(14, 3, 0).toString());
    assertEquals("PT04", TimePeriod.of(0, 0, 14400000).toString());
    assertEquals("PT00:05", TimePeriod.of(0, 0, 300000).toString());
    assertEquals("PT00:00:06", TimePeriod.of(0, 0, 6000).toString());
    assertEquals("PT00:00:00.007", TimePeriod.of(0, 0, 7).toString());
    assertEquals("P1Y2M3T04:05:06.007", TimePeriod.of(14, 3, 14706007).toString());

    // Days and millis will never overflow to months/days, to respect differences
    // in months and days (daylight saving).
    assertEquals("P123T1193:02:47.295", TimePeriod.of(0, 123, 4294967295L).toString());
  }

  private void assertComponents(long months, long days, long millis, TimePeriod timePeriod) {
    List<TemporalUnit> expectedUnits = new ArrayList<>(Arrays.asList(MONTHS, DAYS, MILLIS));
    if (months == 0) {
      expectedUnits.remove(MONTHS);
    }
    if (days == 0) {
      expectedUnits.remove(DAYS);
    }
    if (millis == 0) {
      expectedUnits.remove(MILLIS);
    }
    assertEquals(expectedUnits, timePeriod.getUnits());

    assertEquals(months, timePeriod.getMonths());
    assertEquals(months, timePeriod.get(MONTHS));
    assertEquals(days, timePeriod.getDays());
    assertEquals(days, timePeriod.get(DAYS));
    assertEquals(millis, timePeriod.getMillis());
    assertEquals(millis, timePeriod.get(MILLIS));
  }

  private static class TimeAmount implements TemporalAmount {
    private final Map<TemporalUnit, Long> amountsPerUnit = new LinkedHashMap<>();

    static TimeAmount of(TemporalUnit unit, long amount) {
      return new TimeAmount().with(unit, amount);
    }

    TimeAmount with(TemporalUnit unit, long amount) {
      amountsPerUnit.put(unit, amount);
      return this;
    }

    @Override
    public long get(TemporalUnit unit) {
      return amountsPerUnit.get(unit);
    }

    @Override
    public List<TemporalUnit> getUnits() {
      return new ArrayList<>(amountsPerUnit.keySet());
    }

    @Override
    public Temporal addTo(Temporal temporal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Temporal subtractFrom(Temporal temporal) {
      throw new UnsupportedOperationException();
    }
  }

  private static class DummyUnit implements TemporalUnit {
    private static final DummyUnit INSTANCE = new DummyUnit();

    @Override
    public Duration getDuration() {
      return null;
    }

    @Override
    public boolean isDurationEstimated() {
      return false;
    }

    @Override
    public boolean isDateBased() {
      return false;
    }

    @Override
    public boolean isTimeBased() {
      return false;
    }

    @Override
    public <R extends Temporal> R addTo(R temporal, long amount) {
      return null;
    }

    @Override
    public long between(Temporal temporal1Inclusive, Temporal temporal2Exclusive) {
      return 0;
    }
  }
}
