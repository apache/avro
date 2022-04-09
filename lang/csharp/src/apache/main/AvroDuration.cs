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

using System;

namespace Avro
{
    /// <summary>
    /// Represents a duration.
    /// </summary>
    public struct AvroDuration : IComparable, IComparable<AvroDuration>, IEquatable<AvroDuration>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDuration" /> struct.
        /// </summary>
        /// <param name="months">Number of months.</param>
        /// <param name="days">Number of days.</param>
        /// <param name="milliseconds">Number of milliseconds.</param>
        public AvroDuration(int months, int days, int milliseconds)
        {
            Months = months;
            Days = days;
            Milliseconds = milliseconds;
        }

        /// <summary>
        /// Gets/sets the number of months represented by the current <see cref="AvroDuration" />.
        /// </summary>
        /// <value>
        /// Number of months.
        /// </value>
        public int Months { get; set; }

        /// <summary>
        /// Gets/sets the number of days represented by the current <see cref="AvroDuration" />.
        /// </summary>
        /// <value>
        /// Number of days.
        /// </value>
        public int Days { get; set; }

        /// <summary>
        /// Gets/sets the number of milliseconds represented by the current <see cref="AvroDuration" />.
        /// </summary>
        /// <value>
        /// Number of milliseconds.
        /// </value>
        public int Milliseconds { get; set; }

        /// <summary>
        /// Converts the current <see cref="AvroDuration" /> to a string.
        /// </summary>
        /// <returns>
        /// A string representation of the duration value.
        /// </returns>
        public override string ToString()
        {
            return $"(M{Months},D{Days},MS{Milliseconds})";
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator ==(AvroDuration left, AvroDuration right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator !=(AvroDuration left, AvroDuration right)
        {
            return !left.Equals(right);
        }

        /// <summary>
        /// Implements the operator &gt;.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator >(AvroDuration left, AvroDuration right)
        {
            return left.CompareTo(right) > 0;
        }

        /// <summary>
        /// Implements the operator &gt;=.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator >=(AvroDuration left, AvroDuration right)
        {
            return left.CompareTo(right) >= 0;
        }

        /// <summary>
        /// Implements the operator &lt;.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator <(AvroDuration left, AvroDuration right)
        {
            return left.CompareTo(right) < 0;
        }

        /// <summary>
        /// Implements the operator &lt;=.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator <=(AvroDuration left, AvroDuration right)
        {
            return left.CompareTo(right) <= 0;
        }

        /// <summary>
        /// Returns a value that indicates whether the current <see cref="AvroDuration" /> and a specified object
        /// have the same value.
        /// </summary>
        /// <param name="obj">The object to compare.</param>
        /// <returns>
        /// true if the obj argument is an <see cref="AvroDuration" /> object, and its value
        /// is equal to the value of the current <see cref="AvroDuration" /> instance; otherwise false.
        /// </returns>
        public override bool Equals(object obj)
        {
            return (obj is AvroDuration duration) && Equals(duration);
        }

        /// <summary>
        /// Returns the hash code for the current <see cref="AvroDuration" />.
        /// </summary>
        /// <returns>
        /// The hash code.
        /// </returns>
        public override int GetHashCode()
        {
            return Months ^ Days ^ Milliseconds;
        }

        /// <summary>
        /// Compares the value of the current <see cref="AvroDuration" /> to the value of another object.
        /// </summary>
        /// <param name="obj">The object to compare.</param>
        /// <returns>
        /// A value that indicates the relative order of the objects being compared.
        /// </returns>
        public int CompareTo(object obj)
        {
            if (obj == null)
            {
                return 1;
            }

            if (obj is AvroDuration duration)
            {
                return CompareTo(duration);
            }

            return 1;
        }

        /// <summary>
        /// Compares the value of the current <see cref="AvroDuration" /> to the value of another
        /// <see cref="AvroDuration" />.
        /// </summary>
        /// <param name="other">The <see cref="AvroDuration" /> to compare.</param>
        /// <returns>
        /// A value that indicates the relative order of the <see cref="AvroDuration" />
        /// instances being compared.
        /// </returns>
        public int CompareTo(AvroDuration other)
        {
            return TotalMilliseconds.CompareTo(other.TotalMilliseconds);
        }

        // This is not a perfect total milliseconds value, because it assumes that every month has 30 days.
        private long TotalMilliseconds => (Months * 30L + Days) * 24L * 3600000L + Milliseconds;

        /// <summary>
        /// Returns a value that indicates whether the current <see cref="AvroDuration" /> has the same
        /// value as another <see cref="AvroDuration" />.
        /// </summary>
        /// <param name="other">The <see cref="AvroDuration" /> to compare.</param>
        /// <returns>
        /// true if the current <see cref="AvroDuration" /> has the same value as <paramref name="other" />;
        /// otherwise false.
        /// </returns>
        public bool Equals(AvroDuration other)
        {
            return Months == other.Months && Days == other.Days && Milliseconds == other.Milliseconds;
        }
    }
}
