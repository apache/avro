# frozen_string_literal: true
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_help'

# A block with a very high compression ratio can expand to far more memory than
# its compressed size. These tests ensure that decompressing such a block raises
# an error instead of allocating without bound.
class TestCodecLimits < Test::Unit::TestCase
  LIMIT = 1024

  def with_limit(bytes)
    previous = ENV['AVRO_MAX_DECOMPRESS_LENGTH']
    ENV['AVRO_MAX_DECOMPRESS_LENGTH'] = bytes.to_s
    yield
  ensure
    if previous.nil?
      ENV.delete('AVRO_MAX_DECOMPRESS_LENGTH')
    else
      ENV['AVRO_MAX_DECOMPRESS_LENGTH'] = previous
    end
  end

  def oversized_payload
    ("\x00".b * (LIMIT * 8))
  end

  def test_deflate_rejects_oversized_block
    codec = Avro::DataFile::DeflateCodec.new
    compressed = codec.compress(oversized_payload)
    with_limit(LIMIT) do
      assert_raise(Avro::DataFile::DecompressionSizeError) do
        codec.decompress(compressed)
      end
    end
  end

  def test_deflate_allows_block_within_limit
    codec = Avro::DataFile::DeflateCodec.new
    payload = ('the quick brown fox ' * 10).b
    compressed = codec.compress(payload)
    with_limit(LIMIT) do
      assert_equal payload, codec.decompress(compressed)
    end
  end

  def test_snappy_rejects_declared_oversized_length
    begin
      require 'snappy'
    rescue LoadError
      omit('snappy gem not available')
    end
    codec = Avro::DataFile::SnappyCodec.new
    compressed = codec.compress(oversized_payload)
    with_limit(LIMIT) do
      assert_raise(Avro::DataFile::DecompressionSizeError) do
        codec.decompress(compressed)
      end
    end
  end

  def test_snappy_rejects_unparseable_length_header
    begin
      require 'snappy'
    rescue LoadError
      omit('snappy gem not available')
    end
    codec = Avro::DataFile::SnappyCodec.new
    # A header made entirely of continuation bytes never terminates, so the
    # declared length cannot be parsed; the codec must fail closed rather than
    # hand the block to the decompressor with the guard bypassed.
    malformed = ("\x80".b * 12)
    with_limit(LIMIT) do
      assert_raise(Avro::DataFile::DataFileError) do
        codec.decompress(malformed)
      end
    end
  end

  def test_zstandard_rejects_oversized_block
    begin
      require 'zstd-ruby'
    rescue LoadError
      omit('zstd-ruby gem not available')
    end
    codec = Avro::DataFile::ZstandardCodec.new
    compressed = codec.compress(oversized_payload)
    with_limit(LIMIT) do
      assert_raise(Avro::DataFile::DecompressionSizeError) do
        codec.decompress(compressed)
      end
    end
  end

  def test_zstandard_within_limit_round_trips
    begin
      require 'zstd-ruby'
    rescue LoadError
      omit('zstd-ruby gem not available')
    end
    codec = Avro::DataFile::ZstandardCodec.new
    payload = ('the quick brown fox ' * 10).b # well under the limit
    compressed = codec.compress(payload)
    with_limit(LIMIT) do
      assert_equal payload, codec.decompress(compressed)
    end
  end
end
