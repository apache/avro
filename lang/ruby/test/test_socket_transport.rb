require 'test_help'

class TestSocketTransport < Test::Unit::TestCase
  def test_buffer_writing
    io = StringIO.new
    st = Avro::IPC::SocketTransport.new(io)
    buffer_length = "\000\000\000\006"  # 6 in big-endian
    message = 'abcdef'
    null_ending = "\000\000\000\000" # 0 in big-endian
    full = buffer_length + message + null_ending
    st.write_framed_message('abcdef')
    assert_equal full, io.string
  end

  def test_buffer_reading
    buffer_length = "\000\000\000\005" # 5 in big-endian
    message = "hello"
    null_ending = "\000\000\000\000" # 0 in big-endian
    full = buffer_length + message + null_ending
    io = StringIO.new(full)
    st = Avro::IPC::SocketTransport.new(io)
    assert_equal 'hello', st.read_framed_message
  end
end
