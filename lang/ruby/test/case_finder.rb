class CaseFinder
  PATH = File.expand_path("../../../../share/test/data/schema-tests.txt", __FILE__)

  Case = Struct.new(:id, :input, :canonical, :fingerprint)

  def self.cases
    new.cases
  end

  def initialize
    @scanner = StringScanner.new(File.read(PATH))
    @cases = []
  end

  def cases
    until @scanner.eos?
      test_case = scan_case
      @cases << test_case if test_case
    end

    @cases
  end

  private

  def scan_case
    if id = @scanner.scan(/\/\/ \d+\n/)
      while @scanner.skip(/\/\/ .*\n/); end

      input = scan_input
      canonical = scan_canonical
      fingerprint = scan_fingerprint

      Case.new(id, input, canonical, fingerprint)
    else
      @scanner.skip(/.*\n/)
      nil
    end
  end

  def scan_item(name)
    if @scanner.scan(/<<#{name}\n/)
      lines = []
      while line = @scanner.scan(/.+\n/)
        break if line.chomp == name
        lines << line
      end
      lines.join
    elsif @scanner.scan(/<<#{name} /)
      input = @scanner.scan(/.+$/)
      @scanner.skip(/\n/)
      input
    end
  end

  def scan_input
    scan_item("INPUT")
  end

  def scan_canonical
    scan_item("canonical")
  end

  def scan_fingerprint
    scan_item("fingerprint")
  end
end
