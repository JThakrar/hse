include Java

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HTablePool
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class HBaseShellExtension
  # Sample class to show the power of jruby under HBase shell.
  # Class methods allow reading a row and making guess about the data type of each column for display.
  #
  # Sample Interactive Usage
  # -------------------------
  # hse = HBaseShellExtension.new
  # hse.read_row?("sample_table", "rowkey")
  # hse.print_row
  # hse.print_column("column_family", "column_name")
  # Bytes.toString(hse.raw_column("column_family", "column_name"))
  #
  # Sample Batch Usage
  # -------------------
  # source 'hbase_shell_extension.rb'
  # hse = HBaseShellExtension.new
  # ARGF.each_line do |line|
  #   parts = line.split()
  #   table_name = parts[0]
  #   rowkey = parts[1]
  #   if hse.read_row?(table_name, rowkey)
  #      hse.print_row
  #   end
  # end

  # Exposing class attributes allows "visibility" outside the class.
  attr_reader :htable_pool
  attr_reader :htable
  attr_reader :result
  attr_reader :get
  attr_reader :columns
  attr_reader :table_name
  attr_reader :rowkey
  Struct.new("Column", :column_family, :column_qualifier, :timestamp, :column_type, :value_bytearray, :value_translated)

  def initialize()
    # Initialization is very simple - just create a new HTablePool
    @htable_pool = HTablePool.new
    nil
  end

  def translate_value(column_value_bytes)
    # Given the "byte array" of a column, the method guesses its data type and returns it appropriately translated/converted.
    # The guess is made based on the data type sizes in org.apache.hadoop.hbase.util.Bytes
    if column_value_bytes == nil
      return nil
    end
    column_size = column_value_bytes.length
    case column_size
    when Bytes::SIZEOF_BOOLEAN
      column_translation = Bytes.toBoolean(column_value_bytes)
      column_type = "BOOLEAN"
    when Bytes::SIZEOF_BYTE
      column_translation = Bytes.toByte(column_value_bytes)
      column_type = "BYTE"
    when Bytes::SIZEOF_SHORT
      column_translation = Bytes.toShort(column_value_bytes)
      column_type = "SHORT"
    when Bytes::SIZEOF_INT
      column_translation = Bytes.toInt(column_value_bytes)
      column_type = "INTEGER"
    when Bytes::SIZEOF_LONG
      column_translation = Bytes.toLong(column_value_bytes)
      column_type = "LONG"
    when Bytes::SIZEOF_DOUBLE
      column_translation = Bytes.toDouble(column_value_bytes)
      column_type = "DOUBLE"
    when Bytes::SIZEOF_FLOAT
      column_translation = Bytes.toFloat(column_value_bytes)
      column_type = "FLOAT"
    else
      column_translation = Bytes.toString(column_value_bytes)
      column_type = "STRING"
    end
    return [column_type, column_translation]
  end

  def read_row?(table_name, rowkey)
    # Read a row from a specified table. This method has been made "boolean" to allow
    # its use in "if" conditions.
    return false if table_name == nil or rowkey == nil
    @table_name = table_name
    @htable = @htable_pool.getTable(@table_name)
    @rowkey = rowkey
    @get = Get.new(Bytes.toBytes(@rowkey))
    @get.setMaxVersions(1) # Retrieve only the latest version of each column.
    @result = @htable.get(@get)
    if result.list().length <= 1 # Row not found
      return false
    end
    @columns = Hash.new
    @result.list().each do |key_value|
      column_family = Bytes.toString(key_value.getFamily())
      column_qualifier = Bytes.toString(key_value.getQualifier())
      column_value = key_value.getValue()
      column_timestamp = key_value.getTimestamp()
      translation = translate_value(column_value)
      column_type = translation[0]
      column_translation = translation[1]
      column = Struct::Column.new(column_family, column_qualifier, column_timestamp, column_type, column_value, column_translation)
      column_key = column_family + ":" + column_qualifier
      @columns[column_key] = column
    end
    return true
  end

  def print_column(column_family, column_qualifier)
    column_key = column_family + ":" + column_qualifier
    if column_family == nil or column_qualifier == nil
      puts "#{column_key}: nil"
      return nil
    end
    column_value = @columns.has_key?(column_key) ? @columns[column_key].value_translated : nil
    puts "#{column_key} = #{column_value ? column_value : "nil"}"
  end

  def raw_column(column_family, column_qualifier)
    column_key = column_family + ":" + column_qualifier
    if column_family == nil or column_qualifier == nil
      return nil
    end
    column_value = @columns.has_key?(column_key) ? @columns[column_key].value_bytearray : nil
  end

  def print_row()
    puts "Table = #{@table_name}"
    puts "Rowkey = #{@rowkey}"
    @columns.each do |key, value|
      puts "#{key} (#{value.column_type.to_s}) = #{value.value_translated.to_s}"
    end
    nil
  end

end
