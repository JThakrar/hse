include Java
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
htable = HTable.new(HBaseConfiguration.new, "sample")
rowkey = Bytes.toBytes("some_rowkey")
get = Get.new(rowkey)
result = htable.get(get)
result.list.collect {|kv|  puts "#{Bytes.toString(kv.getFamily)}:#{Bytes.toString(kv.getQualifier)}"}
