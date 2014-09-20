include Java
import org.apache.hadoop.hbase. HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

htable = HTable.new(HBaseConfiguration.new, ".META.")
scanner = htable.getScanner(Scan.new())
tables = {}
scanner.each do |region_rowkey| 
    table_name = Bytes.toString(region_rowkey.getRow).split(",")[0]
    if not tables.has_key?(table_name) 
        tables[table_name] = 0 
    end
    tables[table_name] = tables[table_name] + 1
end
tables.keys.each { |t| puts "Table #{t} has #{tables[t]} regions"}
