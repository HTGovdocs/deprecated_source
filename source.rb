require 'securerandom'
require 'json'
require 'digest/md5'
require 'htph'
require 'fileutils'

module Source 

  @@jdbc = HTPH::Hathijdbc::Jdbc.new()
  @@conn = @@jdbc.get_conn()
  @@add_rec = "INSERT INTO source_refs ( id, file_path, line_number )
              VALUES(?, ?, ?)"
  @@base_path = '/htdata/govdocs/source_records/'

  def self.get_path uuid
    uuid[0..3].split(//).join('/')
  end

  def self.add_md5 rec
    #if it already has one, we only want to hash everything else
    rec = rec.tap{|x| x.delete(:md5_hash)}   
    rec[:md5_hash] = Digest::MD5.hexdigest(rec.to_s)
    return rec
  end

  def self.save_source_reference id, file_name, line_number
    #add the new id to the database so we can track
    @@conn.prepared_update(@@add_rec, [id, file_name, line_number])
  rescue
    @@conn = @@jdbc.get_conn()
    retry 
  end  

  def self.put_rec rec
    path = @@base_path + (self.get_path rec[:id])
    #create the dir and subs
    unless File.directory?(path)
      FileUtils.mkdir_p(path)
    end

    File.open(path+'/'+rec[:id]+'.json', "w"){ |fout| fout.puts rec.to_json }
  end

end
  
if __FILE__ == $0
  @@count = 0
  @@start = Time.now

  open(ARGV.shift, 'r').each do | file_path | 
    file_path.chomp! 
    open(file_path, 'r').each_with_index do | line, line_number | 
      rec = JSON.parse line
      rec[:id] = SecureRandom.uuid 
      rec = Source.add_md5 rec
      begin 
        Source.put_rec rec
	Source.save_source_reference rec[:id], file_path, line_number
        @@count += 1
      end
    end
    puts @@count
    puts file_path
    puts "duration: "+(Time.now - @@start).to_s
  end
end

#line = File.open('/htdata/govdocs/MARC/extracted_json_fixed/arizona20140207.ndj', &:readline)
#puts line
#puts Digest::MD5.hexdigest(line)
#rec = JSON.parse line
#rec[:id] = id
#puts Digest::MD5.hexdigest(rec.to_s)
#rec[:md5_hash] = Digest::MD5.hexdigest(rec.to_s)

#puts rec
#recless = rec.clone.tap{|x| x.delete(:md5_hash)}
#puts rec.to_json
#puts recless.to_json
#puts Digest::MD5.hexdigest(recless.to_s)



