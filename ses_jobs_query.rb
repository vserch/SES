class Job
	attr_accessor :id,:title,:context,:date
	def initialize(id)
		@id = id
	end
end

class JobHash < Hash

	def find_by_id(doc_id,id)
		self[doc_id.to_s] ||= Job.new(id.to_i)
	end
	
	def parse_job(doc_id,id,date,job,context)
		sub = find_by_id(doc_id,id)
		sub.title = job
		sub.date = date
		sub.context = context
	end
end

begin
require "database_connection_tool" # not real
all = JobHash.new
jobs = []
CSV.foreach("job_list.csv") do |line|
	jobs << line[0].downcase
end
exclusions = []
CSV.foreach("exclude_list.csv") do |line|
	exclusions << line[0]
end
prefixes = []
CSV.foreach("prefix_list.csv") do |line|
	prefixes << line[0].downcase
end
postfixes = ["for"]

jobs.each_with_index do |job,i|
	puts "Running for #{job} (#{i.to_s}/#{jobs.size})"
	session = DatabaseConnnection.session
	results = session.run_sql("SELECT subject.id, document.date, document.text, document.id as doc_id
	FROM 
		subject
		inner join
		document
		on (subject.id = document.subject_id)
	WHERE
		downcase(document.text) like '%#{job}%'
	")
	while results.next
		doc_id = rs.get_string("DOC_ID")
		id = rs.get_string("ID")
		text = rs.get_string("TEXT")
		date = rs.get_string("DATE")
		text = text.gsub(/\r\n/,"\n").gsub(/\n/," ")
		context = text.scan(/.{0,20}#{job}.{0,20}/).first
		next if context.nil?
		if exclusions.any? { |e| context.include?(e) }
			next
		end
		if prefixes.any? { |e| context.include?(e + " " + job)} or context.include?(job + " for")
			all.parse_job(doc_id,id,date,job,context)
		end
	end
end

CSV.open("ses_jobs_output.csv","w") do |csv|
	csv << ["id","date","job title","context"]
	all.each do |_,sub|
		csv << [sub.id,sub.date,sub.title,sub.context]
	end
end

ensure
results.close rescue nil
session.close rescue nil
end
