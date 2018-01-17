require "database_connection_tool" # not real

EducationMatch = Struct.new(:level,:matched,:date)

class Subject

	def initialize(id)
		@id = id
		@education_matches = []
		@unemployment_dates = []
		@retired_dates = []
		@uninsured_dates = []
		@medicaid_dates = []
		@homeless_dates = []
	end

	def parse_education(level,matched,date)
		@education_matches << EducationMatch.new(level,matched,date)
	end

	def parse_unemployment(date)
		@unemployment_dates << date
	end

	def parse_retired(date)
		@retired_dates << date
	end

	def parse_uninsured(date)
		@uninsured_dates << date
	end

	def parse_medicaid(date)
		@medicaid_dates << date
	end

	def parse_homeless(date)
		@homeless_dates << date
	end

	def education_columns
		highest_level = @education_matches.map(&:level).max
		selected = @education_matches.select { |match| match.level == highest_level }
		latest = selected.sort_by(&:date).last
		if latest.nil?
			[nil,nil,nil]
		else
			[latest.level,latest.matched,latest.date]
		end
	end

	def unemployment_columns
		if @unemployment_dates.empty?
			[0,nil]
		else
			[1,@unemployment_dates.sort.last]
		end
	end

	def retired_columns
		if @retired_dates.empty?
			[0,nil]
		else
			[1,@retired_dates.sort.last]
		end
	end

	def insurance_columns
		if @uninsured_dates.empty?
			[0,nil]
		else
			[1,@uninsured_dates.sort.last]
		end
	end

	def medicaid_columns
		if @medicaid_dates.empty?
			[0,nil]
		else
			[1,@medicaid_dates.sort.last]
		end
	end

	def homeless_columns
		if @homeless_dates.empty?
			[0,nil]
		else
			[1,@homeless_dates.sort.last]
		end
	end

	def row
		[@id] + education_columns + unemployment_columns + retired_columns + insurance_columns + medicaid_columns + homeless_columns
	end

end

class SubjectHash < Hash

	def find_by_id(id)
		self[id.to_i] ||= Subject.new(id.to_i)
	end

	def parse_education(id,level,matched,date)
		sub = find_by_id(id)
		sub.parse_education(level,matched,date)
	end

	def parse_unemployment(id,date)
		sub = find_by_id(id)
		sub.parse_unemployment(date)
	end

	def parse_retired(id,date)
		sub = find_by_id(id)
		sub.parse_retired(date)
	end

	def parse_uninsured(id,date)
		sub = find_by_id(id)
		sub.parse_uninsured(date)
	end

	def parse_medicaid(id,date)
		sub = find_by_id(id)
		sub.parse_medicaid(date)
	end

	def parse_homeless(id,date)
		sub = find_by_id(id)
		sub.parse_homeless(date)
	end

end

all = SubjectHash.new

DatabaseConnection::get_subjects.results do |id|
	all.find_by_id(id)
end

puts "Education..."
education = Hash.new { |h,k| h[k] = [] }

education[0] = ["did not attend school","no education"]
education[1] = ["kindergarten","1st grade","2nd grade","3rd grade","4th grade","5th grade","6th grade","7th grade","8th grade","9th grade","10th grade","11th grade","12th grade","did not finish/complete high school","completed grade school","last grade attended","1 year of education","2 years of education","3 years of education","4 years of education","5 years of education","6 years of education","7 years of education","8 years of education","9 years of education","10 years of education","11 years of education"]
education[2] = ["high school graduate","completed 12th grade","completed high school","college student","student","freshman","sophomore","junior","senior","going for","hs degree","high school degree","in college","some college","12 years of education","13 years of education","education level hs","gradauted from high school"]
education[3] = ["GED"] #case sensitive
education[4] = ["technical school","associate degree","years of college","14 years of education","15 years of education"]
education[5] = ["medical student","graduate student","graduate\\s*\\w+\\s*student","phd candidate","bs degree","bachelor''s degree","college graduate","ba degree","completed college","degree in","16 years of education","17 years of education","education level coll","nursing school","grad student"]
education[6] = ["master''s degree","18 years of education","18 years of education"]
education[7] = ["JD","law school","Law School","Law school","law School"] #case sensitive
education[8] = ["PhD","MDPhD","MD/PhD","Doctoral degree","EdD"] #case sensitive
education_exclusions = ["daughter or son in college","patient education","family education","diet education","dialysis education","diabetic education","education provided","going for","student health","senior care","liphdl","student nurse","pharmacy student","medical student","], phd"]
education_exclusion_clause = education_exclusions.map { |e| "downcase(document.TEXT) NOT LIKE '%#{e}%' " }.join(" AND ")

education.keys.each do |key|
	if [3,7,8].include? key
		clause = education[key].map { |e| "regular_expression_compare(document.TEXT,'\\b#{e}\\b') " }.join(" OR ")
	else
		clause = education[key].map { |e| "regular_expression_compare(document.TEXT,'\\b#{e}\\b','i') " }.join(" OR ")
	end
	session = DatabaseConnection.session
	results = session.run_sql("
SELECT
	subject.ID,
	document.TEXT,
	document.DATE
FROM
	subject
	inner join
	document
	ON (document.subject_id = subject.id)
WHERE
	(#{clause}) AND
	(#{education_exclusion_clause})
")
	while results.next
		id = results.get_string("ID")
		date = results.get_string("DATE")
		text = results.get_string("TEXT")
		next if date.nil?
		date = Date.strptime(date,'%Y-%m-%d')
		matched = education[key].map(&:downcase).select { |term| !text.downcase.scan(/\b#{term}\b/).empty? }.join("|")
		all.parse_education(id,key,matched,date)
	end
end

puts "Unemployment..."
unemployment_terms = ["unemployed","unemployment","does not work","does not work on disability","does not work disabled","unemploy"]
unemployment_exclusion_terms = ["if this does not work","if that does not work"]
unemployment_clause = unemployment_terms.map { |e| "lower(d.TEXT) LIKE '%#{e}%'" }.join(" OR ")
unemployment_exclusion_clause = unemployment_exclusion_terms.map { |e| "lower(d.TEXT) NOT LIKE '%#{e}%'" }.join(" AND ")
session = DatabaseConnection.session
results = session.run_sql("
SELECT
	subject.ID,
	document.TEXT,
	document.DATE
FROM
	subject
	inner join
	document
	on (subject.id = document.subject_id)
WHERE
	(#{unemployment_clause}) AND
	#{unemployment_exclusion_clause}
")
while results.next
	id = results.get_string("ID")
	content = results.get_string("TEXT")
	date = results.get_string("DATE")
	next if date.nil?
	date = Date.strptime(date,'%Y-%m-%d')
	all.parse_unemployment(id,date)
end

puts "Retired..."
session = DatabaseConnection.session
results = session.run_sql("
SELECT
	subject.ID,
	document.DATE
FROM
	subject
	inner join
	document
	on (subject.id = document.subject_id)
WHERE
	lower(document.TEXT) LIKE '%retired%'
")
while results.next
	id = results.get_string("ID")
	date = results.get_string("DATE")
	next if date.nil?
	date = Date.strptime(date,'%Y-%m-%d')
	all.parse_retired(id,date)
end

puts "Uninsured..."
insurance_terms = ["do not have insurance","no insurance","does not have insurance","no ins","uninsured"]
insurance_clause = insurance_terms.map { |e| "downcase(document.TEXT) LIKE '%#{e}%'" }.join(" OR ")

session = DatabaseConnection.session
results = session.run_sql("
SELECT
	subject.ID,
	document.DATE
FROM
	subject
	inner join
	document
	on (subject.id = document.subject_id)
WHERE
	(#{insurance_clause}) AND
	downcase(document.TEXT) NOT LIKE '%are uninsured, the emergency%'
")
while results.next
	id = results.get_string("ID")
	date = results.get_string("DATE")
	next if date.nil?
	date = Date.strptime(date,'%Y-%m-%d')
	all.parse_uninsured(id,date)
end

puts "Mediciad..."
medicaid_terms = ["medicaid","tenncare","tenn care"]
medicaid_clause = medicaid_terms.map { |e| "downcase(document.TEXT) LIKE '%#{e}%'" }.join(" OR ")
session = DatabaseConnection.session
results = session.run_sql("
SELECT
	subject.ID,
	document.DATE
FROM
	subject
	inner join
	document
	on (subject.id = document.subject_id)
WHERE
	(#{medicaid_clause}) AND
	downcase(document.TEXT) NOT LIKE '%tenn care patients must contact%' AND
	downcase(document.TEXT) NOT LIKE '%tenncare patients must contact%'
")
while results.next
	id = results.get_string("ID")
	date = results.get_string("DATE")
	next if date.nil?
	date = Date.strptime(date,"%Y-%m-%d")
	all.parse_medicaid(id,date)
end

puts "Homeless..."
homeless_terms = ["homeless","shelter"]
homeless_clause = homeless_terms.map { |e| "downcase(document.TEXT) LIKE '%#{e}%'" }.join(" OR ")
homeless_exclusion_terms = ["volunteering at a homeless shelter","works at a homeless shelter","homeless shelter as a volunteer","shelter manager","running a shelter","works with the homeless","animal shelter"]
homeless_exclusion_clause = homeless_exclusion_terms.map { |e| "downcase(document.TEXT) NOT LIKE '%#{e}%'" }.join(" AND ")

session = DatabaseConnection.session
results = session.run_sql("
SELECT
	subject.ID,
	document.DATE
FROM
	subject
	inner join
	document
	on (subject.id = document.subject_id)
WHERE
	(#{homeless_clause}) AND
	(#{homeless_exclusion_clause})
")
while results.next
	id = results.get_string("ID")
	date = results.get_string("DATE")
	next if date.nil?
	date = Date.strptime(date,'%Y-%m-%d')
	all.parse_homeless(id,date)
end

puts "Writing to file..."
CSV.open("education_output.csv","w") do |csv|
	header = ["id","highest_education_level","highest_education_matched_text","highest_education_doc_date","unemployment","unemployment_doc_date","retired","retired_doc_date","uninsured","uninsured_doc_date","mediciad","medicaid_doc_date","homeless","homeless_doc_date"]
	csv << header
	all.values.each do |sub|
		csv << sub.row
	end
end
