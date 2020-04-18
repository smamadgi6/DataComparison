
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
# Tests outbound fidelity of OSSCR API -> mysql and CRDB

$ProjDir            ||= ((__FILE__[/(.*\/)active\b/, 1] || `pwd`[/(.*\/)active\b/, 1]).untaint or
                         raise "Failed to identify project directory (looking for 'active' in path).");

arguments			= " " + ARGV.join(" ") + " "
$CurrentInst		= ($ProjDir =~ /(\w+)\/?\Z/)      ? $1.untaint : raise("Failed to parse inst name from #{$ProjDir}")
$Env		        = (arguments =~ / --inst (\w+) /) ? $1.untaint : $CurrentInst == "offlinedev" ? "dev" : $CurrentInst
$Tier        		= (arguments =~ / --tier (\w+) /) ? $1.untaint : $Env ## Default to tier == env e.g. prod.prod, test.test, dev.dev, offlinedev.offlinedev

# For testing in Test environment
$Env = "test"
$Tier = "test"

require $ProjDir + "active/lib_local/ruby/osscr_ruby_environment.rb"
require $ProjDir + "active/lib_local/ruby/Trading_Diff_Watchdog.rb"
require 'pp'
require 'uri'
puts "testing transactions in inst=#{$Env} / tier=#{$Tier}"

raise "Cowardly refusing to run in prod.prod environment -- this is a test that actually changes data." if $Env == "prod" && $Tier == "prod"

# Database connections -- connection code is in db_functions.rb
$mysqldbh			= getDBConnection(:OSSCR_RO)	# ruby DBI interface to mysql (using osscr_readonly user)
$oracledbh			= getDBConnection(:Oracle_DBI)	# ruby DBI interface (DBI:OCI8) to Oracle

# DEBUG: Test the db connections
#pp mysqldbh.select_all(%Q{show tables like 'CPG_XCDE%'})[0..4];
#pp oracledbh.select_all(%Q{SELECT table_name FROM all_tables where table_name like 'CPG_XCDE%' and rownum <= 5});
#pp oracledbh.select_all(%Q{SELECT * FROM HZ_PERSON_PROFILES_V where rownum <=5});

$TradingPartnerID   = $properties["crdb_trading_party_site_id"]


$webserviceurl		= "https://#{$properties.osscr_public_webservice_https_fqdn}:#{$properties.osscr_public_webservice_https_port}/"

puts "Note: will connect to osscr web service at #{$webserviceurl}."

# Other params for the web service API --

$appid				= 'OSSCR_TEST'
$apptoken			= 'PASS'
$method				= 'MultiRecordTransaction2'

$TableNameToOracleView    = Hash[ * %w[
                                   HZ_ORGANIZATION_PROFILES     apps.HZ_ORGANIZATION_PROFILES_v
                                   HZ_PERSON_PROFILES           apps.HZ_PERSON_PROFILES_v
                                   HZ_CONTACT_POINTS            apps.HZ_CONTACT_POINTS
                                   HZ_PARTY_SITES               apps.HZ_PARTY_SITES_v
                                   HZ_RELATIONSHIPS             apps.HZ_RELATIONSHIPS
                                   HZ_CONTACT_PREFERENCES       apps.HZ_CONTACT_PREFERENCES
                                   HZ_EMPLOYMENT_HISTORY        apps.HZ_EMPLOYMENT_HISTORY
                                   HZ_EDUCATION                 apps.HZ_EDUCATION
                                   HZ_HONORS                    apps.HZ_HONORS
                                   HZ_CERTIFICATIONS            apps.HZ_CERTIFICATIONS
                                   CPG_ECD_SOCIETY              apps.HZ_SOCIETY
                                   CPG_HZ_ECD_NOTES             apps.CPG_HZ_ECD_NOTES
								   CPG_HZ_COMPENSATION          BOLINF.CPG_HZ_COMPENSATION
								   CPG_HZ_EXEMPT                BOLINF.CPG_HZ_EXEMPT		
								   CPG_HZ_ONETIME_PAYMENTS      BOLINF.CPG_HZ_ONETIME_PAYMENTS
 								   CPG_HZ_SCHEDULED_HOURS       BOLINF.CPG_HZ_SCHEDULED_HOURS
								 ]];


$Ownertablename    = Hash[ * %w[
  HZ_HONORS					HZ_HONORS
  HZ_CONTACT_POINTS			HZ_CONTACT_POINTS
  HZ_EDUCATION				HZ_EDUCATION
  HZ_PERSON_PROFILES		HZ_PARTIES
  HZ_ORGANIZATION_PROFILES	HZ_PARTIES
  HZ_RELATIONSHIPS			HZ_PARTIES
  HZ_PARTY_SITES			HZ_LOCATIONS
  HZ_EMPLOYMENT_HISTORY		HZ_EMPLOYMENT_HISTORY
  HZ_CERTIFICATIONS			HZ_CERTIFICATIONS
  HZ_CONTACT_PREFERENCES	HZ_CONTACT_PREFERENCES
  CPG_ECD_SOCIETY			HZ_SOCIETY
  CPG_HZ_ECD_NOTES			CPG_HZ_ECD_NOTES
  CPG_HZ_COMPENSATION       HZ_COMPENSATION
  CPG_HZ_EXEMPT             HZ_EXEMPT
  CPG_HZ_ONETIME_PAYMENTS   HZ_PAYMENTS
  CPG_HZ_SCHEDULED_HOURS    HZ_SCHEDULED_HRS
  ]];


$TableNameToOSSCRPrimaryKeys = Hash[ * %w[
	HZ_RELATIONSHIPS            O_PARTY_ID
	HZ_CONTACT_POINTS           O_CONTACT_POINT_ID
	HZ_PARTY_SITES              O_LOCATION_ID
	HZ_ORGANIZATION_PROFILES    O_PARTY_ID
	HZ_CERTIFICATIONS           O_CERTIFICATION_ID
	HZ_PERSON_PROFILES          O_PARTY_ID
	HZ_EMPLOYMENT_HISTORY       O_EMPLOYMENT_HISTORY_ID
	HZ_CONTACT_PREFERENCES      O_CONTACT_PREFERENCE_ID
	HZ_EDUCATION                O_EDUCATION_ID
	CPG_HZ_ECD_NOTES            O_ECD_NOTES_ID
	HZ_HONORS                   O_HONOR_ID
	CPG_ECD_SOCIETY             O_SOCIETY_ID
    CPG_HZ_COMPENSATION         O_COMPENSATION_ID
    CPG_HZ_EXEMPT               O_EXEMPT_ID
	CPG_HZ_ONETIME_PAYMENTS     O_PAYMENT_ID
	CPG_HZ_SCHEDULED_HOURS      O_SCHEDULED_ID
  ]]

$TableNameToCRDBPrimaryKeys = Hash[ * %w[
	HZ_RELATIONSHIPS            PARTY_ID
	HZ_CONTACT_POINTS           CONTACT_POINT_ID
	HZ_PARTY_SITES              LOCATION_ID
	HZ_ORGANIZATION_PROFILES    PARTY_ID
	HZ_CERTIFICATIONS           CERTIFICATION_ID
	HZ_PERSON_PROFILES          PARTY_ID
	HZ_EMPLOYMENT_HISTORY       EMPLOYMENT_HISTORY_ID
	HZ_CONTACT_PREFERENCES      CONTACT_PREFERENCE_ID
	HZ_EDUCATION                EDUCATION_ID
	CPG_HZ_ECD_NOTES            ECD_NOTES_ID
	HZ_HONORS                   HONOR_ID
	CPG_ECD_SOCIETY             SOCIETY_ID
    CPG_HZ_COMPENSATION         COMPENSATION_ID
    CPG_HZ_EXEMPT               EXEMPT_ID
    CPG_HZ_ONETIME_PAYMENTS     PAYMENT_ID
	CPG_HZ_SCHEDULED_HOURS      SCHEDULED_ID
   ]]
  

$TableNameToOSSCRForeignKeys  = ({
                                   'HZ_ORGANIZATION_PROFILES'     => [],
                                   'HZ_PERSON_PROFILES'           => [],
                                   'HZ_CONTACT_POINTS'            => ['O_OWNER_TABLE_ID'],
                                   'HZ_PARTY_SITES'               => ['O_PARTY_ID'],
                                   'HZ_RELATIONSHIPS'             => ['O_SUBJECT_ID',
									 'O_OBJECT_ID'],
                                   'HZ_CONTACT_PREFERENCES'       => ['O_CONTACT_LEVEL_TABLE_ID'],
                                   'HZ_EMPLOYMENT_HISTORY'        => ['O_PARTY_ID',
									 'O_EMPLOYED_BY_DIVISION_NAME'],
                                   'HZ_EDUCATION'                 => ['O_PARTY_ID'],
                                   'HZ_HONORS'                    => ['O_PARTY_ID'],
                                   'HZ_CERTIFICATIONS'            => ['O_PARTY_ID'],
                                   'CPG_ECD_SOCIETY'              => ['O_PARTY_ID'],
                                   'CPG_HZ_ECD_NOTES'             => ['O_PARTY_ID'],
                                   'CPG_HZ_COMPENSATION'          => ['O_REL_PARTY_ID'],
                                   'CPG_HZ_EXEMPT'                => ['O_REL_PARTY_ID'],
								   'CPG_HZ_ONETIME_PAYMENTS'      => ['O_REL_PARTY_ID'],
								   'CPG_HZ_SCHEDULED_HOURS'       => ['O_REL_PARTY_ID']
								 });


#These fields are not considered for comparison of create records. All foreign keys are to be ignored.
$FieldsToIgnoreForCreate  = ({
							   'HZ_ORGANIZATION_PROFILES'     => ['ATTRIBUTE4','ATTRIBUTE5','ATTRIBUTE10','ATTRIBUTE12','ATTRIBUTE13','ATTRIBUTE14'],
							   'HZ_PERSON_PROFILES'           => [
								 #'ATTRIBUTE1', 						# Client number not assignable from OSSCR API
								 'KNOWN_AS5'],						# Lay/Clergy status not assignable from OSSCR
							   'HZ_CONTACT_POINTS'            => ['O_OWNER_TABLE_ID','TELEPHONE_TYPE','TIME_ZONE','DO_NOT_USE_FLAG'],
							   'HZ_PARTY_SITES'               => ['O_PARTY_ID'],
							   'HZ_RELATIONSHIPS'             => ['O_SUBJECT_ID',
								 'O_OBJECT_ID'],
							   'HZ_CONTACT_PREFERENCES'       => ['O_CONTACT_LEVEL_TABLE_ID'],
							   'HZ_EMPLOYMENT_HISTORY'        => ['O_PARTY_ID',
								 'O_EMPLOYED_BY_DIVISION_NAME'],
							   'HZ_EDUCATION'                 => ['O_PARTY_ID'],
							   'HZ_HONORS'                    => ['O_PARTY_ID'],
							   'HZ_CERTIFICATIONS'            => ['O_PARTY_ID'],
							   'CPG_ECD_SOCIETY'              => ['O_PARTY_ID'],
							   'CPG_HZ_ECD_NOTES'             => ['O_PARTY_ID'],
							   'CPG_HZ_COMPENSATION'          => ['O_REL_PARTY_ID'],
							   'CPG_HZ_EXEMPT'                => ['O_REL_PARTY_ID'],
							   'CPG_HZ_ONETIME_PAYMENTS'      => ['O_REL_PARTY_ID'],
							   'CPG_HZ_SCHEDULED_HOURS'       => ['O_REL_PARTY_ID']
							 });

#These fields are not considered for comparison of update records.All foreign keys are to be ignored.
$FieldsToIgnoreForUpdate  = ({
							   'HZ_ORGANIZATION_PROFILES'     => ['DUNS_NUMBER', 'EMPLOYEES_TOTAL','ORGANIZATION_NAME','ATTRIBUTE9',
								 'ATTRIBUTE4','ATTRIBUTE5','ATTRIBUTE10','ATTRIBUTE12','ATTRIBUTE13','ATTRIBUTE14'],
							   'HZ_PERSON_PROFILES'           => [
								 #'ATTRIBUTE1', 						# Client number not assignable from OSSCR API
								 'KNOWN_AS5'],						# Lay/Clergy status not assignable from OSSCR
							   'HZ_CONTACT_POINTS'            => ['O_OWNER_TABLE_ID','TELEPHONE_TYPE','TIME_ZONE',
								 'DO_NOT_USE_FLAG','CONTACT_POINT_TYPE','PHONE_LINE_TYPE'],
							   'HZ_PARTY_SITES'               => ['O_PARTY_ID','PRIMARY_PER_TYPE','SITE_USE_TYPE'],
							   'HZ_RELATIONSHIPS'             => ['O_SUBJECT_ID',
								 'O_OBJECT_ID'],
							   'HZ_CONTACT_PREFERENCES'       => ['O_CONTACT_LEVEL_TABLE_ID'],
							   'HZ_EMPLOYMENT_HISTORY'        => ['O_PARTY_ID',
								 'O_EMPLOYED_BY_DIVISION_NAME'],
							   'HZ_EDUCATION'                 => ['O_PARTY_ID'],
							   'HZ_HONORS'                    => ['O_PARTY_ID'],
							   'HZ_CERTIFICATIONS'            => ['O_PARTY_ID'],
							   'CPG_ECD_SOCIETY'              => ['O_PARTY_ID'],
							   'CPG_HZ_ECD_NOTES'             => ['O_PARTY_ID'],
							   'CPG_HZ_COMPENSATION'          => ['O_REL_PARTY_ID'],
							   'CPG_HZ_EXEMPT'                => ['O_REL_PARTY_ID'],
							   'CPG_HZ_ONETIME_PAYMENTS'      => ['O_REL_PARTY_ID'],
							   'CPG_HZ_SCHEDULED_HOURS'       => ['O_REL_PARTY_ID']
							 });

#One of the fields from each table is chosen to confirm the update is completed. This field will determine when to exit the wait loop for update test.
$FieldsToVerifyUpdate     = ({
							   'HZ_ORGANIZATION_PROFILES'     => ['ATTRIBUTE11'],
							   'HZ_PERSON_PROFILES'           => ['ATTRIBUTE9'],
							   'HZ_CONTACT_POINTS'            => ['PHONE_NUMBER'],
							   'HZ_PARTY_SITES'               => ['ADDRESS1'],
							   'HZ_RELATIONSHIPS'             => ['ATTRIBUTE2'],
							   'HZ_CONTACT_PREFERENCES'       => ['CONTACT_TYPE'],
							   'HZ_EMPLOYMENT_HISTORY'        => ['RESPONSIBILITY'],
							   'HZ_EDUCATION'                 => ['SCHOOL_ATTENDED_NAME'],
							   'HZ_HONORS'                    => ['HON_AWARD'],
							   'HZ_CERTIFICATIONS'            => ['ISSUED_BY_AUTHORITY'],
							   'CPG_ECD_SOCIETY'              => ['SOC_SOCIETY'],
							   'CPG_HZ_ECD_NOTES'             => ['ECD_NOTES_TYPE'],
							   'CPG_HZ_ONETIME_PAYMENTS'      => ['PAYMENT_TYPE'],
							   'CPG_HZ_EXEMPT'                => ['EXEMPT_STATUS'],
							   'CPG_HZ_SCHEDULED_HOURS'       => ['SCHEDULED_HOURS'],
							   'CPG_HZ_COMPENSATION'          => ['LEAVE_OF_ABSENCE_TYPE']
							 });


$DateFields				  = ({
							   'HZ_PERSON_PROFILES'	 	      => ['DATE_OF_BIRTH','DATE_OF_DEATH','MARITAL_STATUS_EFFECTIVE_DATE'],
							   'CPG_ECD_SOCIETY' 		      => ['SOC_END_YEAR','SOC_START_YEAR'],
							   'HZ_HONORS' 				      => ['HON_START_YEAR'],
							   'HZ_EDUCATION' 			      => ['LAST_DATE_ATTENDED'],
							   'HZ_CONTACT_PREFERENCES'       => ['PREFERENCE_END_DATE','PREFERENCE_START_DATE'],
							   'HZ_RELATIONSHIPS' 		      => ['END_DATE','START_DATE'],
							   'HZ_EMPLOYMENT_HISTORY' 	      => ['END_DATE','BEGIN_DATE'],
							   'HZ_CERTIFICATIONS' 		      => ['ISSUED_ON_DATE'],
							   'HZ_PARTY_SITES' 		      => ['ADDRESS_EXPIRATION_DATE','ADDRESS_EFFECTIVE_DATE'],
							   'CPG_HZ_ONETIME_PAYMENTS'      => ['ONE_TIME_PAYMENT_DATE'],
							   'CPG_HZ_SCHEDULED_HOURS'       => ['SCHEDULED_HOURS_EFF_DATE'],
							   'CPG_HZ_EXEMPT'                => ['EXEMPT_STATUS_EFF_DATE'],
							   'CPG_HZ_COMPENSATION'          => ['COMPENSATION_EFF_DATE']
							 });

def run
  check=true
  while check
    do_create=false
    do_update=false
    do_delete=false
    puts "\n"
    puts "Please enter the test to perform on OSSCR: c/u/d/cu/e ? "
    puts " c   - Create Test"
    puts " u   - Update Test"
    puts " d   - Delete Test"
    puts " e   - Exit"
    option = gets.chomp
	
    ## Get these booleans from command line options
    case option
    when /^[cC]$/
      do_create = true
    when /^[uU]$/
      do_update = true
    when /^[dD]$/
      do_delete = true
    end
            
    create_test if do_create
    update_test if do_update
    delete_test if do_delete
    check = false if (option.eql?("e"))
  end
end

def create_test
  # Read 16 hardcoded records to be created
  json_file			= "#{$ProjDir}/active/tests/end_to_end/OSSCR_JSON.txt"
  json_code			= File.read(json_file) or raise "Failed to read json code from #{json_file}"
  $create_transactions = JSON.load(json_code)

  # DEBUG: uncomment this to do just the person (first Transaction)
  # $create_transactions = [$create_transactions[0]]
  
  # Adding Random numbers to make SSN and DOB as unique records in every transaction
  person_profiles=$create_transactions[0]
  str= person_profiles["c"]["JGZZ_FISCAL_CODE"]
  person_profiles["c"]["JGZZ_FISCAL_CODE"]= str[0..6] + (str[7..10].to_i+rand(8999)).to_s 
  person_profiles["c"]["DATE_OF_BIRTH"]= (Date.parse(person_profiles["c"]["DATE_OF_BIRTH"])+rand(1000)).to_s

  data				= URI.escape($create_transactions.to_json, %r{[ %#&?=/\\<>|']})
  puts "webserviceurl is #{$webserviceurl}"

  puts "Note: will connect to osscr web service at #{$webserviceurl}."
  
  output            = `curl -s -i '#{$webserviceurl}/?Application_Id=#{$appid}&Application_Token=#{$apptoken}&Method=#{$method}' -d DataRecords='#{data}'`

  puts "Results of calling web service:"
  puts "#{output}"
  
  # Extract all OSSCR id's from the output into a hash
  
  if (output	    =~ /TRUE/)	# i.e. transaction returned successfully
    $osscr_ids		= JSON.parse(output.to_s.grep(/MethodResponse/).to_s.sub(/MethodResponse: (.*)/, '\1').chomp)
  else
	error           = output.to_s.grep(/ErrorMessage/)
	raise "#{error}"
  end
  
  # Save the created osscr_ids for use in the update/delete test phases...
  # ~/projects/cpg-osscr/dev/testing/created_osscr_ids.json
  fJson             = File.open("#{$ProjDir}/testing/created_osscr_ids.json","w") do |f|
    f.write($osscr_ids.to_json)
  end
  
  # Copy the create_transaction records and supply the IDs from OSSCR.
  # These records should now exactly match the corresponding records in mysql.
  expected_data = {}
  $create_transactions.each{|tx|
    tn              = tx["t"]
    rec             = (tx["c"] or raise "no create record found").clone   ## this is always "c" for the "create"
    keyfield        = $TableNameToOSSCRPrimaryKeys[tn] or raise "No primary key found for table #{tn}."
    placeholderkey  = rec[keyfield]
    rec[keyfield]   = $osscr_ids[placeholderkey] or raise "no ID found for #{placeholderkey}"
    foreign_keys    = $TableNameToOSSCRForeignKeys[tn] or raise "no foreign keys found for table #{tn}."
    foreign_keys.each{|keyfield|
      placeholderkey= rec[keyfield]	
      rec[keyfield] = $osscr_ids[placeholderkey] or raise "no ID found for #{placeholderkey}"	
    }
    expected_data[tn]=rec  
  }
  
  # MYSQL Test
  # Get table's id field name and query this record from mysql into a hash
  # Compare hash from mysql with rec; any differences = a bug
  puts "********CREATE TEST - MYSQL OSSCR DATA COMPARISON*************"
  expected_data.each{|tn,rec|
    keyfield        = $TableNameToOSSCRPrimaryKeys[tn] or raise "No primary key found for table #{tn}."
    primarykey      = rec[keyfield]
    osscr_data      = $mysqldbh.execute(%Q{SELECT #{rec.keys.join ","} FROM #{tn} where #{keyfield} = "#{primarykey}"}).fetch_hash
    success         = true
    osscr_data.keys.each do |key| 
      if rec[key].to_s != osscr_data[key].to_s
        puts "Data Mismatch: JSON value for #{key} is #{rec[key]} compared to OSSCR value of #{osscr_data[key]}"
	    success     = false
      end
    end
    if ! success
      puts "TEST FAILED for #{tn}"
    else
      puts "Test passed for #{tn}"
    end
  }
  
  #ORACLE Test
  #Compare records created in CRDB with JSON data
  #Compare hash from oracle with rec; any differences = a bug

  $TradingPartnerID = $properties["crdb_trading_party_site_id"]
  org_key           = nil
  puts "\n"
  puts "***********CREATE TEST - ORACLE CRDB DATA COMPARISON*************"
  expected_data.each{|tn,rec|
    crdb_tn         = $TableNameToOracleView[tn]
    keyfield        = $TableNameToOSSCRPrimaryKeys[tn] or raise "No primary key found for table #{tn}."
    crdb_key        = $TableNameToCRDBPrimaryKeys[tn]
	primarykey      = rec[keyfield]
    owner_tn        = $Ownertablename[tn]
    timewait        = true
    before          = Time.now
    new_key         = nil
    while timewait do
      puts "#{`date`} waiting for trade to complete in CRDB for #{tn} with #{keyfield} = #{primarykey} ... (control-C to skip Oracle testing step)"
      new_key       = $oracledbh.select_all(%Q{SELECT r.owner_table_id as CRDB_PRIMARY_KEY from apps.HZ_ORIG_SYS_REFERENCES r 
							 WHERE r.orig_system = '#{$TradingPartnerID}' AND STATUS='A' AND r.orig_system_reference = '#{primarykey}'})
      sleep 2
      elapsed       = Time.now-before
      puts "elapsed time is #{elapsed}"

	  # Break from the wait loop as soon as the id is found.
	  if (!new_key.empty?||elapsed>80)
	    puts "Trading completed for #{tn} with CRDB Primary key #{new_key}"
		timewait = false 
	  end
	  
	  # If the trade never seems to go through, stop trying
	  break if (elapsed>380)
      end	
	  # Warn the user if we had to stop trying, and skip further testing of this record.
	  if (elapsed>380)
	    puts "Trading not completed for create record in #{tn}.... SKIPPING create test."
		puts "\n"
	    next
	  end
    new_key         = new_key[0][0]	# Get first value from first record of SELECT above

    #LOOP until CRDB KEYS are received in OSSCR and the record is updated with CRDB Id's
	
	timewait1		= true
	before1			=Time.now
	#new_id = nil
	while timewait1 do
	 puts "Waiting for OSSCR to receive the CRDB Keys.........(control-C to skip this step)"
	 new_id = $mysqldbh.select_all(%Q{SELECT #{crdb_key} from #{tn} where #{keyfield}='#{primarykey}'})
	 sleep 2
	 elapsed1 = Time.now-before1
	 puts "elapsed time is #{elapsed1}"
	
	#Break from wait loop as soon as the id is found.
	if(!new_id[0][0].nil?)
	if(new_id[0][0].size>0||elapsed1>60)
	 puts "OSSCR received the CRDB keys."
	 timewait1=false
	end
    end
	break if (elapsed1>10)
	end
	if(elapsed1>10)
	 puts "CRDB keys are not yet updated in OSSCR for #{tn}....SKIPPING create test."
	 puts "\n"
	end
    
	# LOOP UNTIL CLIENT NUMBER (ATTRIBUTE1) IS ASSIGNED FOR PERSON_PROFLES
    crdb_key        = $TableNameToCRDBPrimaryKeys[tn]
    timewait2       = true 
    if (tn=="HZ_PERSON_PROFILES") 
      while timewait2 do
        puts "#{`date`} waiting for client number to be traded for #{tn}.........(control-C to skip Oracle testing step)"
        client_no   = $oracledbh.select_all(%Q{SELECT ATTRIBUTE1 from #{crdb_tn} where #{crdb_key} = #{new_key}})
        sleep 2
        if (!client_no[0][0].nil?)
        if (client_no[0][0].length>0)
          timewait2 = false
        end
      end
    end
    puts "Client number for #{tn} is #{client_no[0][0]}"
    end

 
   #crdb_data = $oracledbh.execute(%Q{SELECT r.ORIG_SYSTEM_REFERENCE as #{keyfield},s.* from apps.HZ_ORIG_SYS_REFERENCES r, #{crdb_tn} s where r.owner_table_id=s.#{crdb_key} AND r  .owner_table_name = '#{owner_tn}' AND s.#{crdb_key}=#{new_key}}).fetch_hash

   crdb_data = $oracledbh.execute(%Q{SELECT * FROM #{crdb_tn} where #{crdb_key} = #{new_key}}).fetch_hash

   osscr_ids_in_crdb = $oracledbh.select_all(%Q{SELECT DISTINCT r.ORIG_SYSTEM_REFERENCE as #{keyfield} from apps.HZ_ORIG_SYS_REFERENCES r, #{crdb_tn} s where r.orig_system = '#{$TradingPartnerID}' AND r  .owner_table_id=s.#{crdb_key} AND r.owner_table_name = '#{owner_tn}' AND s.#{crdb_key}=#{new_key}})
   puts "The osscr id's in CRDB database for #{new_key} are:"
   puts osscr_ids_in_crdb

   crdb_data["#{keyfield}"] = osscr_ids_in_crdb[0][0]
   success=true
   rec.keys.each do |key| 
   # OSSCR only deals with integers; for some reason Oracle returns these as float types.  Adjust that here.
     #if (crdb_data[key].class.to_s == "Float") then	crdb_data[key] = crdb_data[key].to_i; end
     if (crdb_data[key].class.to_s == "Float") then 
	 crdb_data[key] ="%.2f" % crdb_data[key].to_f
	 end

     if ( tn == "HZ_PERSON_PROFILES" && key=="GENDER") then crdb_data[key]= MapOracleToOSSCR_PersonGender("#{crdb_data[key]}"); end
	
     if ( tn == "HZ_RELATIONSHIPS" && key=="SUBJECT_TYPE") then crdb_data[key]= MapOracleToOSSCR_RelationshipSubjObjType("#{crdb_data[key]}"); end

     if ( tn == "HZ_RELATIONSHIPS" && key=="OBJECT_TYPE") then crdb_data[key]= MapOracleToOSSCR_RelationshipSubjObjType("#{crdb_data[key]}"); end
	  
	 if ($DateFields[tn] && $DateFields[tn].include?(key))
		crdb_data[key]=crdb_data[key].to_date
	 end
	  
     if !($FieldsToIgnoreForCreate[tn].include?(key))&&(rec[key].to_s != crdb_data[key].to_s)
       puts "Data Mismatch: JSON value for #{key} is #{rec[key]} compared to CRDB value of #{crdb_data[key]}"
       success=false
     end
   end
   if ! success
     puts "TEST FAILED for #{crdb_tn}"
   else
     puts "Test passed for #{crdb_tn}"
   end
   puts "\n"
   }     
end


# Update transactions
def update_test
  # Read 12 hardcoded records to be updated -- these are intentionally
  # similar to the "create" tests, but each field that can be changed,
  # has been, so we can be sure the changes are coming through.
  
  json_update_file           = "#{$ProjDir}/active/tests/end_to_end/OSSCRJSON_Update.txt"
  json_update_code           = File.read(json_update_file) or raise "Failed to read json code from #{json_update_file}"
  update_transactions        = JSON.load(json_update_code)

  # Read in the created osscr_ids (this allows us to test update_test
  # during development without always re-running the create).
  # ~/projects/cpg-osscr/dev/testing/created_osscr_ids.json

  update_osscrids            = File.read("#{$ProjDir}/testing/created_osscr_ids.json") or raise "Failed to read json code from #{json_file}"
  $osscr_ids                 = JSON.load(update_osscrids)

  # Apply the created IDs to the placeholders ("NEW1", etc.) in the
  # hardcoded transactions.

  update_transactions.each{|tx|
    tn 				= tx["t"]
	rec  			= tx["r"] or raise "no record found"   ## this is always "r" for the "replace"
	keyfield		= $TableNameToOSSCRPrimaryKeys[tn] or raise "No primary key found for table #{tn}."
	placeholderkey	= rec[keyfield]
	rec[keyfield]	= $osscr_ids[placeholderkey] or raise "no ID found for #{placeholderkey}"
  }
  
  update_data         = URI.escape(update_transactions.to_json, %r{[ %#&?=/\\<>|']})
  
  puts "connecting to #{$webserviceurl}......"
  output2             = `curl -s -i '#{$webserviceurl}?Application_Id=#{$appid}&Application_Token=#{$apptoken}&Method=#{$method}' -d DataRecords='#{update_data}'`
  puts "Web service output from update commands is #{output2}"

  
  # Check that OSSCR's mysql database received the changes
  puts "********UPDATE TEST - MYSQL OSSCR DATA COMPARISON*************"
  update_transactions.each{|tx|
    tn = tx["t"]
    rec = tx["r"]
    keyfield = $TableNameToOSSCRPrimaryKeys[tn] or raise "No primary key found for table #{tn}."
    primarykey = rec[keyfield]
    osscr_updated_data = $mysqldbh.execute(%Q{SELECT #{rec.keys.join ","} FROM #{tn} where #{keyfield} = "#{primarykey}"}).fetch_hash
    success=true
    osscr_updated_data.keys.each do |key| 
      #puts "rec value is #{rec[key]}"
      #puts "OSSCR value is #{osscr_updated_data[key]}"
      if rec[key].to_s != osscr_updated_data[key].to_s
        puts "Data Mismatch: JSON value for #{key} is #{rec[key]} compared to OSSCR value of #{osscr_updated_data[key]}"
	    success=false
      end
    end
    if ! success
      puts "TEST FAILED for #{tn}"
    else
      puts "Test passed for #{tn}"
    end
  }

  # In case the user stopped the create step and then jumped to the
  # update step, we re-check that all the expected records have been
  # created before proceeding with the update.
  puts "\n"
  puts "********UPDATE TEST - ORACLE CRDB DATA COMPARISON*************"
  update_transactions.each{|tx|
    tn= tx["t"]
	rec=tx["r"]
	crdb_tn    = $TableNameToOracleView[tn]
	keyfield   = $TableNameToOSSCRPrimaryKeys[tn] or raise "No primary key found for table #{tn}."
	primarykey = rec[keyfield]
	owner_tn   = $Ownertablename[tn]
	crdb_key    = $TableNameToCRDBPrimaryKeys[tn]
	
	#puts "primary key is #{primarykey}"
	timecheck = true
	ckey=nil
	while timecheck do
	  puts "#{`date`} waiting for trade to complete in CRDB for #{tn} with #{keyfield} = #{primarykey} ... (control-C to skip Oracle testing step)"    
	  ckey = $oracledbh.select_all(%Q{SELECT r.owner_table_id as CRDB_PRIMARY_KEY from apps.HZ_ORIG_SYS_REFERENCES r WHERE r.orig_system = '#{$TradingPartnerID}' AND STATUS='A' AND r.orig_system_reference = '#{primarykey}'})
	  sleep 2
	  timecheck = false if (!ckey.empty? )
	end
  
  #puts "ckey is #{ckey}"
  }
  update_transactions.each{|tx|
    tn= tx["t"]
	rec=tx["r"]
	crdb_tn    = $TableNameToOracleView[tn]
	keyfield   = $TableNameToOSSCRPrimaryKeys[tn] or raise "No primary key found for table #{tn}."
	primarykey = rec[keyfield]
	owner_tn   = $Ownertablename[tn]
	crdb_key    = $TableNameToCRDBPrimaryKeys[tn]
	update_key = $FieldsToVerifyUpdate[tn]
	ckey = $oracledbh.select_all(%Q{SELECT r.owner_table_id as CRDB_PRIMARY_KEY from apps.HZ_ORIG_SYS_REFERENCES r WHERE r.orig_system = '#{$TradingPartnerID}' AND STATUS='A' AND r.orig_system_reference = '#{primarykey}'})	  	
    ckey=ckey[0][0] 

	# Now check each updated record by querying oracle and comparing
    updated_crdb = $oracledbh.execute(%Q{SELECT * from #{crdb_tn} where #{crdb_key} = #{ckey}}).fetch_hash
	timecheck2 = true
	before=Time.now
	while timecheck2 do
	  updated_crdb = $oracledbh.execute(%Q{SELECT * from #{crdb_tn} where #{crdb_key} = #{ckey}}).fetch_hash
	  puts "#{`date`}  waiting for update trade to complete in  CRDB for #{tn}....(control-C to skip testing step)"
	  sleep 2
	  #puts "osscr value is #{rec["#{update_key}"]}"
	  #puts "crdb value is #{updated_crdb["#{update_key}"]}"
	  elapsed  =Time.now-before
	  puts "elapsed time is #{elapsed}"
	  
	  # Break from the wait loop as soon as one representative field has been updated.
	  if ((rec["#{update_key}"].to_s == updated_crdb["#{update_key}"].to_s)||(elapsed>600))
	    timecheck2 = false 
	  end
	  
	  # If the trade never seems to go through, stop trying after 10 minutes
	  break if (elapsed>600)
      end	
	  # Warn the user if we had to stop trying, and skip further testing of this record.
	  if (elapsed>600)
	    puts "Trading not completed for updated record in #{tn}.... SKIPPING update test."
		puts "\n"
	    next
	  end
      crdb_ids = $oracledbh.select_all(%Q{SELECT DISTINCT r.ORIG_SYSTEM_REFERENCE as #{keyfield} from apps.HZ_ORIG_SYS_REFERENCES r, #{crdb_tn} s where r.orig_system = '#{$TradingPartnerID}' AND r  .owner_table_id=s.#{crdb_key} AND r.owner_table_name = '#{owner_tn}' AND s.#{crdb_key}=#{ckey}})
      updated_crdb["#{keyfield}"] = crdb_ids[0][0]

      check=true
      rec.keys.each do |key|
        # OSSCR only deals with integers; for some reason Oracle returns these as float types.  Adjust that here.
        # if (updated_crdb[key].class.to_s == "Float") then updated_crdb[key] = updated_crdb[key].to_i; end

        if (updated_crdb[key].class.to_s == "Float") then 
	      updated_crdb[key] ="%.2f" % updated_crdb[key].to_f
	    end
        
		if ( tn == "HZ_PERSON_PROFILES" && key=="GENDER") then updated_crdb[key]= MapOracleToOSSCR_PersonGender("#{updated_crdb[key]}"); end
	
        if ( tn == "HZ_RELATIONSHIPS" && key=="SUBJECT_TYPE") then updated_crdb[key]= MapOracleToOSSCR_RelationshipSubjObjType("#{updated_crdb[key]}"); end
   
        if ( tn == "HZ_RELATIONSHIPS" && key=="OBJECT_TYPE") then updated_crdb[key]= MapOracleToOSSCR_RelationshipSubjObjType("#{updated_crdb[key]}"); end

	    #Adjust the Oracle date format
		if ($DateFields[tn] && $DateFields[tn].include?(key))
	      updated_crdb[key]=updated_crdb[key].to_date
	    end
	    if !($FieldsToIgnoreForUpdate[tn].include?(key))&&(rec[key].to_s != updated_crdb[key].to_s)
	      puts "Data Mismatch: JSON value for #{key} is #{rec[key]} compared to CRDB value of #{updated_crdb[key]}"
	      check=false
        end
      end	
  
      if !check
        puts "TEST FAILED for #{crdb_tn}"
      else
        puts "Test passed for #{crdb_tn}"
      end
      puts "\n\n"
    }
end

def delete_test
  json_delete_file           = "#{$ProjDir}/active/tests/end_to_end/OSSCRJSON_Delete.txt"
  json_delete_code           = File.read(json_delete_file) or raise "Failed to read json code from #{json_delete_file}"
  delete_transactions        = JSON.load(json_delete_code)

  # Read in the created osscr_ids (this allows us to test delete_test
  # during development without always re-running the create).
  # ~/projects/cpg-osscr/dev/testing/created_osscr_ids.json

  delete_osscrids            = File.read("#{$ProjDir}/testing/created_osscr_ids.json") or raise "Failed to read json code from #{json_file}"
  $osscr_ids                 = JSON.load(delete_osscrids)
  # puts "osscr ids are #{$osscr_ids}"
  # Apply the created IDs to the placeholders ("NEW1", etc.) in the
  # hardcoded transactions.

  delete_transactions.each{|tx|
    tn              = tx["t"]
	rec             = tx["d"] or raise "no record found"   ## this is always "d" for the "delete"
    tx["d"] = $osscr_ids["#{rec}"].to_s
  }
  delete_data         = URI.escape(delete_transactions.to_json, %r{[ %#&?=/\\<>|']})
  puts "connecting to #{$webserviceurl}......"
  output3             = `curl -s -i '#{$webserviceurl}?Application_Id=#{$appid}&Application_Token=#{$apptoken}&Method=#{$method}' -d DataRecords='#{delete_data}'`
  puts "Web service output from update commands is #{output3}"
  
  # Check that OSSCR's mysql database deleted the records
  puts "********DELETE TEST - MYSQL OSSCR DATA COMPARISON*************"
  delete_transactions.each{|tx|
    tn = tx["t"]
    primarykey = tx["d"]
    keyfield = $TableNameToOSSCRPrimaryKeys[tn] or raise "No primary key found for table #{tn}."
    osscr_deleted_data = $mysqldbh.execute(%Q{SELECT * FROM #{tn} where #{keyfield} = "#{primarykey}"}).fetch_hash
	#pp osscr_deleted_data
    if osscr_deleted_data.empty?
	  puts "Test passed for #{tn}"
    else
      puts "TEST FAILED for #{tn}"
    end
  }

  # Check the CRDB's oracle database updated the status to I(Inactive)
  puts "********DELETE TEST - ORACLE CRDB DATA COMPARISON*************"
  delete_transactions.each{|tx|
    tn= tx["t"]
	primarykey=tx["d"]
	crdb_tn    = $TableNameToOracleView[tn]
	owner_tn   = $Ownertablename[tn]
	crdb_key    = $TableNameToCRDBPrimaryKeys[tn]
	ckey = $oracledbh.select_all(%Q{SELECT r.owner_table_id as CRDB_PRIMARY_KEY from apps.HZ_ORIG_SYS_REFERENCES r WHERE r.orig_system = '#{$TradingPartnerID}' AND STATUS='A' AND r.orig_system_reference = '#{primarykey}'})	  	
    #ckey=ckey[0][0] or raise "ckey value not found"
    #puts "ckey is #{ckey}"
    
	timecheck_delete = true
	before=Time.now
	while timecheck_delete do
      # Now check each deleted record by querying oracle and comparing
      deleted_crdb = $oracledbh.execute(%Q{SELECT * from #{crdb_tn} where #{crdb_key} = #{ckey}}).fetch_hash
	  puts "#{`date`}  waiting for delete trade to complete in  CRDB for #{tn}....(control-C to skip testing step)"
      sleep 2
      elapsed       = Time.now-before
      #puts "elapsed time is #{elapsed}"
	  # Break from the wait loop as soon as the id is found.
	  if (deleted_crdb["STATUS"].eql?("I")||elapsed>300)
		timecheck_delete = false 
	  end
	  # If the trade never seems to go through, stop trying
	  break if (elapsed>300)
      end	
	  # Warn the user if we had to stop trying, and skip further testing of this record.
	  if (elapsed>300)
	    puts "Trading not completed for create record in #{tn}.... SKIPPING delete test."
		puts "\n"
	    next
	  end    
    deleted_crdb = $oracledbh.execute(%Q{SELECT * from #{crdb_tn} where #{crdb_key} = #{ckey}}).fetch_hash
    #puts deleted_crdb["STATUS"]
	if deleted_crdb["STATUS"].eql?("I")
	 #puts deleted_crdb["STATUS"]
	 puts "Test passed for #{tn} \n"
	else
	 puts "TEST FAILED for #{tn}. Status is still A \n"
	end
	puts "\n"
  }
end


######## Helper functions ######

#Gender field of HZ_PERSON_PROFILES is mapped in CRDB tables. 
def MapOracleToOSSCR_PersonGender(gender)
  gender.gsub!("FEMALE","F") if (gender=="FEMALE")
  gender.gsub!("MALE","M") if (gender=="MALE")
  return gender  
end

#SUBJECT & OBJECT TYPE for HZ_RELATIONSHIPS is mapped in CRDB tables.
def MapOracleToOSSCR_RelationshipSubjObjType(type)
  type.gsub!("PERSON","INDIVIDUAL") if (type=="PERSON")
  type.gsub!("ORGANIZATION","INSTITUTION") if (type=="ORGANIZATION")
  return type
end

########

run;
