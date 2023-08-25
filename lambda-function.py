import json  
import boto3                #   Amazon SDK
import pandas as pd         #   Formatting of excel file and performing operation on excel
import io                   #   Handling Input/Output
from io import BytesIO 
import botocore             #   ErrorHandling
import xlsxwriter           #   Writing to Excel
import awswrangler          #   Importing Openpyxl and performing complex read operations on it 

key = 'Account Details for S3 Versioning.xlsx'                  									# ^^^^INPUT FOLDER NAME/INPUT DOCUMENT NAME
bucket = 'mind-s3-versioning'                                  					# ^^^^BUCKET NAME
                    
s3=boto3.client('s3')   
file_object =s3.get_object(Bucket=bucket, Key=key)
file_content = file_object['Body'].read()
b_file_content = io.BytesIO(file_content)                                
df= pd.read_excel(b_file_content)                                                     # ^^^^df = dataframe for pandas
df_sheet_index = pd.read_excel(b_file_content, sheet_name=0)                          # ^^^^first sheet from excel is read
h_column_list_of_excel_file = df_sheet_index.columns.ravel().tolist()
b_file_content.close()    

acc_id=[]
acc_name=[]
account_id = []
name_missing_list = []
Comments = []
Reason_for_error = []
account_ID = []
Flag_for_name = False
Flag_for_bucket_permission_role_error = True 
acc_id_causing_error = []
acc_name_causing_error =[]
serial_number_for_comments_sheet = []
serial_number_for_comments = 0
Flag_for_id = False  
id_missing_list = [] 
accId=[] 
accName = [] 
accid_from_excel=df_sheet_index[h_column_list_of_excel_file[1]].tolist()
accName_from_excel=df_sheet_index[h_column_list_of_excel_file[2]].tolist() 
print(accid_from_excel) 
for i in range(len(accid_from_excel)):
    if pd.isnull(accid_from_excel[i]) == False :    
        accId.append(int(accid_from_excel[i])) 
        accName.append(accName_from_excel[i])
    else: 
        id_missing_list.append(i+1)
        Flag_for_id = True 
        Reason_for_error.append("Account Id Missing") 
        Comments.append("Account Id Missing at {}".format(i+1))
        acc_name_causing_error.append(accName_from_excel[i]) 
        acc_id_causing_error.append("")
        serial_number_for_comments = serial_number_for_comments + 1 
        serial_number_for_comments_sheet.append(serial_number_for_comments) 
    
print(accId)

for each in range(len(accName)): 
    if pd.isnull(accName[each])== False :   
        account_ID.append(accId[each])
        acc_name.append(accName[each]) 
    else:
        name_missing_list.append(i+1)
        Flag_for_name = True 
        Reason_for_error.append("Account Name Missing") 
        Comments.append("Account Name Missing at {}".format(each+1))
        acc_name_causing_error.append("")  
        acc_id_causing_error.append(accId[each])  
        serial_number_for_comments = serial_number_for_comments + 1 
        serial_number_for_comments_sheet.append(serial_number_for_comments) 
print(account_ID)         
for each in account_ID:
    account_id.append(str(each))
print(account_id) 

client = boto3.client('sts')
master_acc_id = client.get_caller_identity()['Account']
print(master_acc_id) 

for each in account_id:
    if len(each)==12:
        acc_id.append(each)
    else :
        N=12-len(each)
        each = each.rjust(N + len(each), '0')
        acc_id.append(each)  
  
rolearn = []  
for each in range(len(acc_id)):
    if acc_id[each] != master_acc_id:
        rolearn.append("arn:aws:iam::{}:role/Cross_Account_Role".format(acc_id[each]))   		# ^^^^ROLE NAME
dict_for_name = dict(zip(acc_id,acc_name))        
print(rolearn)
Flag_for_role_error = False
Flag_for_bucket_permission_role_error = False

#--------------------Status Report---------------------------------------------------------------------------
def generate_status(): 
    serial_number_for_comments_new = serial_number_for_comments
    serial_number = 0
    serial_number_stored_in_xlsx = [] 
    acc_id_stored_in_xlsx = []
    acc_name_stored_in_xlsx = []  
    bucket_stored_in_xlsx = []  
    bucket_with_status = []
    bucket_without_status = []
    Current_bucket_status = []
    Status = []
    Locked_Bucket = [] 
    Locked_Bucket_master =[] 
    
    for each in range(len(rolearn)): 
        try:
            sts_connection = boto3.client('sts') 
            acct_b = sts_connection.assume_role(
            RoleArn=rolearn[each],     
            RoleSessionName="Cross_Account_Role"                               					# ^^^^ROLE NAME
            )   
            
            ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
            SECRET_KEY = acct_b['Credentials']['SecretAccessKey']    
            SESSION_TOKEN = acct_b['Credentials']['SessionToken']
    
            s3 = boto3.client('s3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            aws_session_token=SESSION_TOKEN,
                )
            
            try:
                response = s3.list_buckets()
                ACC_ID = rolearn[each].split(":")[4]
                
                for bucket in response['Buckets']:                      					# ^^^^obtains the list of all buckets
                    print(bucket['Name'])
                    try:
                        response_for_locked_bucket = s3.get_object_lock_configuration(  
                        Bucket=bucket['Name'],    
                        ) 
                        if response_for_locked_bucket['ObjectLockConfiguration']['ObjectLockEnabled'] == 'Enabled':
                            Locked_Bucket.append(bucket['Name']) 
                            acc_id_stored_in_xlsx.append(ACC_ID)
                            serial_number = serial_number + 1      
                            serial_number_stored_in_xlsx.append(serial_number)  
                            for ac_id,name in dict_for_name.items(): 
                                if ac_id == ACC_ID: 
                                    acc_name_stored_in_xlsx.append(name)
                            Current_bucket_status = s3.get_bucket_versioning(
                            Bucket=bucket['Name']
                                ) 
                            k = ['MFADelete','Status']                                      			# ^^^^some buckets were not having the Status key and the MFADelete key 
                            if k[0] not in Current_bucket_status.keys() and k[1] not in Current_bucket_status.keys(): 
                                bucket_stored_in_xlsx.append(bucket['Name'])
                                Status.append("Default"+ "-Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket")
                            else :
                                if Current_bucket_status['Status'] == 'Suspended':
                                    bucket_stored_in_xlsx.append(bucket['Name'])
                                    Status.append(Current_bucket_status['Status']+ "-Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket")
                                else :
                                    bucket_stored_in_xlsx.append(bucket['Name'])
                                    Status.append(Current_bucket_status['Status']+ "-Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket")
                            
                            
                    except botocore.exceptions.ClientError as error:
                        acc_id_stored_in_xlsx.append(ACC_ID)   
                        serial_number = serial_number + 1      
                        serial_number_stored_in_xlsx.append(serial_number)
                        for ac_id,name in dict_for_name.items(): 
                            if ac_id == ACC_ID: 
                                acc_name_stored_in_xlsx.append(name) 
                        Current_bucket_status = s3.get_bucket_versioning(   
                        Bucket=bucket['Name']
                            )  
                        k = ['MFADelete','Status']                                      			# ^^^^some buckets were not having the Status key and the MFADelete key 
                        if k[0] not in Current_bucket_status.keys() and k[1] not in Current_bucket_status.keys(): 
                            bucket_stored_in_xlsx.append(bucket['Name'])
                            Status.append("Default")
                        else :
                            if Current_bucket_status['Status'] == 'Suspended':
                                bucket_stored_in_xlsx.append(bucket['Name'])
                                Status.append(Current_bucket_status['Status'])
                            else :
                                bucket_stored_in_xlsx.append(bucket['Name'])
                                Status.append(Current_bucket_status['Status'])
            except botocore.exceptions.ClientError as error:
                Flag_for_bucket_permission_role_error = True
                Comments.append(error)
                serial_number_for_comments_new = serial_number_for_comments_new + 1
                serial_number_for_comments_sheet.append(serial_number_for_comments_new)  
                Reason_for_error.append("Bucket Related")
                ACC_ID = rolearn[each].split(":")[4] 
                acc_id_causing_error.append(ACC_ID)
                for ac_id,name in dict_for_name.items(): 
                    if ac_id == ACC_ID: 
                        acc_name_causing_error.append(name) 
                
        except botocore.exceptions.ClientError as error:
            Flag_for_role_error = True
            print(error) 
            Comments.append(error)
            Reason_for_error.append("Assume Role Issue")
            serial_number_for_comments_new = serial_number_for_comments_new + 1
            serial_number_for_comments_sheet.append(serial_number_for_comments_new)
            ACC_ID = rolearn[each].split(":")[4]
            acc_id_causing_error.append(ACC_ID)
            for ac_id,name in dict_for_name.items(): 
                if ac_id == ACC_ID: 
                    acc_name_causing_error.append(name)  
            
                       
    for i in range(len(acc_id)):
        if acc_id[i]==master_acc_id:
            s3_master = boto3.client('s3')
            try:
                response = s3_master.list_buckets()
                
                for bucket in response['Buckets']:                      				# ^^^^obtains the list of all buckets
                    print(bucket['Name'])  
                    try:
                    
                        response_for_locked_bucket = s3_master.get_object_lock_configuration(  
                        Bucket=bucket['Name'],    
                        ) 
                        if response_for_locked_bucket['ObjectLockConfiguration']['ObjectLockEnabled'] == 'Enabled':
                            Locked_Bucket_master.append(bucket['Name'])
                            acc_id_stored_in_xlsx.append(acc_id[i])
                            serial_number = serial_number + 1      
                            serial_number_stored_in_xlsx.append(serial_number)     
                            acc_name_stored_in_xlsx.append(acc_name[i])
                            Current_bucket_status = s3_master.get_bucket_versioning(   
                            Bucket=bucket['Name']
                                ) 
                            k = ['MFADelete','Status']                                       		# ^^^^some buckets were not having the Status key and the MFADelete key 
                            if k[0] not in Current_bucket_status.keys() and k[1] not in Current_bucket_status.keys(): 
                                bucket_stored_in_xlsx.append(bucket['Name'])
                                Status.append("Default"+ "-Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket")
                            else :
                                if Current_bucket_status['Status'] == 'Suspended':
                                    bucket_stored_in_xlsx.append(bucket['Name'])
                                    Status.append(Current_bucket_status['Status']+ "-Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket")
                                else :
                                    bucket_stored_in_xlsx.append(bucket['Name'])
                                    Status.append(Current_bucket_status['Status']+ "-Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket")
                           
                    except botocore.exceptions.ClientError as error:
                        acc_id_stored_in_xlsx.append(acc_id[i])
                        serial_number = serial_number + 1      
                        serial_number_stored_in_xlsx.append(serial_number)     
                        acc_name_stored_in_xlsx.append(acc_name[i])
                        
                        Current_bucket_status = s3_master.get_bucket_versioning(   
                        Bucket=bucket['Name']
                            )  
                        k = ['MFADelete','Status']                                       			# ^^^^some buckets were not having the Status key and the MFADelete key 
                        if k[0] not in Current_bucket_status.keys() and k[1] not in Current_bucket_status.keys(): 
                            bucket_stored_in_xlsx.append(bucket['Name'])
                            Status.append("Default")
                        else :
                            if Current_bucket_status['Status'] == 'Suspended':
                                bucket_stored_in_xlsx.append(bucket['Name'])
                                Status.append(Current_bucket_status['Status'])
                            else :
                                bucket_stored_in_xlsx.append(bucket['Name'])
                                Status.append(Current_bucket_status['Status'])
                                
            except botocore.exceptions.ClientError as error:
                Flag_for_bucket_permission_role_error = True
                Comments.append(error)
                serial_number_for_comments_new = serial_number_for_comments_new + 1
                serial_number_for_comments_sheet.append(serial_number_for_comments_new)  
                Reason_for_error.append("Bucket Related")
                ACC_ID = rolearn[each].split(":")[4] 
                acc_id_causing_error.append(ACC_ID)
                for ac_id,name in dict_for_name.items(): 
                    if ac_id == ACC_ID: 
                        acc_name_causing_error.append(name) 
    list_new = Locked_Bucket + Locked_Bucket_master
    
    data={'S No ':serial_number_stored_in_xlsx, 'Account Id':acc_id_stored_in_xlsx, 'Account Name':acc_name_stored_in_xlsx,'Bucket Name': bucket_stored_in_xlsx,'Versioning Status':Status}
    data_frame=pd.DataFrame(data)
    
    data_for_error={'S.No':serial_number_for_comments_sheet, 'Account Id':acc_id_causing_error,'Account Name':acc_name_causing_error,'Possible Cause ':Reason_for_error, 'Comments':Comments}
    data_frame_error=pd.DataFrame(data_for_error)
    
    io_buffer = io.BytesIO()   
    s3 = boto3.resource('s3')  
    writer = pd.ExcelWriter(io_buffer, engine='xlsxwriter')
    sheets_in_writer=['Status','Comments']
    data_frame_for_writer=[data_frame, data_frame_error]
    for i,j in zip(data_frame_for_writer,sheets_in_writer):
        i.to_excel(writer,j,index=False)    
    workbook=writer.book
    header_format = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'FBB1A1','border': 1})
    max_col=4   
    header_format_comments = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'F2FBA1','border': 1}) 
    
    
    worksheet=writer.sheets["Status"]   
    
    for col_num, value in enumerate(data_frame.columns.values): 
        worksheet.write(0, col_num, value, header_format) 
        worksheet.set_column(1, 4, 20)
        worksheet.set_column(3,3,40)   
        
    worksheet=writer.sheets["Comments"]  
    
    for col_num, value in enumerate(data_frame_error.columns.values): 
        worksheet.write(0, col_num, value, header_format_comments)  
        worksheet.set_column(0,2,15)  
        worksheet.set_column(3,3,25)  
        worksheet.set_column(4,4,45)   
        
    filepath = 'Bucket Versioning Status.xlsx' 
    writer.save()     
    data = io_buffer.getvalue() 
    s3.Bucket('mind-s3-versioning').put_object(Key=filepath, Body=data)  
    io_buffer.close()   
    generate_status.has_been_called = True 
    
    

#------------ENABLES ALL THE BUCKETS------------------------------------------------
        
def enable_all():
    serial_number_for_comments_new = serial_number_for_comments
    serial_number = 0
    serial_number_stored_in_xlsx = [] 
    acc_id_stored_in_xlsx = []
    Status_stored_in_xlsx = []
    acc_name_stored_in_xlsx = []  
    bucket_stored_in_xlsx = [] 
    Locked_Bucket = [] 
    Locked_Bucket_master = [] 
    
    for each in range(len(rolearn)): 
        try:
            sts_connection = boto3.client('sts') 
            acct_b = sts_connection.assume_role(
            RoleArn=rolearn[each],     
            RoleSessionName="Cross_Account_Role"                               				# ^^^^ROLE NAME
            )   
            
            ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
            SECRET_KEY = acct_b['Credentials']['SecretAccessKey']    
            SESSION_TOKEN = acct_b['Credentials']['SessionToken']
    
        
            s3 = boto3.client('s3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            aws_session_token=SESSION_TOKEN,
                )
                
            try:
                response = s3.list_buckets()
                ACC_ID = rolearn[each].split(":")[4]
                
                for bucket in response['Buckets']:                      				# ^^^^obtains the list of all buckets
                    print(bucket['Name']) 
                    try:
                        response_for_locked_bucket = s3.get_object_lock_configuration(  
                        Bucket=bucket['Name'],    
                        ) 
                        if response_for_locked_bucket['ObjectLockConfiguration']['ObjectLockEnabled'] == 'Enabled':
                            Locked_Bucket.append(bucket['Name']) 
                            acc_id_stored_in_xlsx.append(ACC_ID)
                            serial_number = serial_number + 1      
                            serial_number_stored_in_xlsx.append(serial_number)
                            bucket_stored_in_xlsx.append(bucket['Name'])
                            for ac_id,name in dict_for_name.items(): 
                                if ac_id == ACC_ID: 
                                    acc_name_stored_in_xlsx.append(name) 
                            Current_bucket_status = s3.get_bucket_versioning(   
                            Bucket=bucket['Name']
                            ) 
                            Status_stored_in_xlsx.append(Current_bucket_status['Status'] + " -Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket") 
                            # Status_stored_in_xlsx.append("Enabled-Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket") 
                    except botocore.exceptions.ClientError as error:
                        bucket_stored_in_xlsx.append(bucket['Name'])
                        acc_id_stored_in_xlsx.append(ACC_ID)
                        # Status_stored_in_xlsx.append("Enabled")
                        serial_number = serial_number + 1      
                        serial_number_stored_in_xlsx.append(serial_number)     
                        for ac_id,name in dict_for_name.items(): 
                            if ac_id == ACC_ID: 
                                acc_name_stored_in_xlsx.append(name) 
                    
                        s3.put_bucket_versioning(Bucket=bucket['Name'],
                        VersioningConfiguration={
                            
                            'Status': 'Enabled',
                            'MFADelete': 'Disabled',
                         },
                         ) 
                        Current_bucket_status = s3.get_bucket_versioning(
                        Bucket=bucket['Name']
                            ) 
                        Status_stored_in_xlsx.append(Current_bucket_status['Status'])
            except botocore.exceptions.ClientError as error:
                Flag_for_bucket_permission_role_error = True
                Comments.append(error)
                serial_number_for_comments_new = serial_number_for_comments_new + 1
                serial_number_for_comments_sheet.append(serial_number_for_comments_new)  
                Reason_for_error.append("Bucket Related")
                ACC_ID = rolearn[each].split(":")[4] 
                acc_id_causing_error.append(ACC_ID)
                for ac_id,name in dict_for_name.items(): 
                    if ac_id == ACC_ID: 
                        acc_name_causing_error.append(name)  
                    
        except botocore.exceptions.ClientError as error:
            Flag_for_role_error = True
            print(error) 
            ACC_ID = rolearn[each].split(":")[4]
            Comments.append(error)
            Reason_for_error.append("Assume Role Related")
            acc_id_causing_error.append(ACC_ID)
            serial_number_for_comments_new = serial_number_for_comments_new + 1
            serial_number_for_comments_sheet.append(serial_number_for_comments_new) 
            for ac_id,name in dict_for_name.items(): 
                    if ac_id == ACC_ID: 
                        acc_name_causing_error.append(name) 
                         
                        
    for i in range(len(acc_id)):
        if acc_id[i]==master_acc_id:
            s3_master = boto3.client('s3')
            response = s3_master.list_buckets()
        
            for bucket in response['Buckets']:                      				# ^^^^obtains the list of all bucket
                print(bucket['Name']) 
                try:
                    response_for_locked_bucket = s3_master.get_object_lock_configuration(   
                    Bucket=bucket['Name'],    
                    ) 
                    if response_for_locked_bucket['ObjectLockConfiguration']['ObjectLockEnabled'] == 'Enabled':
                        Locked_Bucket_master.append(bucket['Name']) 
                        acc_id_stored_in_xlsx.append(acc_id[i])
                        serial_number = serial_number + 1      
                        serial_number_stored_in_xlsx.append(serial_number)
                        bucket_stored_in_xlsx.append(bucket['Name'])
                        acc_name_stored_in_xlsx.append(acc_name[i]) 
                        Current_bucket_status = s3_master.get_bucket_versioning(   
                            Bucket=bucket['Name']
                            ) 
                        Status_stored_in_xlsx.append(Current_bucket_status['Status'] + " -Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket") 
                        # Status_stored_in_xlsx.append("Enabled-Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket")
                     
                except botocore.exceptions.ClientError as error:        
                    bucket_stored_in_xlsx.append(bucket['Name'])
                    acc_id_stored_in_xlsx.append(acc_id[i])
                    # Status_stored_in_xlsx.append("Enabled")
                    serial_number = serial_number + 1      
                    serial_number_stored_in_xlsx.append(serial_number)     
                    acc_name_stored_in_xlsx.append(acc_name[i])
                    s3_master.put_bucket_versioning(Bucket=bucket['Name'],
                    VersioningConfiguration={
                        
                        'Status': 'Enabled',
                        'MFADelete': 'Disabled',
                     },
                     )   
                    Current_bucket_status = s3_master.get_bucket_versioning(
                        Bucket=bucket['Name']
                            )  
                    Status_stored_in_xlsx.append(Current_bucket_status['Status'])    
    list_new = Locked_Bucket_master + Locked_Bucket
    
    data={'S No ':serial_number_stored_in_xlsx, 'Account Id':acc_id_stored_in_xlsx, 'Account Name':acc_name_stored_in_xlsx,'Bucket Name': bucket_stored_in_xlsx,'Versioning Status':Status_stored_in_xlsx}
    data_frame=pd.DataFrame(data)
    data_for_error={'S.No':serial_number_for_comments_sheet, 'Account Id':acc_id_causing_error,'Account Name':acc_name_causing_error,'Possible Cause ':Reason_for_error, 'Comments':Comments}
    data_frame_error=pd.DataFrame(data_for_error)

    io_buffer = io.BytesIO()
    s3 = boto3.resource('s3')  
    writer = pd.ExcelWriter(io_buffer, engine='xlsxwriter')
    sheets_in_writer=['Enabled Buckets List','Comments']
    data_frame_for_writer=[data_frame, data_frame_error]
    for i,j in zip(data_frame_for_writer,sheets_in_writer):
        i.to_excel(writer,j,index=False)
    workbook=writer.book
    header_format = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'FBB1A1','border': 1})
    max_col=4   
    header_format_comments = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'F2FBA1','border': 1}) 
    
    
    worksheet=writer.sheets["Enabled Buckets List"]     
    
    for col_num, value in enumerate(data_frame.columns.values): 
        worksheet.write(0, col_num, value, header_format) 
        worksheet.set_column(1, 4, 20)
        worksheet.set_column(3,3,40)   
        
    worksheet=writer.sheets["Comments"]  
    
    for col_num, value in enumerate(data_frame_error.columns.values): 
        worksheet.write(0, col_num, value, header_format_comments)  
        worksheet.set_column(0,2,20) 
        worksheet.set_column(3,3,25)  
        worksheet.set_column(4,4,45)  
         
    filepath = 'Versioning-Enabled.xlsx'    									# ^^^^specify the name of excel file for saving the final excel sheet info
    writer.save()       
    data = io_buffer.getvalue()        
    s3.Bucket('mind-s3-versioning').put_object(Key=filepath, Body=data)  
    io_buffer.close()
    enable_all.has_been_called = True     

#------------------DISABLES ALL TTHE BUCKETS-----------------------------------------------------------
            
def suspend_all():
    
    serial_number_for_comments_new = serial_number_for_comments
    serial_number = 0
    serial_number_stored_in_xlsx = [] 
    acc_id_stored_in_xlsx = []
    Status_stored_in_xlsx = []
    acc_name_stored_in_xlsx = []  
    bucket_stored_in_xlsx = [] 
    Locked_Bucket = [] 
    Locked_Bucket_master = [] 
    
    for each in range(len(rolearn)): 
        try:
            
            sts_connection = boto3.client('sts') 
            acct_b = sts_connection.assume_role(
            RoleArn=rolearn[each],     
            RoleSessionName="Cross_Account_Role"                               			# ^^^^ROLE NAME
            )   
            
            ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
            SECRET_KEY = acct_b['Credentials']['SecretAccessKey']    
            SESSION_TOKEN = acct_b['Credentials']['SessionToken']
    
            s3 = boto3.client('s3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            aws_session_token=SESSION_TOKEN,
                )
                
            try:
                response = s3.list_buckets()
                ACC_ID = rolearn[each].split(":")[4]
                
                for bucket in response['Buckets']:                      			# ^^^^obtains the list of all buckets
                    print(bucket['Name']) 
                    try:
                        response_for_locked_bucket = s3.get_object_lock_configuration(  
                        Bucket=bucket['Name'],    
                        ) 
                        if response_for_locked_bucket['ObjectLockConfiguration']['ObjectLockEnabled'] == 'Enabled':
                            Locked_Bucket.append(bucket['Name']) 
                            acc_id_stored_in_xlsx.append(ACC_ID)
                            serial_number = serial_number + 1      
                            serial_number_stored_in_xlsx.append(serial_number)
                            bucket_stored_in_xlsx.append(bucket['Name'])
                            for ac_id,name in dict_for_name.items(): 
                                if ac_id == ACC_ID: 
                                    acc_name_stored_in_xlsx.append(name) 
                            Current_bucket_status = s3.get_bucket_versioning(   
                            Bucket=bucket['Name']
                            ) 
                            Status_stored_in_xlsx.append(Current_bucket_status['Status'] + " -Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket") 
                            # Status_stored_in_xlsx.append("Enabled-Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket") 
                    except botocore.exceptions.ClientError as error:
                        bucket_stored_in_xlsx.append(bucket['Name'])
                        acc_id_stored_in_xlsx.append(ACC_ID)
                        # Status_stored_in_xlsx.append("Suspended")
                        serial_number = serial_number + 1      
                        serial_number_stored_in_xlsx.append(serial_number)     
                        for ac_id,name in dict_for_name.items(): 
                            if ac_id == ACC_ID: 
                                acc_name_stored_in_xlsx.append(name) 
                        
                        s3.put_bucket_versioning(Bucket=bucket['Name'],
                        VersioningConfiguration={
                            
                            'Status': 'Suspended',
                            'MFADelete': 'Disabled',    
                         },
                         ) 
                        Current_bucket_status = s3.get_bucket_versioning(
                        Bucket=bucket['Name']
                            )  
                        Status_stored_in_xlsx.append(Current_bucket_status['Status']) 
                        
            except botocore.exceptions.ClientError as error:
                Flag_for_bucket_permission_role_error = True
                Comments.append(error)
                serial_number_for_comments_new = serial_number_for_comments_new + 1
                serial_number_for_comments_sheet.append(serial_number_for_comments_new)  
                Reason_for_error.append("Bucket Related")
                ACC_ID = rolearn[each].split(":")[4] 
                acc_id_causing_error.append(ACC_ID)
                for ac_id,name in dict_for_name.items(): 
                    if ac_id == ACC_ID: 
                        acc_name_causing_error.append(name)  
                    
        except botocore.exceptions.ClientError as error:
            Flag_for_role_error = True
            print(error) 
            ACC_ID = rolearn[each].split(":")[4]
            Comments.append(error)
            Reason_for_error.append("Assume Role Related")
            acc_id_causing_error.append(ACC_ID)
            serial_number_for_comments_new = serial_number_for_comments_new + 1
            serial_number_for_comments_sheet.append(serial_number_for_comments_new) 
            for ac_id,name in dict_for_name.items(): 
                    if ac_id == ACC_ID: 
                        acc_name_causing_error.append(name) 
                         
                        
    for i in range(len(acc_id)):
        if acc_id[i]==master_acc_id:
            s3_master = boto3.client('s3')
            response = s3_master.list_buckets()
        
            for bucket in response['Buckets']:                      				# ^^^^obtains the list of all bucket
                print(bucket['Name']) 
                try:
                    response_for_locked_bucket = s3_master.get_object_lock_configuration(   
                    Bucket=bucket['Name'],    
                    ) 
                    if response_for_locked_bucket['ObjectLockConfiguration']['ObjectLockEnabled'] == 'Enabled':
                        Locked_Bucket_master.append(bucket['Name']) 
                        acc_id_stored_in_xlsx.append(acc_id[i])
                        serial_number = serial_number + 1      
                        serial_number_stored_in_xlsx.append(serial_number)
                        bucket_stored_in_xlsx.append(bucket['Name'])
                        acc_name_stored_in_xlsx.append(acc_name[i])  
                        Current_bucket_status = s3_master.get_bucket_versioning(   
                            Bucket=bucket['Name']
                            ) 
                        Status_stored_in_xlsx.append(Current_bucket_status['Status'] + " -Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket") 
                        # Status_stored_in_xlsx.append("Enabled-Bucket Versioning can’t be suspended because Object Lock is enabled for this bucket")
                except botocore.exceptions.ClientError as error:
                    bucket_stored_in_xlsx.append(bucket['Name'])
                    acc_id_stored_in_xlsx.append(acc_id[i])
                    # Status_stored_in_xlsx.append("Suspended")    
                    serial_number = serial_number + 1      
                    serial_number_stored_in_xlsx.append(serial_number)     
                    acc_name_stored_in_xlsx.append(acc_name[i])
                    
                    s3_master.put_bucket_versioning(Bucket=bucket['Name'],
                    VersioningConfiguration={
                        
                        'Status': 'Suspended',
                        'MFADelete': 'Disabled',
                     },
                     )   
                    Current_bucket_status = s3_master.get_bucket_versioning(
                        Bucket=bucket['Name']
                            )  
                    Status_stored_in_xlsx.append(Current_bucket_status['Status'])  
    list_new = Locked_Bucket_master + Locked_Bucket
    
    data={'S No ':serial_number_stored_in_xlsx, 'Account Id':acc_id_stored_in_xlsx, 'Account Name':acc_name_stored_in_xlsx,'Bucket Name': bucket_stored_in_xlsx,'Versioning Status':Status_stored_in_xlsx}
    data_frame=pd.DataFrame(data)
    data_for_error={'S.No':serial_number_for_comments_sheet, 'Account Id':acc_id_causing_error,'Account Name':acc_name_causing_error,'Possible Cause ':Reason_for_error, 'Comments':Comments}
    data_frame_error=pd.DataFrame(data_for_error)
    
    io_buffer = io.BytesIO()
    s3 = boto3.resource('s3')  
    writer = pd.ExcelWriter(io_buffer, engine='xlsxwriter')
    sheets_in_writer=['Suspended Buckets List','Comments']
    data_frame_for_writer=[data_frame, data_frame_error]
    for i,j in zip(data_frame_for_writer,sheets_in_writer):
        i.to_excel(writer,j,index=False)
    workbook=writer.book
    header_format = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'FBB1A1','border': 1})
    max_col=4   
    header_format_comments = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'F2FBA1','border': 1}) 
    
    
    worksheet=writer.sheets["Suspended Buckets List"]     
    
    for col_num, value in enumerate(data_frame.columns.values): 
        worksheet.write(0, col_num, value, header_format) 
        worksheet.set_column(1, 4, 20)
        worksheet.set_column(3,3,40)   
        
    worksheet=writer.sheets["Comments"]  
    
    for col_num, value in enumerate(data_frame_error.columns.values): 
        worksheet.write(0, col_num, value, header_format_comments)  
        worksheet.set_column(0,2,15)  
        worksheet.set_column(3,3,25)  
        worksheet.set_column(4,4,45)  
         
    filepath = 'Versioning-Suspended.xlsx'    										# ^^^^specify the name of excel file for saving the final excel sheet info
    writer.save()       
    data = io_buffer.getvalue()        
    s3.Bucket('mind-s3-versioning').put_object(Key=filepath, Body=data)  
    io_buffer.close()
    suspend_all.has_been_called = True    



#------------------MENTION THE FUNCTION NAME FOR ENABLING OR DISABLING--------------------------------------        

def lambda_handler(event, context):
    suspend_all.has_been_called = False
    enable_all.has_been_called = False
    generate_status.has_been_called = False
    
    # suspend_all()     
    # enable_all()     
    generate_status()         
    
    
    if suspend_all.has_been_called and enable_all.has_been_called: 
        result = "Select only one action - Enable/Disable/Status"
    elif suspend_all.has_been_called: 
        result = "Versioning Suspended Successfully"
        if Flag_for_name == True or Flag_for_role_error == True or Flag_for_bucket_permission_role_error == True or Flag_for_id == True:
            result +="....Some entries are missing. Please check the comments sheet" 
    elif enable_all.has_been_called:
        result = "Enabled Successfully"
        if Flag_for_name == True or Flag_for_role_error == True or Flag_for_bucket_permission_role_error == True or Flag_for_id == True:
            result += "....Some entries are missing. Please check the comments sheet" 
    elif generate_status.has_been_called:
        result = "Status Generated"
        if Flag_for_name == True or Flag_for_role_error == True or Flag_for_bucket_permission_role_error == True or Flag_for_id == True:
            result += "....Some entries are missing. Please check the comments sheet" 
    else:          
        result = "Select one of the actions - Enable/Disable/Status"      
    return result 
