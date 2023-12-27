import email
from email.mime.application import MIMEApplication
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import re
import os
from email import encoders
from email.mime.base import MIMEBase
import html

acc = 'no_reply@gatelesis.com'
password = os.getenv('NO_REPLY_USER')
server = 'smtp.office365.com'
port = 587


def send_my_email(to, subject, body, attachments = [], body_type = 'html'):

    '''
        to = a string or a list of strings of email addresses
        subject = the subject of the email 
        body = the message you want to be sent 
        attachments = any files that need to be attached to the email

        description: This function's purpose is to be able to send an email regardless of the amount of recipients or attachments. It also gives 
        an error if the attachment is not found and if the email address is not a real one.
    '''
 
    if isinstance(to, str): #makes the recipients into a list of strings
        to = to.split(';')

    for i in to: 
        i = i.strip() #removes leading/trailing whitespace

        email = MIMEMultipart()
        email['to'] = i
        email['subject'] = subject

        email_pattern = r'[A-Za-z]+[0-9]*@[a-z]+\.[comorgnet]+'

        # Checks if email is in correct format
        if re.match(email_pattern, i):
            
            #If statement to transform the body type to always be html so it can be used in the right format for the body of the email
            if body_type == 'html':
                part1 = MIMEText(body, 'html')
            else:
                html_body = f"<html><body><pre>{html.escape(body)}</pre></body></html>"
                part1 = MIMEText(html_body, 'html')
            email.attach(part1)

            if attachments is None:
                attachments = []

            elif isinstance(attachments, str):
                attachments = [attachments]

            for j in attachments:
                file_exist = os.path.exists(j) #checks if path exists for the attachment
                
                if file_exist:
                    attachment_filename = os.path.basename(j) #gets the type of file the attachment is

                # Reads the file to be able to encode it so it can then be attached to the email 
                if os.path.exists(j):
                    with open(j, 'rb') as attachment_file:
                        file_data = attachment_file.read()
                        if file_data:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(file_data)
                            encoders.encode_base64(part)
                            part.add_header('Content-Disposition', f'attachment; filename={attachment_filename}')
                            email.attach(part)
                        else:
                            print(f"Error! Could not read file: {j}")
                else:
                    print(f"Error! File does not exist: {j}")

            
            # Logs into the smtp server to be able to actually log in and send the email 
            print(f"Email object: {email}")
            print(f"Email as string: {email.as_string()}")

            mail = smtplib.SMTP('smtp.office365.com', 587)
            mail.ehlo()
            mail.starttls()
            mail.login(acc, password)
            mail.sendmail(acc, i, email.as_string())
            mail.quit()
            
        else:
            print('Error! You submitted a wrong email address!')
            break