import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os

load_dotenv()

# Email credentials
 
receiver_email = "mohammedisaam28@gmail.com" 
password = os.environ.get("EMAIL_PASSWORD")
sender_email = os.environ.get("SENDER_EMAIL")
# Email content
subject = "Test Subject" 
body = "Test email body."

def send_email(receiver_email="mohammedisaam28@gmail.com", subject="Test Subject" , body="Test email body."):
    # Setup the MIME
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    # Attach the body text to the email
    message.attach(MIMEText(body, "plain"))

    # SMTP server setup
    try:
        # Create a secure SSL context
        server = smtplib.SMTP_SSL("smtpout.secureserver.net", 465)  # Use SSL
        server.login(sender_email, password)
        
        # Convert the message to a string and send it
        server.sendmail(sender_email, receiver_email, message.as_string())
        print("Email sent successfully!")
        
    except Exception as e:
        print(f"Failed to send email. Error: {e}")
    finally:
        server.quit()
#send_email()