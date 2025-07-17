
import pyotp
from Alice_Module import credentials
from Logger_Module import *
import config
from My_Logger import setup_logger, LogLevel
logger = setup_logger(logger_name="Auto Login", log_level=LogLevel.INFO, log_to_console=config.print_logger)


# from alice_blue import *


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time


# Creds
data = credentials()

userid = data[0]

password_string = data[1]

totp_key = data[6]  # YOUR TOTP KEY

# you have to replace only 1. userid 2. password_string 3.totp_key 4. api_key {below} of your own for this code to work.




# code for headless
# Set up Chrome options for headless mode // LOGIN  WITHOUT  OPENING BROSWER
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run browser in headless mode
chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (useful for headless mode)
chrome_options.add_argument("--no-sandbox")  # Disable sandboxing (sometimes required in headless mode)


# Initialize WebDriver
# service = Service("chromedriver.exe")  # enter the path of chromedrive, if in same folder you can use this address{chromedriver.exe} else use c/folder/wheer_chromedriver/chromedriver.exe


# driver = webdriver.Chrome(service=service, options=chrome_options)
driver = webdriver.Chrome(options=chrome_options)



# Step 1: Open Alice Blue login page
driver.get("https://ant.aliceblueonline.com/")


# Step 1.5: Click on the initial login button to proceed to the next input step
initial_login_button = driver.find_element(By.ID, "initial_loginByUserId")
initial_login_button.click()


#----------------------------- CODE FOR lOGIN ON CHROME AUTO LOGIN ------------------------------ #

# Step 2: Enter User ID
user_id = driver.find_element(By.ID, "userid_inp")
user_id.send_keys(userid)  # Replace with your User ID

# Step 3: Click "Next" after User ID
next_button_userid = driver.find_element(By.ID, "userId_btn_label")
next_button_userid.click()

# Step 4: Wait for the Password field to appear and enter Password
password = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.ID, "password_inp"))
)
password.send_keys(password_string)  # Replace with your Password

# Step 5: Click "Next" after Password
next_button_password = driver.find_element(By.ID, "password_btn_label")
next_button_password.click()

# Step 6: Wait for the TOTP field to appear and enter TOTP
totp_field = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.ID, "totp_otp_inp"))
)

# Replace 'your_secret_key' with your actual TOTP key (this should be a base32 string)
totp = pyotp.TOTP(totp_key)

# Generate the current TOTP code
current_totp = totp.now()

print("Generated TOTP:", current_totp)


totp_field.send_keys(current_totp)  # Replace with your TOTP (generated from an authenticator app)

# Step 7: Click "Next" after TOTP
# next_button_totp = driver.find_element(By.ID, "totp_btn_label")
# next_button_totp.click()

# Step 8: Wait for successful login (replace with specific logic for your dashboard)
time.sleep(5)  # Adjust based on the loading time of the dashboard

# Optional: Print current URL to confirm successful login
msg=f"Logged in! Current URL: {driver.current_url}"
# my_logger(data_to_log=msg, fn='AutoLogin', bot=True)
logger.info(msg)

# Cleanup: Close the browser
driver.quit()


#
# #   CREATING A ALICE OBJECT
#
# alice = Aliceblue(user_id=userid,api_key='7alksdfgjlasd gfasdlgasldfgkjadflasdlasidjhoiagasdgnaskfgj')  # enter your userid and api_key
#
#
# # trying to get session id after , auto chrome login with selenium
#
# print(alice.get_session_id()) # Get Session ID
#
#
# profile = alice.get_profile()
#
# print(profile)
#
#
# try:
#     if profile['stat'] == 'Not_ok':
#
#         print("can't create profile. login failed")
#
#
# except Exception as e:
#
#     print(e)
#
#
# if profile['accountStatus'] == 'Activated':
#
#     print(profile)
#
#     print(f"Logged in successufly, welcome {profile['accountName']}")
#
#
# elif profile['stat'] == 'Not_ok':
#
#     print(f"Alice blue login failed")
#
#
# # you can check balace for double confirmation
# alice.get_balance()


