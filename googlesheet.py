import gspread

def logger(text):
    print(text)

class Spread:
    
    def __init__(self, spreadsheet, sheet):
        self.sh_name = spreadsheet
        self.wks_name = sheet
        self.mail_id = None
        self.gc = gspread.service_account(filename='TradeAB.json')
        self.sh = self.gc.open(self.sh_name)
        self.wks = self.sh.worksheet(self.wks_name)

    def create_spreadsheet(self, name):
        if self.mail_id is not None:
            sh = self.gc.create(name)
            sh.share(self.mail_id, perm_type='user', role='writer')
        else: 
            logger("Assign mail id first.")
    # But that new spreadsheet will be visible only to your script's account.
    # To be able to access newly created spreadsheet you *must* share it
    # with your email. Which brings us to…

    def add_sheet(self, name):
        worksheet = self.sh.add_worksheet(title=name, rows="100", cols="100")
        return worksheet

    def worksheet_len(self):
        list_of_lists = self.wks.get_values()
        return len(list_of_lists)

    def get_all_records(self):
        """return all records in dict"""
        return self.wks.get_all_records()

    def append_row(self, valuelist):
        self.wks.append_row(valuelist)

    def update_df(self,df):
        self.wks.update([df.columns.values.tolist()] + df.values.tolist())

    def all_wks(self):
        print(self.gc.list_spreadsheet_files())