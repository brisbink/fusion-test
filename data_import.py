'''
Created on Jan 10, 2012

@author: Kathryn Hurley
'''


from authorization.clientlogin import ClientLogin
from sql.sqlbuilder import SQL
import ftclient
from fileimport.fileimporter import CSVImporter

import time
import traceback

if __name__ == "__main__":

  import sys, getpass
  try:
    username = sys.argv[1]
    filepath = sys.argv[2]
    tableid = sys.argv[3]
    password = sys.argv[4]
  except:
    print 'Usage: python data_import.py <username> <filepath> <tableid> <pass>'
    sys.exit()
  # password = getpass.getpass("Enter your password: ")

  try:
    token = ClientLogin().authorize(username, password)
    ft_client = ftclient.ClientLoginFTClient(token)
  except:
    print 'oops, couldn\'t log you in. Check your username and password'
    sys.exit()

  ## Commenting out "while" loop to execute only once on command from another
  ## script. restore this line and the sleep line below to restore auto loop,
  ## and re-indent the material below the while.
  # while True:

  #import a table from CSV file
  ft_client.query('DELETE FROM ' + tableid)
  cols = ["Test",
  "ElectionDate",
  "StatePostal",
  "XML_id",
  "FIPSCode",
  "UnitName",
  "Race_ID",
  "OfficeID",
  "TypeID",
  "COL_J",
  "OfficeName",
  "COL_L",
  "Party",
  "Type",
  "COL_O",
  "COL_P",
  "COL_Q",
  "Precincts_Reporting",
  "Precincts_Total",
  "Cand_1_ID",
  "Cand_1_Unk",
  "Cand_1_Party",
  "Cand_1_First",
  "Cand_1_Middle",
  "Cand_1_Last",
  "Cand_1_Suffix",
  "Cand_1_Unk3",
  "Cand_1_Unk4",
  "Cand_1_VoteCount",
  "Cand_1_Unk5",
  "Cand_1_PolID",
  "Cand_2_ID",
  "Cand_2_Unk",
  "Cand_2_Party",
  "Cand_2_First",
  "Cand_2_Middle",
  "Cand_2_Last",
  "Cand_2_Suffix",
  "Cand_2_Unk3",
  "Cand_2_Unk4",
  "Cand_2_VoteCount",
  "Cand_2_Unk5",
  "Cand_2_PolID",
  "Cand_3_ID",
  "Cand_3_Unk",
  "Cand_3_Party",
  "Cand_3_First",
  "Cand_3_Middle",
  "Cand_3_Last",
  "Cand_3_Suffix",
  "Cand_3_Unk3",
  "Cand_3_Unk4",
  "Cand_3_VoteCount",
  "Cand_3_Unk5",
  "Cand_3_PolID",
  "Cand_4_ID",
  "Cand_4_Unk",
  "Cand_4_Party",
  "Cand_4_First",
  "Cand_4_Middle",
  "Cand_4_Last",
  "Cand_4_Suffix",
  "Cand_4_Unk3",
  "Cand_4_Unk4",
  "Cand_4_VoteCount",
  "Cand_4_Unk5",
  "Cand_4_PolID",
  "Cand_5_ID",
  "Cand_5_Unk",
  "Cand_5_Party",
  "Cand_5_First",
  "Cand_5_Middle",
  "Cand_5_Last",
  "Cand_5_Suffix",
  "Cand_5_Unk3",
  "Cand_5_Unk4",
  "Cand_5_VoteCount",
  "Cand_5_Unk5",
  "Cand_5_PolID",
  "Cand_6_ID",
  "Cand_6_Unk",
  "Cand_6_Party",
  "Cand_6_First",
  "Cand_6_Middle",
  "Cand_6_Last",
  "Cand_6_Suffix",
  "Cand_6_Unk3",
  "Cand_6_Unk4",
  "Cand_6_VoteCount",
  "Cand_6_Unk5",
  "Cand_6_PolID",
  "Cand_7_ID",
  "Cand_7_Unk",
  "Cand_7_Party",
  "Cand_7_First",
  "Cand_7_Middle",
  "Cand_7_Last",
  "Cand_7_Suffix",
  "Cand_7_Unk3",
  "Cand_7_Unk4",
  "Cand_7_VoteCount",
  "Cand_7_Unk5",
  "Cand_7_PolID",
  "Cand_8_ID",
  "Cand_8_Unk",
  "Cand_8_Party",
  "Cand_8_First",
  "Cand_8_Middle",
  "Cand_8_Last",
  "Cand_8_Suffix",
  "Cand_8_Unk3",
  "Cand_8_Unk4",
  "Cand_8_VoteCount",
  "Cand_8_Unk5",
  "Cand_8_PolID",
  "Cand_9_ID",
  "Cand_9_Unk",
  "Cand_9_Party",
  "Cand_9_First",
  "Cand_9_Middle",
  "Cand_9_Last",
  "Cand_9_Suffix",
  "Cand_9_Unk3",
  "Cand_9_Unk4",
  "Cand_9_VoteCount",
  "Cand_9_Unk5",
  "Cand_9_PolID",
  "Cand_10_ID",
  "Cand_10_Unk",
  "Cand_10_Party",
  "Cand_10_First",
  "Cand_10_Middle",
  "Cand_10_Last",
  "Cand_10_Suffix",
  "Cand_10_Unk3",
  "Cand_10_Unk4",
  "Cand_10_VoteCount",
  "Cand_10_Unk5",
  "Cand_10_PolID",
  "Cand_11_ID",
  "Cand_11_Unk",
  "Cand_11_Party",
  "Cand_11_First",
  "Cand_11_Middle",
  "Cand_11_Last",
  "Cand_11_Suffix",
  "Cand_11_Unk3",
  "Cand_11_Unk4",
  "Cand_11_VoteCount",
  "Cand_11_Unk5",
  "Cand_11_PolID"]
  response = CSVImporter(ft_client).importMoreRows(filepath, tableid, cols=cols, delimiter=';')
  print response
  print "Fusion Table upload script complete"
  # time.sleep(120) # sleep for 2 minutes
