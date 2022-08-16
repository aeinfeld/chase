#!/usr/bin/env python3

# This script parses a CSV file downloaded from the Chase bank website and
# finds entries that aren't yet added into a Google Sheet. If any are found,
# they are added into the Google Sheet in bold text (to alert you they are
# new).

## SETUP INSTRUCTIONS FOR MACOS
## 1. Install Homebrew
## 2. brew install python
## 3. Restart your terminal
## 4. pip3 install google-api-python-client
## 5. pip3 install oauth2client
## You will also need to modify config.json to match your spreadsheet and you
## will need to obtain a Google Sheets API token which should be saved as
## token.json

from datetime import datetime, date, timedelta
from decimal import Decimal
from httplib2 import Http
import json
import os
import sys

from googleapiclient.discovery import build
from oauth2client import file, client, tools


CONFIG_FILENAME = 'config.json'  # Should be in same directory as this file
CLASSIFICATIONS_FILENAME = 'classifications.json'  # Should be in same directory as this file
SCOPES = 'https://www.googleapis.com/auth/spreadsheets'  # If modifying these scopes, delete the file token.json
RANGE_TO_CHECK = timedelta(days=7 * 4)  # 4 weeks


class ConfigData:
  @staticmethod
  def parse():
    current_path = os.path.dirname(__file__)
    config_filename = os.path.join(current_path, CONFIG_FILENAME)
    parsed_json = json.load(open(config_filename))

    return_value = ConfigData()
    return_value.spreadsheet_id = parsed_json.pop('spreadsheet_id')
    return_value.sheet_name = parsed_json.pop('sheet_name')
    return_value.local_directory = parsed_json.pop('local_directory')
    return_value.last4 = parsed_json.pop('last4')

    if parsed_json:
      print('WARNING: Unexpected keys in %s: %s' % (CONFIG_FILENAME, sorted(parsed_json.keys())))

    return return_value


class ClassificationType:
  FULL = 'full'
  PREFIX = 'prefix'


class Classification:
  @staticmethod
  def parse_single(json_entry):
    classification_type = json_entry.get('type')
    if classification_type not in (ClassificationType.FULL, ClassificationType.PREFIX):
      print('ERROR: Unrecognized classification type in %s' % json_entry)
      return None

    classification_match = json_entry.get('match')
    if not classification_match:
      print('ERROR: Missing classification match in %s' % json_entry)
      return None

    classification_category = json_entry.get('category')
    if not classification_category:
      print('ERROR: Missing classification category in %s' % json_entry)
      return None

    classification_quarter = json_entry.get('quarter')
    if classification_quarter and classification_quarter != 'auto':  # Right now this can be missing or 'auto'
      print('ERROR: Unrecognized classification quarter in %s' % json_entry)
      return None

    classification_subcategory = json_entry.get('subcategory') or ''  # Optional

    return_value = Classification()
    return_value.type = classification_type
    return_value.match = classification_match
    return_value.category = classification_category
    return_value.subcategory = classification_subcategory
    return_value.quarter = classification_quarter

    return return_value

  @staticmethod
  def parse():
    current_path = os.path.dirname(__file__)
    classifications_filename = os.path.join(current_path, CLASSIFICATIONS_FILENAME)
    parsed_json = json.load(open(classifications_filename))

    return_value = []
    for json_entry in parsed_json:
      classification = Classification.parse_single(json_entry)
      if classification:
        return_value.append(classification)

    return return_value

  @staticmethod
  def find(classifications, description):
    def normalize_string(s):
      return s.lower().strip()

    normalized_description = normalize_string(description)

    full_match_classifications = [classification for classification in classifications if classification.type == ClassificationType.FULL]
    prefix_classifications = [classification for classification in classifications if classification.type == ClassificationType.PREFIX]

    for classification in full_match_classifications:
      if normalized_description == normalize_string(classification.match):
        return classification

    for classification in prefix_classifications:
      if normalized_description.startswith(normalize_string(classification.match)):
        return classification

    return None


def get_sheets_service():
  class FakeFlags:
    def __init__(self):
      self.logging_level = 'ERROR'
      self.noauth_local_webserver = False
      self.auth_host_name = 'localhost'
      self.auth_host_port = [8080, 8090]

  current_path = os.path.dirname(__file__)

  store = file.Storage(os.path.join(current_path, 'token.json'))
  creds = store.get()
  if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets(os.path.join(current_path, 'credentials.json'), SCOPES)
    creds = tools.run_flow(flow, store, FakeFlags())
  service = build('sheets', 'v4', http=creds.authorize(Http()))

  return service


def get_sheet_id(config_data, service):
  sheets = service.spreadsheets().get(spreadsheetId=config_data.spreadsheet_id).execute().get('sheets', [])
  for sheet in sheets:
    if sheet.get('properties', {}).get('title') == config_data.sheet_name:
      return sheet['properties']['sheetId']
  return None


def datetime_to_sheets_days(dt):
  """
  Convert datetime object to sheets days format
  """
  # convert date objects to datetimes so that we can get sub-date time values
  # datetime.datetime is a subclass of datetime.date, so we have to check
  # isinstance in the proper order to differentiate
  desired_date = None
  if isinstance(dt, datetime):
    desired_date = dt
  elif isinstance(dt, date):
    desired_date = datetime(dt.year, dt.month, dt.day)
  assert isinstance(desired_date, datetime)

  SECONDS_PER_DAY = 60 * 60 * 24
  # Google sheets uses days since December 30, 1899 as its number format
  SHEETS_START_DAY = datetime(1899, 12, 30)

  delta = desired_date - SHEETS_START_DAY
  days = delta.total_seconds() / SECONDS_PER_DAY
  return days


def get_chase_csv_filename_or_abort(config_data):
  downloads_directory = os.path.expanduser(config_data.local_directory)
  filenames = os.listdir(os.path.expanduser(config_data.local_directory))
  filenames = [filename for filename in filenames if filename.startswith('Chase%s_Activity' % config_data.last4) and filename.upper().endswith('.CSV')]
  if len(filenames) == 0:
    print('ERROR: Could not find chase CSV file in %s' % downloads_directory)
    sys.exit(1)
  if len(filenames) > 1:
    print('ERROR: Found multiple chase CSV files in %s: %s' % (downloads_directory, filenames))
    sys.exit(1)

  filename = os.path.join(downloads_directory, filenames[0])
  return filename


def get_chase_csv_transactions(chase_csv_filename):
  entries = []
  is_first_line = True
  with open(chase_csv_filename, 'r') as file_handle:
    for line in file_handle:
      line = line.strip()
      if not line:
        continue

      if is_first_line:
        if line != 'Transaction Date,Post Date,Description,Category,Type,Amount,Memo':
          print('WARNING: Unexpected CSV header line -- has the Chase CSV format changed?')
        is_first_line = False
        continue

      # Chase CSV doesn't use a real CSV format -- commas aren't handled with quotes
      # So we need to rely on the expected column format
      line_split = line.split(',', 2)
      transaction_date_string = line_split[0]
      post_date_string = line_split[1]
      description, category, entry_type, amount_string, memo = line_split[2].rsplit(',', 4)  # account for the fact description can contain commas
      description = description.replace('&amp;', '&')  # for some reason ampersands are escaped in html style
      description = ' '.join(description.split())  # Get rid of extra consecutive spaces in description

      if entry_type in ['Type', 'Payment', 'Adjustment']:
        # ignore these
        # 'Type' is the first line header
        # 'Payment' is a confirmation of our credit card payment
        # 'Adjustment' is cash back -- not currently logging, but could in the future
        continue

      if entry_type not in ['Sale', 'Return', 'Fee']:
        print('WARNING: Unknown CSV line type %s' % entry_type)
        continue

      transaction_date_split = transaction_date_string.split('/')
      transaction_date = date(
        int(transaction_date_split[2]),
        int(transaction_date_split[0]),
        int(transaction_date_split[1]),
      )

      entries.append((transaction_date, Decimal(amount_string), description))

  # Sort by transaction date
  # This didn't used to be necessary, but at some point, the Chase CSVs started
  # returning the transactions in a garbled order
  entries.sort(key=lambda x: x[0], reverse=True)

  return entries


def get_spreadsheet_transactions(config_data, service):
  sheet = service.spreadsheets()
  range_to_fetch = '%s!A2:C' % config_data.sheet_name
  result = sheet.values().get(spreadsheetId=config_data.spreadsheet_id, range=range_to_fetch).execute()
  values = result.get('values', [])

  entries = []
  for row in values:
    transaction_date = datetime.strptime(row[0], '%B %d, %Y').date()
    amount_string = row[1].replace('$', '').replace(',', '')
    description = row[2]

    entries.append((transaction_date, Decimal(amount_string), description))

  return entries


def get_transaction_set_without_amount(transactions):
  set_without_amount = set()
  for transaction_date, amount, description in transactions:
    set_without_amount.add((transaction_date, description))
  return set_without_amount


def is_transaction_in_non_amount_set(transaction, set_without_amount):
  transaction_date, amount, description = transaction
  return (transaction_date, description) in set_without_amount


def get_missing_transactions(chase_transactions, spreadsheet_transactions):
  recent_transactions = [transaction for transaction in chase_transactions if date.today() - transaction[0] <= RANGE_TO_CHECK]

  # Remove transactions from chase_transactions that are in spreadsheet_transactions
  spreadsheet_transactions_set = set(spreadsheet_transactions)
  initial_missing_transactions = [transaction for transaction in recent_transactions if transaction not in spreadsheet_transactions_set]

  # Then, ignore ones where only money differs (but print warning)
  spreadsheet_transactions_slim_set = get_transaction_set_without_amount(spreadsheet_transactions)
  final_missing_transactions = []
  for transaction in initial_missing_transactions:
    if is_transaction_in_non_amount_set(transaction, spreadsheet_transactions_slim_set):
      print('WARNING: [%s] %s has differing money (%s on Chase CSV)' % (transaction[0], transaction[2], transaction[1]))
    else:
      final_missing_transactions.append(transaction)

  return final_missing_transactions


def add_transactions_to_spreadsheet(config_data, service, sheet_id, transactions_to_add, chase_transactions, spreadsheet_transactions, classifications):
  requests = []
  for transaction_index, transaction in enumerate(transactions_to_add):
    row_number = determine_row_number_for_transaction(transaction, chase_transactions, spreadsheet_transactions)
    row_number += transaction_index  # Take into account rows we've already added
    requests += get_spreadsheet_requests_for_transaction(sheet_id, transaction, row_number, classifications)

  body = {
    'requests': requests,
  }
  service.spreadsheets().batchUpdate(spreadsheetId=config_data.spreadsheet_id, body=body).execute()


def determine_row_number_for_transaction(transaction, chase_transactions, spreadsheet_transactions):
  # Find the first transaction in spreadsheet_transactions
  # that is in chase_transactions but after transaction
  transaction_index = chase_transactions.index(transaction)
  chase_transactions_slim_set = get_transaction_set_without_amount(chase_transactions[transaction_index + 1:])

  row_number = 0
  for transaction in spreadsheet_transactions:
    row_number += 1
    if is_transaction_in_non_amount_set(transaction, chase_transactions_slim_set):
      break

  return row_number


def get_spreadsheet_requests_for_transaction(sheet_id, transaction, row_number, classifications):
  transaction_date, amount, description = transaction
  classification = Classification.find(classifications, description)

  row_data = [
    {
      'userEnteredValue': {
        'numberValue': datetime_to_sheets_days(transaction_date),
      },
      'userEnteredFormat': {
        'numberFormat': {
          'pattern': 'mmmm d, yyy',
          'type': 'DATE',
        },
        'textFormat': {
          'fontFamily': 'Arial',
          'fontSize': 10,
        },
      },
    },
    {
      'userEnteredValue': {
        'numberValue': str(amount),
      },
      'userEnteredFormat': {
        'numberFormat': {
          'pattern': '$0.00',
          'type': 'CURRENCY',
        },
        'textFormat': {
          'fontFamily': 'Arial',
          'fontSize': 10,
        },
      },
    },
    {
      'userEnteredValue': {
        'stringValue': description,
      },
      'userEnteredFormat': {
        'textFormat': {
          'fontFamily': 'Arial',
          'fontSize': 10,
          'bold': True,  # Bold the description to make it clear what is "new"
        },
      },
    },
  ]

  if classification:
    if classification.quarter == 'auto':
      quarter_int = (transaction_date.month + 2) // 3
      quarter_value = '%d Q%d' % (transaction_date.year, quarter_int)
    else:
      quarter_value = ''

    row_data.append({
      'userEnteredValue': {
        'stringValue': quarter_value,
      },
      'userEnteredFormat': {
        'textFormat': {
          'fontFamily': 'Arial',
          'fontSize': 10,
          'bold': True,  # Bold to make it clear what is "new"
        },
      },
    })
    row_data.append({
      'userEnteredValue': {
        'stringValue': classification.category,
      },
      'userEnteredFormat': {
        'textFormat': {
          'fontFamily': 'Arial',
          'fontSize': 10,
          'bold': True,  # Bold to make it clear what is "new"
        },
      },
    })
    if classification.subcategory:
      row_data.append({
        'userEnteredValue': {
          'stringValue': classification.subcategory,
        },
        'userEnteredFormat': {
          'textFormat': {
            'fontFamily': 'Arial',
            'fontSize': 10,
            'bold': True,  # Bold to make it clear what is "new"
          },
        },
      })


  insert_row_request = {
    'insertDimension': {
      'range': {
        'sheetId': str(sheet_id),
        'dimension': 'ROWS',
        'startIndex': row_number,
        'endIndex': row_number + 1,
      },
    },
  }

  update_cells_request = {
    'updateCells': {
      'start': {
        'sheetId': str(sheet_id),
        'rowIndex': row_number,
        'columnIndex': 0,
      },
      'rows': [
        {
          'values': row_data,
        },
      ],
      'fields': '*',
    },
  }

  return [
    insert_row_request,
    update_cells_request,
  ]


def get_oldest_transaction_day(transactions):
  oldest_day = None
  for transaction in transactions:
    transaction_date, amount, description = transaction
    if oldest_day is None:
      oldest_day = transaction_date
    else:
      oldest_day = min(oldest_day, transaction_date)
  return oldest_day


def main():
  config_data = ConfigData.parse()
  classifications = Classification.parse()

  chase_csv_filename = get_chase_csv_filename_or_abort(config_data)
  chase_transactions = get_chase_csv_transactions(chase_csv_filename)

  service = get_sheets_service()
  sheet_id = get_sheet_id(config_data, service)
  spreadsheet_transactions = get_spreadsheet_transactions(config_data, service)

  transactions_to_add = get_missing_transactions(chase_transactions, spreadsheet_transactions)
  if transactions_to_add:
    print('Adding %d new transactions' % len(transactions_to_add))
    add_transactions_to_spreadsheet(config_data, service, sheet_id, transactions_to_add, chase_transactions, spreadsheet_transactions, classifications)
    print('Success! Oldest added day is: %s' % get_oldest_transaction_day(transactions_to_add).strftime('%B %-d, %Y'))
  else:
    print('No new transactions to add')

  os.remove(chase_csv_filename)


if __name__ == '__main__':
  main()
