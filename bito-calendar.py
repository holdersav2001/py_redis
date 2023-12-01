import datetime
import csv

start_date = datetime.date(2022, 12, 12)
end_date = datetime.date(2023, 1, 18)

holidays = ['2022-11-24', '2022-12-26', '2023-01-02', '2023-01-16']
holidays = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in holidays]

calendar_dates = []
previous_dates = []
day_names = []
holiday_indicators = []

current_date = start_date

while current_date <= end_date:

  calendar_dates.append(str(current_date))
  day_names.append(current_date.strftime('%A'))

  if current_date in holidays:
      holiday_indicators.append('Y')
  elif current_date.weekday() in (5,6):
      holiday_indicators.append('W')
  else:
      holiday_indicators.append('N')

  prev_date = current_date - datetime.timedelta(days=1)
  while prev_date.weekday() in (5,6) or prev_date in holidays:
      prev_date -= datetime.timedelta(days=1)

  previous_dates.append(str(prev_date))

  current_date += datetime.timedelta(days=1)

# Print calendar as CSV  

print('calendar_date,previous_working_date,day_name,holiday_indicator')

#for i in range(len(calendar_dates)):
#  print(','.join([calendar_dates[i], previous_dates[i], day_names[i], holiday_indicators[i]]))

# Write calendar to a CSV file
output_file = 'C:\\Users\\holde\\Downloads\\calendar.csv'

with open(output_file, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['calendar_date', 'previous_working_date', 'day_name', 'holiday_indicator'])

    for i in range(len(calendar_dates)):
        csvwriter.writerow([calendar_dates[i], previous_dates[i], day_names[i], holiday_indicators[i]])

print(f'Data written to {output_file}')