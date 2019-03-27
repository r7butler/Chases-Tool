import pandas as pd
from sqlalchemy import create_engine
import numpy as np

print 'reading in initial data'
initial = pd.read_excel("/nfauna_QC_B13_OCSD.xlsm", sheet_name = 'Original_Data')

print 'reading in the QC data'
qc = pd.read_excel("Infauna_QC_B13_OCSD.xlsm", sheet_name = 'QC_Data')
qc.rename(columns={" SITE": "SITE"}, inplace = True)

print 'reading in the original discrepancy report'
original_report = pd.read_excel("Infauna_QC_B13_OCSD.xlsm", sheet_name = 'Discrepancy_Report')


print 'performing merge'
output = pd.merge(initial, qc, left_on = ["SITE", "ORIGINAL SPECIES"], right_on = ['SITE', 'QC SPECIES'], how = 'outer')

print 're-ordering the columns'
output = output[['SITE', 'ORIGINAL SPECIES', 'ORIGINAL ABUNDANCE', 'ORIGINAL VOUCHER', 'QC SPECIES', 'QC ABUNDANCE']]


def discrepancy_check(row):
    'A function to be applied to the "output" dataframe row by row'
    if ((pd.isnull(row['ORIGINAL SPECIES'])) or (row['ORIGINAL SPECIES'] == '')):
        row['Match/No Match'] = 'Not Match'
        row['Type'] = 'ID'
    else:
        if ((pd.isnull(row['QC SPECIES'])) or (row['QC SPECIES'] == '')):
            if row['ORIGINAL ABUNDANCE'] == row['ORIGINAL VOUCHER']:
                row['Match/No Match'] = 'Match'
                row['Type'] = ''
            else:
                row['Match/No Match'] = 'Not Match'
                row['Type'] = 'ID'
        else:
            if not pd.isnull(row['ORIGINAL VOUCHER']):
                if row['QC ABUNDANCE'] == row['ORIGINAL ABUNDANCE'] - row['ORIGINAL VOUCHER']:
                    row['Match/No Match'] = 'Match'
                    row['Type'] = ''
                else:
                    row['Match/No Match'] = 'Not Match'
                    row['Type'] = 'Count'
            else:
                if row['QC ABUNDANCE'] == row['ORIGINAL ABUNDANCE']:
                    row['Match/No Match'] = 'Match'
                    row['Type'] = ''
                else:
                    row['Match/No Match'] = 'Not Match'
                    row['Type'] = 'Count'


    return pd.Series([row['Match/No Match'], row['Type']])

print 'building discrepancy report'
output[['Match/No Match', 'Type']] = output.apply(discrepancy_check, axis=1)
                
print 'ordering the rows'
output.sort_values(['SITE', 'Match/No Match', 'ORIGINAL SPECIES', 'QC SPECIES'], ascending = [True, False, True, True], inplace = True)

print "sorting the original report to match that of the new one created with python"
#sorted_original_report = original_report.rename(columns={'Match /           Not Match': 'Match/No Match'},inplace=True)
sorting_columns = ['SITE', 'Match /           Not Match', 'ORIGINAL SPECIES', 'QC SPECIES']
sorted_original_report = original_report.sort_values(sorting_columns, ascending = [True, False, True, True])




print "Initial:"
print initial.head()
print '\n'
print "QC:"
print qc.head()
print '\n'
print "Discrepancy Report:"
print output.head()
print '\n'
print "sorted original report:"
print sorted_original_report.head()
print '\n'
print 'original report:'
print original_report.head()


sorted_original_report.replace(np.nan, '', inplace=True)
output.replace(np.nan, '', inplace=True)

rows = zip(output['SITE'], 
           output['ORIGINAL SPECIES'], 
           output['ORIGINAL VOUCHER'], 
           output['QC SPECIES'], 
           output['QC ABUNDANCE'], 
           output['Match/No Match'], 
           output['Type'])

original_rows = zip(sorted_original_report['SITE'], 
                    sorted_original_report['ORIGINAL SPECIES'], 
                    sorted_original_report['ORIGINAL VOUCHER'], 
                    sorted_original_report['QC SPECIES'], 
                    sorted_original_report['QC ABUNDANCE'], 
                    sorted_original_report['Match /           Not Match'], 
                    sorted_original_report['Type'])

rows = set(rows)
original_rows = set(original_rows)

print '\n'
print "Rows that were in the original that are not in the python generated report:"
for row in original_rows - rows:
    print '   SITE:               %s' % row[0]
    print '   Original Species:   %s' % row[1]
    print '   Original Voucher:   %s' % row[2]
    print '   QC Species:         %s' % row[3]
    print '   QC Abundance:       %s' % row[4]
    print '   Match or No Match:  %s' % row[5]
    print '   Type:               %s' % row[6]
    print '\n'

print '\n'
print "Rows that are in the python report that are not in the original:"
for row in rows - original_rows:
    print '   SITE:               %s' % row[0]
    print '   Original Species:   %s' % row[1]
    print '   Original Voucher:   %s' % row[2]
    print '   QC Species:         %s' % row[3]
    print '   QC Abundance:       %s' % row[4]
    print '   Match or No Match:  %s' % row[5]
    print '   Type:               %s' % row[6]
    print '\n'




initial.to_csv("InitialData.csv")
qc.to_csv("QCData.csv")
output.to_csv("DiscrepancyReport-Python.csv")
sorted_original_report.to_csv("DiscrepancyReport-Original-Sorted.csv")
original_report.to_csv("DiscrepancyReport-Original.csv")





