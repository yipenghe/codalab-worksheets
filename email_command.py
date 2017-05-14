# for testing
venv/bin/python scripts/send-email-notifications.py --subject "CodaLab Worksheets newsletter" --body-file ~/codalab/codalab-deployment/worksheets/newsletters/May\ 2015\ Newsletter\ -\ Overview.html --sent-file sent.json --threshold 2 --only-email maxwang7@stanford.edu --doit

# for actual
venv/bin/python scripts/send-email-notifications.py --subject "CodaLab Worksheets newsletter" --body-file ~/codalab/codalab-deployment/worksheets/newsletters/May\ 2015\ Newsletter\ -\ Overview.html --sent-file sent.json --threshold 2 --doit
