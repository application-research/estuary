#!/bin/bash

## This generates a report with the given case and response.

basename=$1
resp=$2

output=report.log

echo -n '
######
Test Case: '$basename'
Response:
'$resp'
######
' >> report.log